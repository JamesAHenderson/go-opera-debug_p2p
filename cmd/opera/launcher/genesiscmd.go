package launcher

import (
	"archive/zip"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/Fantom-foundation/lachesis-base/common/bigendian"
	gzip "github.com/klauspost/pgzip"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"gopkg.in/urfave/cli.v1"

	"github.com/Fantom-foundation/go-opera/gossip/evmstore"
	"github.com/Fantom-foundation/go-opera/integration"
	"github.com/Fantom-foundation/go-opera/inter/ibr"
	"github.com/Fantom-foundation/go-opera/inter/ier"
	"github.com/Fantom-foundation/go-opera/opera/genesis"
	"github.com/Fantom-foundation/go-opera/opera/genesisstore"
	"github.com/Fantom-foundation/go-opera/opera/genesisstore/fileshash"
	"github.com/Fantom-foundation/go-opera/opera/genesisstore/fileshash_unfixed"
	"github.com/Fantom-foundation/go-opera/opera/genesisstore/fileszip"
	utils2 "github.com/Fantom-foundation/go-opera/utils"
	"github.com/Fantom-foundation/go-opera/utils/iodb"
)

type dropableFile struct {
	io.ReadWriteSeeker
	io.Closer
	path string
}

func (f dropableFile) Drop() error {
	return os.Remove(f.path)
}

type mptIterator struct {
	kvdb.Iterator
}

func (it mptIterator) Next() bool {
	for it.Iterator.Next() {
		if evmstore.IsMptKey(it.Key()) {
			return true
		}
	}
	return false
}

type mptAndPreimageIterator struct {
	kvdb.Iterator
}

func (it mptAndPreimageIterator) Next() bool {
	for it.Iterator.Next() {
		if evmstore.IsMptKey(it.Key()) || evmstore.IsPreimageKey(it.Key()) {
			return true
		}
	}
	return false
}

func wrapIntoHashFile(backend *zip.Writer, tmpDirPath, name string) *fileshash.Writer {
	zWriter, err := backend.Create(name)
	if err != nil {
		log.Crit("Zip file creation error", "err", err)
	}
	return fileshash.WrapWriter(zWriter, genesisstore.FilesHashPieceSize, func(tmpI int) fileshash.TmpWriter {
		tmpI++
		tmpPath := path.Join(tmpDirPath, fmt.Sprintf("genesis-%s-tmp-%d", name, tmpI))
		_ = os.MkdirAll(tmpDirPath, os.ModePerm)
		tmpFh, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR, os.ModePerm)
		if err != nil {
			log.Crit("File opening error", "path", tmpPath, "err", err)
		}
		return dropableFile{
			ReadWriteSeeker: tmpFh,
			Closer:          tmpFh,
			path:            tmpPath,
		}
	})
}

const (
	fixTxBlock1    = 4738821
	fixTxBlockPos1 = 2
	fixTxBlock2    = 4801307
	fixTxBlockPos2 = 1
)

var (
	fixTxSender1 = common.HexToAddress("0x04d02149058cc8c8d0cf5f6fd1dc5394a80d7371")
	fixTxSender2 = common.HexToAddress("0x1325625ae81846e80ac9d0b8113f31e1f8b479a8")
)

func exportGenesis(ctx *cli.Context) error {
	if len(ctx.Args()) < 1 {
		utils.Fatalf("This command requires an argument.")
	}

	from := idx.Epoch(1)
	if len(ctx.Args()) > 1 {
		n, err := strconv.ParseUint(ctx.Args().Get(1), 10, 32)
		if err != nil {
			return err
		}
		from = idx.Epoch(n)
	}
	to := idx.Epoch(math.MaxUint32)
	if len(ctx.Args()) > 2 {
		n, err := strconv.ParseUint(ctx.Args().Get(2), 10, 32)
		if err != nil {
			return err
		}
		to = idx.Epoch(n)
	}
	mode := ctx.String(EvmExportMode.Name)
	if mode != "full" && mode != "ext-mpt" && mode != "mpt" && mode != "none" {
		return errors.New("--export.evm.mode must be one of {full, ext-mpt, mpt, none}")
	}

	cfg := makeAllConfigs(ctx)
	tmpPath := path.Join(cfg.Node.DataDir, "tmp")
	_ = os.RemoveAll(tmpPath)
	defer os.RemoveAll(tmpPath)

	rawProducer := integration.DBProducer(path.Join(cfg.Node.DataDir, "chaindata"), cacheScaler(ctx))
	gdb, err := makeRawGossipStore(rawProducer, cfg)
	if err != nil {
		log.Crit("DB opening error", "datadir", cfg.Node.DataDir, "err", err)
	}
	if gdb.GetHighestLamport() != 0 {
		log.Warn("Attempting genesis export not in a beginning of an epoch. Genesis file output may contain excessive data.")
	}
	defer gdb.Close()

	fn := ctx.Args().First()

	// Open the file handle
	fh, err := os.OpenFile(fn, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}
	defer fh.Close()

	// Write file header and version
	_, err = fh.Write(append(genesisstore.FileHeader, genesisstore.FileVersion...))
	if err != nil {
		return err
	}

	log.Info("Exporting genesis header")
	err = rlp.Encode(fh, genesis.Header{
		GenesisID:   *gdb.GetGenesisID(),
		NetworkID:   gdb.GetEpochState().Rules.NetworkID,
		NetworkName: gdb.GetEpochState().Rules.Name,
	})
	if err != nil {
		return err
	}
	// write dummy genesis hashes
	hashesFilePos, err := fh.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	dummy := genesis.Hashes{
		Blocks:      hash.Hashes{hash.Zero},
		Epochs:      hash.Hashes{hash.Zero},
		RawEvmItems: hash.Hashes{hash.Zero},
	}
	if mode == "none" {
		dummy.RawEvmItems = hash.Hashes{}
	}
	b, _ := rlp.EncodeToBytes(dummy)
	hashesFileLen := len(b)
	_, err = fh.Write(b)
	if err != nil {
		return err
	}
	hashes := genesis.Hashes{}

	// Create the zip archive
	z := zip.NewWriter(fh)
	defer z.Close()

	if from < 1 {
		// avoid underflow
		from = 1
	}
	if to > gdb.GetEpoch() {
		to = gdb.GetEpoch()
	}
	toBlock := idx.Block(0)
	fromBlock := idx.Block(0)
	{
		log.Info("Exporting epochs", "from", from, "to", to)
		writer := wrapIntoHashFile(z, tmpPath, genesisstore.EpochsSection)
		for i := to; i >= from; i-- {
			er := gdb.GetFullEpochRecord(i)
			if er == nil {
				log.Warn("No epoch record", "epoch", i)
				break
			}
			b, _ := rlp.EncodeToBytes(ier.LlrIdxFullEpochRecord{
				LlrFullEpochRecord: *er,
				Idx:                i,
			})
			_, err := writer.Write(b)
			if err != nil {
				return err
			}
			if i == from {
				fromBlock = er.BlockState.LastBlock.Idx
			}
			if i == to {
				toBlock = er.BlockState.LastBlock.Idx
			}
		}
		sectionRoot, err := writer.Flush()
		if err != nil {
			return err
		}
		hashes.Epochs.Add(sectionRoot)
		err = z.Flush()
		if err != nil {
			return err
		}
	}

	if fromBlock < 1 {
		// avoid underflow
		fromBlock = 1
	}
	{
		log.Info("Exporting blocks", "from", fromBlock, "to", toBlock)
		writer := wrapIntoHashFile(z, tmpPath, genesisstore.BlocksSection)
		for i := toBlock; i >= fromBlock; i-- {
			br := gdb.GetFullBlockRecord(i)
			if br == nil {
				log.Warn("No block record", "block", i)
				break
			}
			if i == fixTxBlock1 {
				tx := br.Txs[fixTxBlockPos1]
				br.Txs[fixTxBlockPos1] = types.NewTx(&types.LegacyTx{
					Nonce:    tx.Nonce(),
					GasPrice: tx.GasPrice(),
					Gas:      tx.Gas(),
					To:       tx.To(),
					Value:    tx.Value(),
					Data:     tx.Data(),
					V:        new(big.Int),
					R:        new(big.Int),
					S:        new(big.Int).SetBytes(fixTxSender1.Bytes()),
				})
			}
			if i == fixTxBlock2 {
				tx := br.Txs[fixTxBlockPos2]
				br.Txs[fixTxBlockPos2] = types.NewTx(&types.LegacyTx{
					Nonce:    tx.Nonce(),
					GasPrice: tx.GasPrice(),
					Gas:      tx.Gas(),
					To:       tx.To(),
					Value:    tx.Value(),
					Data:     tx.Data(),
					V:        new(big.Int),
					R:        new(big.Int),
					S:        new(big.Int).SetBytes(fixTxSender2.Bytes()),
				})
			}
			if i%200000 == 0 {
				log.Info("Exporting blocks", "last", i)
			}
			b, _ := rlp.EncodeToBytes(ibr.LlrIdxFullBlockRecord{
				LlrFullBlockRecord: *br,
				Idx:                i,
			})
			_, err := writer.Write(b)
			if err != nil {
				return err
			}
		}
		sectionRoot, err := writer.Flush()
		if err != nil {
			return err
		}
		hashes.Blocks.Add(sectionRoot)
		err = z.Flush()
		if err != nil {
			return err
		}
	}

	if mode != "none" {
		log.Info("Exporting EVM storage")
		writer := wrapIntoHashFile(z, tmpPath, genesisstore.EvmSection)
		it := gdb.EvmStore().EvmDb.NewIterator(nil, nil)
		if mode == "mpt" {
			// iterate only over MPT data
			it = mptIterator{it}
		} else if mode == "ext-mpt" {
			// iterate only over MPT data and preimages
			it = mptAndPreimageIterator{it}
		}
		defer it.Release()
		err = iodb.Write(writer, it)
		if err != nil {
			return err
		}
		sectionRoot, err := writer.Flush()
		if err != nil {
			return err
		}
		hashes.RawEvmItems.Add(sectionRoot)
		err = z.Flush()
		if err != nil {
			return err
		}
	}

	// write real file hashes after they were calculated
	_, err = fh.Seek(hashesFilePos, io.SeekStart)
	if err != nil {
		return err
	}
	b, _ = rlp.EncodeToBytes(hashes)
	if len(b) != hashesFileLen {
		return fmt.Errorf("real hashes length doesn't match to dummy hashes length: %d!=%d", len(b), hashesFileLen)
	}
	_, err = fh.Write(b)
	if err != nil {
		return err
	}
	_, err = fh.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	fmt.Printf("- Epochs hashes: %v \n", hashes.Epochs)
	fmt.Printf("- Blocks hashes: %v \n", hashes.Blocks)
	fmt.Printf("- EVM hashes: %v \n", hashes.RawEvmItems)

	return nil
}

func openGenesisStoreRaw(path string) (*fileszip.Map, genesis.Header, genesis.Hashes, func() error, error) {
	headFh, c := openFileszipReader(path)
	zMap, header, hashes, err := genesisstore.OpenGenesisStoreRaw([]fileszip.Reader{headFh})
	return zMap, header, hashes, c.Close, err
}

type GenesisUnitHeader struct {
	UnitName string
	Network  genesis.Header
}

func wrapIntoHashFile2(backend io.Writer, tmpDirPath, name string) *fileshash.Writer {
	return fileshash.WrapWriter(backend, genesisstore.FilesHashPieceSize, func(tmpI int) fileshash.TmpWriter {
		tmpPath := path.Join(tmpDirPath, fmt.Sprintf("genesis-%s-tmp-%d", name, tmpI))
		_ = os.MkdirAll(tmpDirPath, os.ModePerm)
		tmpFh, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR, os.ModePerm)
		if err != nil {
			log.Crit("File opening error", "path", tmpPath, "err", err)
		}
		return dropableFile{
			ReadWriteSeeker: tmpFh,
			Closer:          tmpFh,
			path:            tmpPath,
		}
	})
}

func mergeGenesis(ctx *cli.Context) error {
	if len(ctx.Args()) < 5 {
		utils.Fatalf("This command requires 5 arguments.")
	}

	start := time.Now()

	_, header, _, hCloser, err := openGenesisStoreRaw(ctx.Args().Get(0))
	if err != nil {
		return err
	}
	hCloser()

	fn := ctx.Args().Get(4)

	// Open the file handle
	fh, err := os.OpenFile(fn, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}
	defer fh.Close()

	cfg := makeAllConfigs(ctx)
	tmpPath := path.Join(cfg.Node.DataDir, "tmp")
	_ = os.RemoveAll(tmpPath)
	defer os.RemoveAll(tmpPath)

	writeSection := func(readerZip *fileszip.Map, name string, h hash.Hash, level int) error {
		// Write unit marker and version
		_, err = fh.Write(append(genesisstore.FileHeader, genesisstore.FileVersion...))
		if err != nil {
			return err
		}

		// write genesis header
		err = rlp.Encode(fh, GenesisUnitHeader{
			UnitName: name,
			Network:  header,
		})
		if err != nil {
			return err
		}

		dataStartPos, err := fh.Seek(8+8+32, io.SeekCurrent)
		if err != nil {
			return err
		}

		zReader, _, err := readerZip.Open(name)
		if err != nil {
			return err
		}
		defer zReader.Close()

		reader := fileshash_unfixed.WrapReader(zReader, opt.GiB, h)

		gWriter, _ := gzip.NewWriterLevel(fh, level)

		writer := wrapIntoHashFile2(gWriter, tmpPath, name)

		size := uint64(0)
		buffersToRead := make(chan []byte, 10)
		buffersToRead <- make([]byte, 32*1024)
		buffersToRead <- make([]byte, 32*1024)
		buffersToRead <- make([]byte, 32*1024)

		buffersForWrite := make(chan struct {
			b []byte
			s int
		}, 10)

		wg := sync.WaitGroup{}
		wg.Add(1)
		quit := make(chan interface{})
		var newH hash.Hash
		go func() {
			defer wg.Done()
			for {
				select {
				case buf := <-buffersForWrite:
					_, err = writer.Write(buf.b[:buf.s])
					if err != nil {
						panic(err)
					}
					buffersToRead <- buf.b
				case <-quit:
					if len(buffersForWrite) != 0 {
						continue
					}
					newH, err = writer.Flush()
					if err != nil {
						panic(err)
					}
					err = gWriter.Flush()
					if err != nil {
						panic(err)
					}
					return
				}
			}
		}()
		eof := false
		for !eof {
			buf := <-buffersToRead
			n, err := reader.Read(buf)
			if err == io.EOF {
				eof = true
			} else if err != nil {
				return err
			}
			buffersForWrite <- struct {
				b []byte
				s int
			}{buf, n}
			size += uint64(n)
		}
		close(quit)
		wg.Wait()

		endPos, err := fh.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}

		println(newH.String(), dataStartPos, endPos, endPos-dataStartPos, size)
		_, err = fh.Seek(dataStartPos-(8+8+32), io.SeekStart)
		if err != nil {
			return err
		}

		_, err = fh.Write(newH.Bytes())
		if err != nil {
			return err
		}
		_, err = fh.Write(bigendian.Uint64ToBytes(uint64(endPos - dataStartPos)))
		if err != nil {
			return err
		}
		_, err = fh.Write(bigendian.Uint64ToBytes(size))
		if err != nil {
			return err
		}

		gg, err := fh.Seek(0, io.SeekEnd)
		println(gg)
		if err != nil {
			return err
		}

		return nil
	}

	log.Info("Writing epochs", "elapsed", utils2.PrettyDuration(time.Since(start)))
	if ctx.Args().Get(1) != "-" {
		evsZip, _, evsHash, evsCloser, err := openGenesisStoreRaw(ctx.Args().Get(1))
		if err != nil {
			return err
		}
		err = writeSection(evsZip, genesisstore.EpochsSection, evsHash.Epochs[0], gzip.BestCompression)
		if err != nil {
			return err
		}
		fmt.Printf("- Epochs hashes: %v \n", evsHash.Epochs)
		evsCloser()
	}
	log.Info("Writing blocks", "elapsed", utils2.PrettyDuration(time.Since(start)))
	if ctx.Args().Get(2) != "-" {
		bvsZip, _, bvsHash, bvsCloser, err := openGenesisStoreRaw(ctx.Args().Get(2))
		if err != nil {
			return err
		}
		err = writeSection(bvsZip, genesisstore.BlocksSection, bvsHash.Blocks[0], gzip.BestCompression)
		if err != nil {
			return err
		}
		fmt.Printf("- Blocks hashes: %v \n", bvsHash.Blocks)
		bvsCloser()
	}
	if ctx.Args().Get(3) != "-" {
		log.Info("Writing EVM", "elapsed", utils2.PrettyDuration(time.Since(start)))
		evmZip, _, evmHash, evmCloser, err := openGenesisStoreRaw(ctx.Args().Get(3))
		if err != nil {
			return err
		}
		err = writeSection(evmZip, genesisstore.EvmSection, evmHash.RawEvmItems[0], gzip.BestCompression)
		fmt.Printf("- EVM hashes: %v \n", evmHash.RawEvmItems)
		evmCloser()
	}
	if err != nil {
		return err
	}
	log.Info("Genesis is merged", "elapsed", utils2.PrettyDuration(time.Since(start)))

	return nil
}
