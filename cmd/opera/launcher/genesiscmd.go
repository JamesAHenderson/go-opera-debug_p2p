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

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"gopkg.in/urfave/cli.v1"

	"github.com/Fantom-foundation/go-opera/gossip/evmstore"
	"github.com/Fantom-foundation/go-opera/integration"
	"github.com/Fantom-foundation/go-opera/inter/ibr"
	"github.com/Fantom-foundation/go-opera/inter/ier"
	"github.com/Fantom-foundation/go-opera/opera/genesis"
	"github.com/Fantom-foundation/go-opera/opera/genesisstore"
	"github.com/Fantom-foundation/go-opera/opera/genesisstore/fileshash"
	"github.com/Fantom-foundation/go-opera/opera/genesisstore/fileszip"
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
	tmpI := 0
	return fileshash.WrapWriter(zWriter, genesisstore.FilesHashPieceSize, 64*opt.GiB, func() fileshash.TmpWriter {
		tmpI++
		tmpPath := path.Join(tmpDirPath, fmt.Sprintf("genesis-%s-tmp-%d", name, tmpI))
		_ = os.MkdirAll(tmpDirPath, os.ModePerm)
		tmpFh, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.ModePerm)
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
	if path == "-" {
		return nil, genesis.Header{}, genesis.Hashes{}, func() error { return nil }, nil
	}
	headFh, c := openFileszipReader(path)
	zMap, header, hashes, err := genesisstore.OpenGenesisStoreRaw([]fileszip.Reader{headFh})
	return zMap, header, hashes, c.Close, err
}

func mergeGenesis(ctx *cli.Context) error {
	if len(ctx.Args()) < 5 {
		utils.Fatalf("This command requires 5 arguments.")
	}

	_, header, _, hCloser, err := openGenesisStoreRaw(ctx.Args().Get(0))
	if err != nil {
		return err
	}
	defer hCloser()

	evsZip, _, evsHash, evsCloser, err := openGenesisStoreRaw(ctx.Args().Get(1))
	if err != nil {
		return err
	}
	defer evsCloser()

	bvsZip, _, bvsHash, bvsCloser, err := openGenesisStoreRaw(ctx.Args().Get(2))
	if err != nil {
		return err
	}
	defer bvsCloser()

	evmZip, _, evmHash, evmCloser, err := openGenesisStoreRaw(ctx.Args().Get(3))
	if err != nil {
		return err
	}
	defer evmCloser()

	fn := ctx.Args().Get(4)

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
	err = rlp.Encode(fh, header)
	if err != nil {
		return err
	}
	// write genesis hashes
	hashes := genesis.Hashes{
		Blocks:      evsHash.Blocks,
		Epochs:      bvsHash.Epochs,
		RawEvmItems: evmHash.RawEvmItems,
	}
	err = rlp.Encode(fh, hashes)
	if err != nil {
		return err
	}

	// Create the zip archive
	z := zip.NewWriter(fh)
	defer z.Close()

	writeSection := func(readerZip *fileszip.Map, name string) error {
		reader, _, err := readerZip.Open(name)
		if err != nil {
			return err
		}
		defer reader.Close()
		//writer, err := z.Create(name)
		writer, err := z.CreateHeader(&zip.FileHeader{
			Name:   name,
			Method: zip.Store,
		})
		if err != nil {
			return err
		}
		buf := make([]byte, 64*1024)
		eof := false
		for !eof {
			n, err := reader.Read(buf)
			if err == io.EOF {
				eof = true
			} else if err != nil {
				return err
			}
			_, err = writer.Write(buf[:n])
			if err != nil {
				return err
			}
		}
		err = z.Flush()
		if err != nil {
			return err
		}
		return nil
	}

	log.Info("Writing epochs")
	err = writeSection(evsZip, genesisstore.EpochsSection)
	if err != nil {
		return err
	}
	log.Info("Writing blocks")
	err = writeSection(bvsZip, genesisstore.BlocksSection)
	if err != nil {
		return err
	}
	log.Info("Writing EVM")
	err = writeSection(evmZip, genesisstore.EvmSection)
	if err != nil {
		return err
	}
	log.Info("Genesis is generated")

	fmt.Printf("- Epochs hashes: %v \n", hashes.Epochs)
	fmt.Printf("- Blocks hashes: %v \n", hashes.Blocks)
	fmt.Printf("- EVM hashes: %v \n", hashes.RawEvmItems)

	return nil
}
