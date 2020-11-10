package gossip

import (
	"errors"
	"fmt"
	"github.com/Fantom-foundation/go-opera/opera/genesis"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
	"github.com/Fantom-foundation/lachesis-base/lachesis"
	"github.com/ethereum/go-ethereum/common"

	"github.com/Fantom-foundation/go-opera/evmcore"
	"github.com/Fantom-foundation/go-opera/gossip/blockproc"
	"github.com/Fantom-foundation/go-opera/gossip/evmstore"
	"github.com/Fantom-foundation/go-opera/inter"
	"github.com/Fantom-foundation/go-opera/inter/sfctype"
	"github.com/Fantom-foundation/go-opera/opera"
)

// GenesisMismatchError is raised when trying to overwrite an existing
// genesis block with an incompatible one.
type GenesisMismatchError struct {
	Stored, New hash.Event
}

// Error implements error interface.
func (e *GenesisMismatchError) Error() string {
	return fmt.Sprintf("database contains incompatible gossip genesis (have %s, new %s)", e.Stored.FullID(), e.New.FullID())
}

// ApplyGenesis writes initial state.
func (s *Store) ApplyGenesis(blockProc BlockProc, g opera.Genesis) (genesisAtropos hash.Event, new bool, err error) {
	storedGenesis := s.GetBlock(0)
	if storedGenesis != nil {
		newHash := calcGenesisHash(blockProc, g)
		if storedGenesis.Atropos != newHash {
			return genesisAtropos, true, &GenesisMismatchError{storedGenesis.Atropos, newHash}
		}

		genesisAtropos = storedGenesis.Atropos
		return genesisAtropos, false, nil
	}
	// if we'here, then it's first time genesis is applied
	genesisAtropos, err = s.applyEpoch1Genesis(blockProc, g)
	if err != nil {
		return genesisAtropos, true, err
	}

	return genesisAtropos, true, err
}

// calcGenesisHash calcs hash of genesis state.
func calcGenesisHash(blockProc BlockProc, g opera.Genesis) hash.Event {
	s := NewMemStore()
	defer s.Close()

	h, _ := s.applyEpoch1Genesis(blockProc, g)

	return h
}

func (s *Store) applyEpoch0Genesis(g opera.Genesis) (evmBlock *evmcore.EvmBlock, err error) {
	// apply app genesis
	evmBlock, err = s.evm.ApplyGenesis(g.State)
	if err != nil {
		return evmBlock, err
	}

	// write genesis blocks
	var highestBlock idx.Block
	g.State.Blocks.ForEach(func(index idx.Block, block genesis.Block) {
		txHashes := make([]common.Hash, len(block.Txs))
		internalTxHashes := make([]common.Hash, len(block.Txs))
		for i, tx := range block.Txs {
			txHashes[i] = tx.Hash()
		}
		for i, tx := range block.InternalTxs {
			internalTxHashes[i] = tx.Hash()
		}
		for i, tx := range append(block.InternalTxs, block.Txs...) {
			s.evm.SetTxPosition(tx.Hash(), evmstore.TxPosition{
				Block:       index,
				BlockOffset: uint32(i),
			})
			s.evm.SetTx(tx.Hash(), tx)
		}
		gasUsed := uint64(0)
		if len(block.Receipts) != 0 {
			gasUsed = block.Receipts[len(block.Receipts) - 1].GasUsed
		}

		s.SetBlock(index, &inter.Block{
			Time:        block.Time,
			Atropos:     block.Atropos,
			Events:      hash.Events{},
			Txs:         txHashes,
			InternalTxs: internalTxHashes,
			SkippedTxs:  []uint32{},
			GasUsed:     gasUsed,
			Root:        block.Root,
		})
		s.evm.SetRawReceipts(index, block.Receipts)
		highestBlock = index
	})

	s.SetBlockState(blockproc.BlockState{
		LastBlock:             highestBlock,
		EpochBlocks:           0,
		ValidatorStates:       make([]blockproc.ValidatorBlockState, 0),
		NextValidatorProfiles: make(map[idx.ValidatorID]sfctype.SfcValidator),
	})
	s.SetEpochState(blockproc.EpochState{
		Epoch:             g.State.FirstEpoch - 1,
		EpochStart:        g.State.Time - 1,
		PrevEpochStart:    g.State.Time - 2,
		Validators:        pos.NewBuilder().Build(),
		ValidatorStates:   make([]blockproc.ValidatorEpochState, 0),
		ValidatorProfiles: make(map[idx.ValidatorID]sfctype.SfcValidator),
	})

	return evmBlock, nil
}

func (s *Store) applyEpoch1Genesis(blockProc BlockProc, g opera.Genesis) (genesisAtropos hash.Event, err error) {
	evmBlock0, err := s.applyEpoch0Genesis(g)
	if err != nil {
		return genesisAtropos, err
	}

	evmStateReader := &EvmStateReader{store: s}
	statedb := s.evm.StateDB(hash.Hash(evmBlock0.Root))

	bs, es := s.GetBlockState(), s.GetEpochState()

	blockCtx := blockproc.BlockCtx{
		Idx:  bs.LastBlock,
		Time: g.State.Time,
		CBlock: lachesis.Block{
			Atropos:  hash.Event{},
			Cheaters: make(lachesis.Cheaters, 0),
		},
	}

	sealer := blockProc.SealerModule.Start(blockCtx, bs, es)
	sealing := true
	txListener := blockProc.TxListenerModule.Start(blockCtx, bs, es, statedb)
	evmProcessor := blockProc.EVMModule.Start(blockCtx, statedb, evmStateReader, txListener.OnNewLog)

	// Execute genesis-internal transactions
	genesisInternalTxs := blockProc.GenesisTxTransactor.PopInternalTxs(blockCtx, bs, es, sealing, statedb)
	evmProcessor.Execute(genesisInternalTxs, true)
	bs = txListener.Finalize()

	// Execute pre-internal transactions
	preInternalTxs := blockProc.PreTxTransactor.PopInternalTxs(blockCtx, bs, es, sealing, statedb)
	evmProcessor.Execute(preInternalTxs, true)
	bs = txListener.Finalize()

	// Seal epoch if requested
	if sealing {
		sealer.Update(bs, es)
		bs, es = sealer.SealEpoch()
		s.SetEpochState(es)
		txListener.Update(bs, es)
	}

	// Execute post-internal transactions
	internalTxs := blockProc.PostTxTransactor.PopInternalTxs(blockCtx, bs, es, sealing, statedb)
	evmProcessor.Execute(internalTxs, true)
	evmBlock, skippedTxs, receipts := evmProcessor.Finalize()
	for _, r := range receipts {
		if r.Status == 0 {
			return genesisAtropos, errors.New("genesis transaction reverted")
		}
	}
	if len(skippedTxs) != 0 {
		return genesisAtropos, errors.New("genesis transaction is skipped")
	}
	bs = txListener.Finalize()

	s.SetBlockState(bs)

	prettyHash := func(root common.Hash, g opera.Genesis) hash.Event {
		e := inter.MutableEventPayload{}
		// for nice-looking ID
		e.SetEpoch(0)
		e.SetLamport(idx.Lamport(g.Rules.Dag.MaxEpochBlocks))
		// actual data hashed
		e.SetExtra(append(root[:], g.State.ExtraData...))
		e.SetCreationTime(g.State.Time)

		return e.Build().ID()
	}
	genesisAtropos = prettyHash(evmBlock.Root, g)

	block := &inter.Block{
		Time:       g.State.Time,
		Atropos:    genesisAtropos,
		Events:     hash.Events{},
		SkippedTxs: skippedTxs,
		GasUsed:    evmBlock.GasUsed,
		Root:       hash.Hash(evmBlock.Root),
	}

	// store txs index
	for i, tx := range append(genesisInternalTxs, append(preInternalTxs, internalTxs...)...) {
		block.InternalTxs = append(block.InternalTxs, tx.Hash())
		s.evm.SetTx(tx.Hash(), tx)
		s.evm.SetTxPosition(tx.Hash(), evmstore.TxPosition{
			Block:       blockCtx.Idx,
			BlockOffset: uint32(i),
		})
	}
	s.evm.SetReceipts(blockCtx.Idx, receipts)

	s.SetBlock(blockCtx.Idx, block)
	s.SetBlockIndex(genesisAtropos, blockCtx.Idx)

	return genesisAtropos, nil
}
