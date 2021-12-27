package gossip

import (
	"errors"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/Fantom-foundation/go-opera/eventcheck"
	"github.com/Fantom-foundation/go-opera/gossip/evmstore"
	"github.com/Fantom-foundation/go-opera/inter"
	"github.com/Fantom-foundation/go-opera/inter/ibr"
	"github.com/Fantom-foundation/go-opera/inter/ier"
)

func actualizeLowestIndex(current, upd uint64, exists func(uint64) bool) uint64 {
	if current == upd {
		current++
		for exists(current) {
			current++
		}
	}
	return current
}

func (s *Service) processBlockVote(block idx.Block, epoch idx.Epoch, bv hash.Hash, val idx.Validator, vals *pos.Validators, llrs *LlrState) {
	newWeight := s.store.AddLlrBlockVoteWeight(block, epoch, bv, val, vals.Len(), vals.GetWeightByIdx(val))
	if newWeight >= vals.TotalWeight()/3+1 {
		wonBr := s.store.GetLlrBlockResult(block)
		if wonBr == nil {
			s.store.SetLlrBlockResult(block, bv)
			llrs.LowestBlockToDecide = idx.Block(actualizeLowestIndex(uint64(llrs.LowestBlockToDecide), uint64(block), func(u uint64) bool {
				return s.store.GetLlrBlockResult(idx.Block(u)) != nil
			}))
		} else if *wonBr != bv {
			s.Log.Error("LLR voting doublesign is met", "block", block)
		}
	}
}

func (s *Service) ProcessBlockVotes(bvs inter.LlrSignedBlockVotes) error {
	// engineMu should be locked here
	if len(bvs.Val.Votes) == 0 {
		// short circuit if no records
		return nil
	}
	if s.store.HasBlockVotes(bvs.Val.Epoch, bvs.Val.LastBlock(), bvs.Signed.Locator.ID()) {
		return eventcheck.ErrAlreadyProcessedBVs
	}
	done := s.procLogger.BlockVotesConnectionStarted(bvs)
	defer done()
	vid := bvs.Signed.Locator.Creator
	// get the validators group
	epoch := bvs.Signed.Locator.Epoch
	es := s.store.GetHistoryEpochState(epoch)
	if es == nil {
		return eventcheck.ErrUnknownEpochBVs
	}

	llrs := s.store.GetLlrState()
	b := bvs.Val.Start
	for _, bv := range bvs.Val.Votes {
		s.processBlockVote(b, bvs.Val.Epoch, bv, es.Validators.GetIdx(vid), es.Validators, &llrs)
		b++
	}
	s.store.SetLlrState(llrs)
	s.store.SetBlockVotes(bvs)
	lBVs := s.store.GetLastBVs()
	lBVs.Lock()
	if bvs.Val.LastBlock() > lBVs.Val[vid] {
		lBVs.Val[vid] = bvs.Val.LastBlock()
		s.store.SetLastBVs(lBVs)
	}
	lBVs.Unlock()

	s.mayCommit(false)
	return nil
}

func indexRawReceipts(s *Store, receiptsForStorage []*types.ReceiptForStorage, txs types.Transactions, blockIdx idx.Block, atropos hash.Event) {
	s.evm.SetRawReceipts(blockIdx, receiptsForStorage)
	receipts, _ := evmstore.UnwrapStorageReceipts(receiptsForStorage, blockIdx, nil, common.Hash(atropos), txs)
	for _, r := range receipts {
		s.evm.IndexLogs(r.Logs...)
	}
}

func (s *Store) WriteFullBlockRecord(br ibr.LlrIdxFullBlockRecord) {
	txHashes := make([]common.Hash, 0, len(br.Txs))
	internalTxHashes := make([]common.Hash, 0, 2)
	for _, tx := range br.Txs {
		if tx.GasPrice().Sign() == 0 {
			internalTxHashes = append(internalTxHashes, tx.Hash())
		} else {
			txHashes = append(txHashes, tx.Hash())
		}
		s.EvmStore().SetTx(tx.Hash(), tx)
	}

	if len(br.Receipts) != 0 {
		// Note: it's possible for receipts to get indexed twice by BR and block processing
		indexRawReceipts(s, br.Receipts, br.Txs, br.Idx, br.Atropos)
	}
	for i, tx := range br.Txs {
		s.EvmStore().SetTx(tx.Hash(), tx)
		s.EvmStore().SetTxPosition(tx.Hash(), evmstore.TxPosition{
			Block:       br.Idx,
			BlockOffset: uint32(i),
		})
	}
	s.SetBlock(br.Idx, &inter.Block{
		Time:        br.Time,
		Atropos:     br.Atropos,
		Events:      hash.Events{},
		Txs:         txHashes,
		InternalTxs: internalTxHashes,
		SkippedTxs:  []uint32{},
		GasUsed:     br.GasUsed,
		Root:        br.Root,
	})
	s.SetBlockIndex(br.Atropos, br.Idx)
}

func (s *Service) ProcessFullBlockRecord(br ibr.LlrIdxFullBlockRecord) error {
	// engineMu should NOT be locked here
	if s.store.HasBlock(br.Idx) {
		return eventcheck.ErrAlreadyProcessedBR
	}
	done := s.procLogger.BlockRecordConnectionStarted(br)
	defer done()
	res := s.store.GetLlrBlockResult(br.Idx)
	if res == nil {
		return eventcheck.ErrUndecidedBR
	}

	if br.Hash() != *res {
		return errors.New("block record hash mismatch")
	}

	s.store.WriteFullBlockRecord(br)
	s.engineMu.Lock()
	defer s.engineMu.Unlock()
	if s.verWatcher != nil {
		// Note: it's possible for logs to get indexed twice by BR and block processing
		for _, r := range br.Receipts {
			for _, l := range r.Logs {
				s.verWatcher.OnNewLog(l)
			}
		}
	}
	updateLowestBlockToFill(br.Idx, s.store)

	s.mayCommit(false)
	return nil
}

func (s *Service) processEpochVote(epoch idx.Epoch, ev hash.Hash, val idx.Validator, vals *pos.Validators, llrs *LlrState) {
	newWeight := s.store.AddLlrEpochVoteWeight(epoch, ev, val, vals.Len(), vals.GetWeightByIdx(val))
	if newWeight >= vals.TotalWeight()/3+1 {
		wonEr := s.store.GetLlrEpochResult(epoch)
		if wonEr == nil {
			s.store.SetLlrEpochResult(epoch, ev)
			llrs.LowestEpochToDecide = idx.Epoch(actualizeLowestIndex(uint64(llrs.LowestEpochToDecide), uint64(epoch), func(u uint64) bool {
				return s.store.GetLlrEpochResult(idx.Epoch(u)) != nil
			}))
		} else if *wonEr != ev {
			s.Log.Error("LLR voting doublesign is met", "epoch", epoch)
		}
	}
}

func (s *Service) ProcessEpochVote(ev inter.LlrSignedEpochVote) error {
	// engineMu should be locked here
	if ev.Val.Epoch == 0 {
		// short circuit if no records
		return nil
	}
	if s.store.HasEpochVote(ev.Val.Epoch, ev.Signed.Locator.ID()) {
		return eventcheck.ErrAlreadyProcessedEV
	}
	done := s.procLogger.EpochVoteConnectionStarted(ev)
	defer done()
	vid := ev.Signed.Locator.Creator
	// get the validators group
	es := s.store.GetHistoryEpochState(ev.Val.Epoch - 1)
	if es == nil {
		return eventcheck.ErrUnknownEpochEV
	}

	llrs := s.store.GetLlrState()
	s.processEpochVote(ev.Val.Epoch, ev.Val.Vote, es.Validators.GetIdx(vid), es.Validators, &llrs)
	s.store.SetLlrState(llrs)
	s.store.SetEpochVote(ev)
	lEVs := s.store.GetLastEVs()
	lEVs.Lock()
	if ev.Val.Epoch > lEVs.Val[vid] {
		lEVs.Val[vid] = ev.Val.Epoch
		s.store.SetLastEVs(lEVs)
	}
	lEVs.Unlock()

	s.mayCommit(false)
	return nil
}

func (s *Store) WriteFullEpochRecord(er ier.LlrIdxFullEpochRecord) {
	s.SetHistoryBlockEpochState(er.Idx, er.BlockState, er.EpochState)
	s.SetEpochBlock(er.BlockState.LastBlock.Idx+1, er.Idx)
}

func (s *Service) ProcessFullEpochRecord(er ier.LlrIdxFullEpochRecord) error {
	// engineMu should NOT be locked here
	if s.store.HasHistoryBlockEpochState(er.Idx) {
		return eventcheck.ErrAlreadyProcessedER
	}
	done := s.procLogger.EpochRecordConnectionStarted(er)
	defer done()
	res := s.store.GetLlrEpochResult(er.Idx)
	if res == nil {
		return eventcheck.ErrUndecidedER
	}

	if er.Hash() != *res {
		return errors.New("epoch record hash mismatch")
	}

	s.store.WriteFullEpochRecord(er)
	s.engineMu.Lock()
	defer s.engineMu.Unlock()
	updateLowestEpochToFill(er.Idx, s.store)

	s.mayCommit(false)
	return nil
}

func updateLowestEpochToFill(epoch idx.Epoch, store *Store) {
	llrs := store.GetLlrState()
	llrs.LowestEpochToFill = idx.Epoch(actualizeLowestIndex(uint64(llrs.LowestEpochToFill), uint64(epoch), func(u uint64) bool {
		return store.HasHistoryBlockEpochState(idx.Epoch(u))
	}))
	store.SetLlrState(llrs)
}
