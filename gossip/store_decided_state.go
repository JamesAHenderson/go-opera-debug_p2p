package gossip

import (
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
	"github.com/ethereum/go-ethereum/log"

	"github.com/Fantom-foundation/go-opera/gossip/blockproc"
)

const lbKey = "d"

const leKey = "e"

// SetBlockState stores the latest block state in memory
func (s *Store) SetBlockState(v blockproc.BlockState) {
	s.cache.BlockState.Store(&v)
}

// FlushEpochState stores the latest block state in DB
func (s *Store) FlushBlockState() {
	s.rlp.Set(s.table.BlockState, []byte(lbKey), s.GetBlockState())
}

// GetBlockState retrieves the latest block state
func (s *Store) GetBlockState() blockproc.BlockState {
	if v := s.cache.BlockState.Load(); v != nil {
		return *v.(*blockproc.BlockState)
	}
	v, ok := s.rlp.Get(s.table.BlockState, []byte(lbKey), &blockproc.BlockState{}).(*blockproc.BlockState)
	if !ok {
		log.Crit("Genesis not applied")
	}
	s.cache.BlockState.Store(v)
	return *v
}

// SetEpochState stores the latest block state in memory
func (s *Store) SetEpochState(v blockproc.EpochState) {
	s.cache.EpochState.Store(&v)
}

// FlushEpochState stores the latest epoch state in DB
func (s *Store) FlushEpochState() {
	s.rlp.Set(s.table.EpochState, []byte(leKey), s.GetEpochState())
}

// GetEpochState retrieves the latest epoch state
func (s *Store) GetEpochState() blockproc.EpochState {
	if v := s.cache.EpochState.Load(); v != nil {
		return *v.(*blockproc.EpochState)
	}
	v, ok := s.rlp.Get(s.table.EpochState, []byte(leKey), &blockproc.EpochState{}).(*blockproc.EpochState)
	if !ok {
		log.Crit("Genesis not applied")
	}
	s.cache.EpochState.Store(v)
	return *v
}

// GetEpoch retrieves the current epoch
func (s *Store) GetEpoch() idx.Epoch {
	return s.GetEpochState().Epoch
}

// GetValidators retrieves alidators atomically
func (s *Store) GetValidators() *pos.Validators {
	return s.GetEpochState().Validators
}

// GetEpoch retrieves the current epoch and validators atomically
func (s *Store) GetEpochValidators() (*pos.Validators, idx.Epoch) {
	state := s.GetEpochState()
	return state.Validators, state.Epoch
}

// GetEpoch retrieves the current block number
func (s *Store) GetLatestBlockIndex() idx.Block {
	return s.GetBlockState().LastBlock
}

// GetEpochBlockIndex retrieves the number of blocks in current epoch
func (s *Store) GetEpochBlocks() idx.Block {
	return s.GetBlockState().EpochBlocks
}
