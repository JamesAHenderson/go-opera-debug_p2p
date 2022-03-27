package evmstore

import (
	"github.com/Fantom-foundation/lachesis-base/kvdb/batched"

	"github.com/Fantom-foundation/go-opera/opera/genesis"
)

// ApplyGenesis writes initial state.
func (s *Store) ApplyGenesis(g genesis.Genesis) (err error) {
	batchedDB := batched.Wrap(s.RawEvmDB())
	g.RawEvmItems.ForEach(func(key, value []byte) bool {
		if err != nil {
			return false
		}
		err = batchedDB.Put(key, value)
		return err == nil
	})
	if err != nil {
		return err
	}
	return batchedDB.Flush()
}
