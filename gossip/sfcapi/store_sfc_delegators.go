package sfcapi

import (
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
)

// GetSfcDelegation returns stored SfcDelegation
func (s *Store) GetSfcDelegation(id DelegationID) *SfcDelegation {
	w, _ := s.rlp.Get(s.table.Delegations, id.Bytes(), &SfcDelegation{}).(*SfcDelegation)
	return w
}

// SetSfcDelegation stores SfcDelegation
func (s *Store) SetSfcDelegation(id DelegationID, v *SfcDelegation) {
	s.rlp.Set(s.table.Delegations, id.Bytes(), v)
}

// DelSfcDelegation deletes SfcDelegation
func (s *Store) DelSfcDelegation(id DelegationID) {
	err := s.table.Delegations.Delete(id.Bytes())
	if err != nil {
		s.Log.Crit("Failed to erase delegation")
	}
}

// ForEachSfcDelegation iterates stored SfcDelegations
func (s *Store) ForEachSfcDelegation(start []byte, do func(SfcDelegationAndID) bool) {
	it := s.table.Delegations.NewIterator(nil, start)
	defer it.Release()
	s.forEachSfcDelegation(it, func(id SfcDelegationAndID) bool {
		return do(id)
	})
}

func (s *Store) forEachSfcDelegation(it ethdb.Iterator, do func(SfcDelegationAndID) bool) {
	_continue := true
	for _continue && it.Next() {
		delegation := &SfcDelegation{}
		err := rlp.DecodeBytes(it.Value(), delegation)
		if err != nil {
			s.Log.Crit("Failed to decode rlp while iteration", "err", err)
		}

		addr := it.Key()[len(it.Key())-DelegationIDSize:]
		_continue = do(SfcDelegationAndID{
			ID:         BytesToDelegationID(addr),
			Delegation: delegation,
		})
	}
}
