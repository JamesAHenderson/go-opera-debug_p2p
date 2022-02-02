package sfcapi

import (
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
)

// SetSfcValidator stores SfcValidator
func (s *Store) SetSfcValidator(validatorID idx.ValidatorID, v *SfcValidator) {
	s.rlp.Set(s.table.Stakers, validatorID.Bytes(), v)
}

// GetSfcValidator returns stored SfcValidator
func (s *Store) GetSfcValidator(validatorID idx.ValidatorID) *SfcValidator {
	w, _ := s.rlp.Get(s.table.Stakers, validatorID.Bytes(), &SfcValidator{}).(*SfcValidator)

	return w
}

// HasSfcValidator returns true if validator exists
func (s *Store) HasSfcValidator(validatorID idx.ValidatorID) bool {
	ok, err := s.table.Stakers.Has(validatorID.Bytes())
	if err != nil {
		s.Log.Crit("Failed to get validator", "err", err)
	}
	return ok
}

// DelSfcValidator deletes SfcValidator
func (s *Store) DelSfcValidator(validatorID idx.ValidatorID) {
	err := s.table.Stakers.Delete(validatorID.Bytes())
	if err != nil {
		s.Log.Crit("Failed to erase validator")
	}
}

// ForEachSfcValidator iterates all stored SfcValidators
func (s *Store) ForEachSfcValidator(do func(SfcValidatorAndID)) {
	it := s.table.Stakers.NewIterator(nil, nil)
	defer it.Release()
	s.forEachSfcValidator(it, do)
}

// GetSfcValidators returns all stored SfcValidators
func (s *Store) GetSfcValidators() []SfcValidatorAndID {
	validators := make([]SfcValidatorAndID, 0, 200)
	s.ForEachSfcValidator(func(it SfcValidatorAndID) {
		validators = append(validators, it)
	})
	return validators
}

func (s *Store) forEachSfcValidator(it ethdb.Iterator, do func(SfcValidatorAndID)) {
	for it.Next() {
		validator := &SfcValidator{}
		err := rlp.DecodeBytes(it.Value(), validator)
		if err != nil {
			s.Log.Crit("Failed to decode rlp while iteration", "err", err)
		}

		validatorIDBytes := it.Key()[len(it.Key())-4:]
		do(SfcValidatorAndID{
			ValidatorID: idx.BytesToValidatorID(validatorIDBytes),
			Staker:   validator,
		})
	}
}
