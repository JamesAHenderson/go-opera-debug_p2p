package sfcapi

import (
	"math/big"

	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/ethereum/go-ethereum/common"

	"github.com/Fantom-foundation/go-opera/inter"
)

var (
	// ForkBit is set if validator has a confirmed pair of fork events
	ForkBit = uint64(1)
	// OfflineBit is set if validator has didn't have confirmed events for a long time
	OfflineBit = uint64(1 << 8)
	// CheaterMask is a combination of severe misbehavings
	CheaterMask = ForkBit
)

const (
	// DelegationIDSize is size of DelegationID serialized object
	DelegationIDSize = 20 + 4
)

// SfcValidator is the node-side representation of SFC validator
type SfcValidator struct {
	CreatedEpoch idx.Epoch
	CreatedTime  inter.Timestamp

	DeactivatedEpoch idx.Epoch
	DeactivatedTime  inter.Timestamp

	Address common.Address

	ReceivedStake *big.Int

	Status uint64
}

// Ok returns true if not deactivated and not pruned
func (s *SfcValidator) Ok() bool {
	return s.Status == 0 && s.DeactivatedEpoch == 0
}

// IsCheater returns true if validator is cheater
func (s *SfcValidator) IsCheater() bool {
	return s.Status&CheaterMask != 0
}

// HasFork returns true if validator has a confirmed fork
func (s *SfcValidator) HasFork() bool {
	return s.Status&ForkBit != 0
}

// Offline returns true if validator was offline for long time
func (s *SfcValidator) Offline() bool {
	return s.Status&OfflineBit != 0
}

// SfcValidatorAndID is pair SfcValidator + validatorID
type SfcValidatorAndID struct {
	ValidatorID idx.ValidatorID
	Staker      *SfcValidator
}

// SfcDelegation is the node-side representation of SFC delegation
type SfcDelegation struct {
	Amount *big.Int
}

// DelegationID is a pair of delegator address and validator ID to which delegation is applied
type DelegationID struct {
	Delegator   common.Address
	ValidatorID idx.ValidatorID
}

func (id *DelegationID) Bytes() []byte {
	return append(id.Delegator.Bytes(), id.ValidatorID.Bytes()...)
}

func BytesToDelegationID(bb []byte) DelegationID {
	if len(bb) < DelegationIDSize {
		panic("delegation ID deserialization failed")
	}
	delegator := common.BytesToAddress(bb[:20])
	validatorID := idx.BytesToValidatorID(bb[20:])
	return DelegationID{delegator, validatorID}
}

// SfcDelegationAndID is pair SfcDelegation + address
type SfcDelegationAndID struct {
	Delegation *SfcDelegation
	ID         DelegationID
}

// EpochStats stores general statistics for an epoch
type EpochStats struct {
	Start    inter.Timestamp
	End      inter.Timestamp
	TotalFee *big.Int

	Epoch                 idx.Epoch `rlp:"-"` // API-only field
	TotalBaseRewardWeight *big.Int  `rlp:"-"` // API-only field
	TotalTxRewardWeight   *big.Int  `rlp:"-"` // API-only field
}

// Duration returns epoch duration
func (s *EpochStats) Duration() inter.Timestamp {
	return s.End - s.Start
}
