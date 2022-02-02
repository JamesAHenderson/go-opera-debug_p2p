package sfcapi

import (
	"math/big"

	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/Fantom-foundation/go-opera/inter"
	"github.com/Fantom-foundation/go-opera/opera/contracts/sfc"
)

func OnNewLog(s *Store, l *types.Log) {
	if l.Address != sfc.ContractAddress {
		return
	}

	// Add/increase delegations
	if l.Topics[0] == Topics.Delegated && len(l.Topics) > 2 && len(l.Data) >= 32 {
		address := common.BytesToAddress(l.Topics[1][12:])
		toValidatorID := idx.ValidatorID(new(big.Int).SetBytes(l.Topics[2][:]).Uint64())
		amount := new(big.Int).SetBytes(l.Data[0:32])

		delegation := s.GetSfcDelegation(DelegationID{address, toValidatorID})
		var delegationAmount *big.Int
		if delegation != nil {
			// increasing of existing delegation
			delegationAmount.Add(delegation.Amount, amount)
		} else {
			// creation of delegation
			delegationAmount = amount
		}
		s.SetSfcDelegation(DelegationID{address, toValidatorID}, &SfcDelegation{
			Amount: delegationAmount,
		})
		// increase validator received stake
		validator := s.GetSfcValidator(toValidatorID)
		validator.ReceivedStake.Add(validator.ReceivedStake, amount)
		s.SetSfcValidator(toValidatorID, validator)
	}

	// Remove/decrease delegations
	if l.Topics[0] == Topics.Undelegated && len(l.Topics) > 2 && len(l.Data) >= 32 {
		address := common.BytesToAddress(l.Topics[1][12:])
		toValidatorID := idx.ValidatorID(new(big.Int).SetBytes(l.Topics[2][:]).Uint64())
		amount := new(big.Int).SetBytes(l.Data[0:32])
		id := DelegationID{address, toValidatorID}

		// decrease delegation
		delegation := s.GetSfcDelegation(id)
		if delegation == nil {
			// shouldn't be possible
			return
		}
		delegation.Amount.Sub(delegation.Amount, amount)
		if delegation.Amount.Sign() > 0 {
			s.SetSfcDelegation(id, delegation)
		} else {
			s.DelSfcDelegation(id)
		}
		// decrease validator received stake
		validator := s.GetSfcValidator(toValidatorID)
		validator.ReceivedStake.Sub(validator.ReceivedStake, amount)
		s.SetSfcValidator(toValidatorID, validator)
	}

	// Add new validators
	if l.Topics[0] == Topics.CreatedValidator && len(l.Topics) > 2 && len(l.Data) >= 32 {
		validatorID := idx.ValidatorID(new(big.Int).SetBytes(l.Topics[1][:]).Uint64())
		address := common.BytesToAddress(l.Topics[2][12:])
		createdEpoch := new(big.Int).SetBytes(l.Data[0:32])
		createdTime := new(big.Int).SetBytes(l.Data[32:64])

		s.SetSfcValidator(validatorID, &SfcValidator{
			CreatedEpoch:  idx.Epoch(createdEpoch.Uint64()),
			CreatedTime:   inter.FromUnix(int64(createdTime.Uint64())),
			Address:       address,
			ReceivedStake: new(big.Int),
		})
	}

	// Deactivate validators
	if (l.Topics[0] == Topics.DeactivatedValidator) && len(l.Topics) > 1 {
		validatorID := idx.ValidatorID(new(big.Int).SetBytes(l.Topics[1][:]).Uint64())
		deactivatedEpoch := new(big.Int).SetBytes(l.Data[0:32])
		deactivatedTime := new(big.Int).SetBytes(l.Data[32:64])

		validator := s.GetSfcValidator(validatorID)
		if validator == nil {
			// shouldn't be possible
			return
		}
		validator.DeactivatedEpoch = idx.Epoch(deactivatedEpoch.Uint64())
		validator.DeactivatedTime = inter.FromUnix(int64(deactivatedTime.Uint64()))
		s.SetSfcValidator(validatorID, validator)
	}

	// Change validator status
	if (l.Topics[0] == Topics.ChangedValidatorStatus) && len(l.Topics) > 1 {
		validatorID := idx.ValidatorID(new(big.Int).SetBytes(l.Topics[1][:]).Uint64())
		status := new(big.Int).SetBytes(l.Data[0:32])

		validator := s.GetSfcValidator(validatorID)
		if validator == nil {
			// shouldn't be possible
			return
		}
		validator.Status = status.Uint64()
		s.SetSfcValidator(validatorID, validator)
	}
}
