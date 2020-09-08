package genesis

import (
	"github.com/Fantom-foundation/go-lachesis/opera/genesis/gpos"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"math/big"

	"github.com/Fantom-foundation/go-lachesis/crypto"
)

// FakeValidators returns validators accounts for fakenet
func FakeValidators(count int, balance *big.Int, stake *big.Int) VAccounts {
	accs := make(Accounts, count)
	validators := make(gpos.Validators, 0, count)

	for i := 1; i <= count; i++ {
		key := crypto.FakeKey(i)
		addr := crypto.PubkeyToAddress(key.PublicKey)
		accs[addr] = Account{
			Balance:    balance,
			PrivateKey: key,
		}
		validatorID := idx.ValidatorID(i)
		validators = append(validators, gpos.Validator{
			ID:      validatorID,
			Address: addr,
			Stake:   stake,
		})
	}

	return VAccounts{Accounts: accs, Validators: validators, SfcContractAdmin: validators[0].Address}
}
