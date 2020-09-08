package genesis

import (
	"github.com/Fantom-foundation/go-lachesis/opera/genesis/gpos"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/Fantom-foundation/go-lachesis/inter"
	"github.com/Fantom-foundation/go-lachesis/opera/genesis/proxy"
	"github.com/Fantom-foundation/go-lachesis/opera/genesis/sfc"
	"github.com/Fantom-foundation/go-lachesis/utils"
)

var (
	genesisTime = inter.Timestamp(1577419000 * time.Second)
)

type Genesis struct {
	Alloc     VAccounts
	Time      inter.Timestamp
	ExtraData []byte
}

func preDeploySfc(g Genesis, implCode []byte) Genesis {
	// pre deploy SFC impl
	g.Alloc.Accounts[sfc.ContractAddressV1] = Account{
		Code:    implCode, // impl account has only code, balance and storage is in proxy account
		Balance: big.NewInt(0),
	}
	// pre deploy SFC proxy
	storage := sfc.AssembleStorage(g.Alloc.Validators, g.Time, g.Alloc.SfcContractAdmin, nil)
	storage = proxy.AssembleStorage(g.Alloc.SfcContractAdmin, sfc.ContractAddressV1, storage) // Add storage of proxy
	g.Alloc.Accounts[sfc.ContractAddress] = Account{
		Code:    proxy.GetContractBin(),
		Storage: storage,
		Balance: g.Alloc.Validators.TotalStake(),
	}
	return g
}

// FakeGenesis generates fake genesis with n-nodes.
func FakeGenesis(accs VAccounts) Genesis {
	g := Genesis{
		Alloc: accs,
		Time:  genesisTime,
	}
	g = preDeploySfc(g, sfc.GetTestContractBinV1())
	return g
}

// MainGenesis returns builtin genesis keys of mainnet.
func MainGenesis() Genesis {
	g := Genesis{
		Time: genesisTime,
		Alloc: VAccounts{
			Accounts: Accounts{
				common.HexToAddress("0xd6A37423Be930019b8CFeA57BE049329f3119a3D"): Account{Balance: utils.ToFtm(2000000100)},
				common.HexToAddress("0x541E408443A592C38e01Bed0cB31f9De8c1322d0"): Account{Balance: utils.ToFtm(100)},
			},
			Validators: gpos.Validators{
				gpos.Validator{
					ID:      1,
					Address: common.HexToAddress("0x541E408443A592C38e01Bed0cB31f9De8c1322d0"),
					Stake:   utils.ToFtm(10000000),
				},
			},
			SfcContractAdmin: common.HexToAddress("0xd6A37423Be930019b8CFeA57BE049329f3119a3D"),
		},
	}
	g = preDeploySfc(g, sfc.GetMainContractBinV1())
	return g
}

// TestGenesis returns builtin genesis keys of testnet.
func TestGenesis() Genesis {
	g := Genesis{
		Time: genesisTime,
		Alloc: VAccounts{
			Accounts: Accounts{
				common.HexToAddress("0xe003e080e8d61207a0a9890c3663b4cd7fb766b8"): Account{Balance: utils.ToFtm(2000000100)},
				common.HexToAddress("0xcc8b10332478e26f676bccfc73f8c687e3ad1d04"): Account{Balance: utils.ToFtm(400)},
			},
			Validators: gpos.Validators{
				gpos.Validator{
					ID:      1,
					Address: common.HexToAddress("0xcc8b10332478e26f676bccfc73f8c687e3ad1d04"),
					Stake:   utils.ToFtm(40000000),
				},
			},
			SfcContractAdmin: common.HexToAddress("0xe003e080e8d61207a0a9890c3663b4cd7fb766b8"),
		},
	}
	g = preDeploySfc(g, sfc.GetTestContractBinV1())
	return g
}
