package opera

import (
	"math/big"
	"time"

	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	ethparams "github.com/ethereum/go-ethereum/params"

	"github.com/Fantom-foundation/go-opera/inter"
	"github.com/Fantom-foundation/go-opera/opera/params"
)

const (
	MainNetworkID uint64 = 0xfa
	TestNetworkID uint64 = 0xfa2
	FakeNetworkID uint64 = 0xfa3
)

// Rules describes opera net.
type Rules struct {
	Name      string
	NetworkID uint64

	// Graph options
	Dag DagConfig

	// Blockchain options
	Blocks BlocksConfig

	// Economy options
	Economy EconomyConfig
}

// GasPowerConfig defines gas power rules in the consensus.
type GasPowerConfig struct {
	InitialAllocPerSec uint64
	MaxAllocPerSec     uint64
	MinAllocPerSec     uint64
	MaxAllocPeriod     inter.Timestamp
	StartupAllocPeriod inter.Timestamp
	MinStartupGas      uint64
}

// DagConfig of Lachesis DAG (directed acyclic graph).
type DagConfig struct {
	MaxParents     uint32
	MaxFreeParents uint32 // maximum number of parents with no gas cost

	MaxEpochBlocks   idx.Block
	MaxEpochDuration inter.Timestamp
}

// BlocksMissed is information about missed blocks from a staker
type BlocksMissed struct {
	BlocksNum idx.Block
	Period    inter.Timestamp
}

// EconomyConfig contains economy constants
type EconomyConfig struct {
	BlockMissedLatency idx.Block

	ShortGasPower GasPowerConfig
	LongGasPower  GasPowerConfig
}

// BlocksConfig contains blocks constants
type BlocksConfig struct {
	BlockGasHardLimit uint64 // technical hard limit, gas is mostly governed by gas power allocation
}

// EvmChainConfig returns ChainConfig for transaction signing and execution
func (c *Rules) EvmChainConfig() *ethparams.ChainConfig {
	cfg := *ethparams.AllEthashProtocolChanges
	cfg.ChainID = new(big.Int).SetUint64(c.NetworkID)
	return &cfg
}

func MainNetRules() Rules {
	return Rules{
		Name:      "main",
		NetworkID: MainNetworkID,
		Dag:       DefaultDagConfig(),
		Economy:   DefaultEconomyConfig(),
		Blocks: BlocksConfig{
			BlockGasHardLimit: 20000000,
		},
	}
}

func TestNetRules() Rules {
	return Rules{
		Name:      "test",
		NetworkID: TestNetworkID,
		Dag:       DefaultDagConfig(),
		Economy:   DefaultEconomyConfig(),
		Blocks: BlocksConfig{
			BlockGasHardLimit: 20000000,
		},
	}
}

func FakeNetRules() Rules {
	return Rules{
		Name:      "fake",
		NetworkID: FakeNetworkID,
		Dag:       FakeNetDagConfig(),
		Economy:   FakeEconomyConfig(),
		Blocks: BlocksConfig{
			BlockGasHardLimit: 20000000,
		},
	}
}

// DefaultEconomyConfig returns mainnet economy
func DefaultEconomyConfig() EconomyConfig {
	return EconomyConfig{
		BlockMissedLatency: 3,
		ShortGasPower:      DefaultShortGasPowerConfig(),
		LongGasPower:       DefaulLongGasPowerConfig(),
	}
}

// FakeEconomyConfig returns fakenet economy
func FakeEconomyConfig() EconomyConfig {
	cfg := DefaultEconomyConfig()
	cfg.ShortGasPower = FakeShortGasPowerConfig()
	cfg.LongGasPower = FakeLongGasPowerConfig()
	return cfg
}

func DefaultDagConfig() DagConfig {
	return DagConfig{
		MaxParents:       10,
		MaxFreeParents:   3,
		MaxEpochBlocks:   1000,
		MaxEpochDuration: inter.Timestamp(4 * time.Hour),
	}
}

func FakeNetDagConfig() DagConfig {
	cfg := DefaultDagConfig()
	cfg.MaxEpochBlocks = 200
	cfg.MaxEpochDuration = inter.Timestamp(10 * time.Minute)
	return cfg
}

// DefaulLongGasPowerConfig is long-window config
func DefaulLongGasPowerConfig() GasPowerConfig {
	return GasPowerConfig{
		InitialAllocPerSec: 100 * params.EventGas,
		MaxAllocPerSec:     1000 * params.EventGas,
		MinAllocPerSec:     10 * params.EventGas,
		MaxAllocPeriod:     inter.Timestamp(60 * time.Minute),
		StartupAllocPeriod: inter.Timestamp(5 * time.Second),
		MinStartupGas:      params.EventGas * 20,
	}
}

// DefaultShortGasPowerConfig is short-window config
func DefaultShortGasPowerConfig() GasPowerConfig {
	// 5x faster allocation rate, 12x lower max accumulated gas power
	cfg := DefaulLongGasPowerConfig()
	cfg.InitialAllocPerSec *= 5
	cfg.MaxAllocPerSec *= 5
	cfg.MinAllocPerSec *= 5
	cfg.StartupAllocPeriod /= 5
	cfg.MaxAllocPeriod /= 5 * 12
	return cfg
}

// FakeLongGasPowerConfig is fake long-window config
func FakeLongGasPowerConfig() GasPowerConfig {
	config := DefaulLongGasPowerConfig()
	config.InitialAllocPerSec *= 1000
	config.MaxAllocPerSec *= 1000
	return config
}

// FakeShortGasPowerConfig is fake short-window config
func FakeShortGasPowerConfig() GasPowerConfig {
	config := DefaultShortGasPowerConfig()
	config.InitialAllocPerSec *= 1000
	config.MaxAllocPerSec *= 1000
	return config
}
