package main

import (
	"crypto/ecdsa"
	"fmt"
	"github.com/Fantom-foundation/go-opera/integration/makegenesis"
	"strconv"
	"strings"

	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/ethereum/go-ethereum/log"
	cli "gopkg.in/urfave/cli.v1"
)

// FakeNetFlag enables special testnet, where validators are automatically created
var FakeNetFlag = cli.StringFlag{
	Name:  "fakenet",
	Usage: "'n/N[,non-validators]' - sets coinbase as fake n-th key from genesis of N validators. Non-validators is a count or json-file.",
}

// FakeNetFlag enables special testnet, where validators are automatically created
var GenesisFlag = cli.StringFlag{
	Name:  "genesis",
	Usage: "'path to genesis file' - sets the network genesis configuration.",
}

func getFakeValidatorKey(ctx *cli.Context) *ecdsa.PrivateKey {
	num, _, err := parseFakeGen(ctx.GlobalString(FakeNetFlag.Name))
	if err != nil {
		log.Crit("Invalid flag", "flag", FakeNetFlag.Name, "err", err)
	}

	if num == 0 {
		return nil
	}

	return makegenesis.FakeKey(int(num))
}

func parseFakeGen(s string) (id idx.ValidatorID, num int, err error) {
	parts := strings.SplitN(s, "/", 2)
	if len(parts) != 2 {
		err = fmt.Errorf("use %%d/%%d format")
		return
	}

	var u32 uint64
	u32, err = strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return
	}
	id = idx.ValidatorID(u32)

	parts = strings.SplitN(parts[1], ",", 2)

	u32, err = strconv.ParseUint(parts[0], 10, 32)
	num = int(u32)
	if num < 0 || int(id) > num {
		err = fmt.Errorf("key-num should be in range from 1 to validators (<key-num>/<validators>), or should be zero for non-validator node")
		return
	}

	return
}
