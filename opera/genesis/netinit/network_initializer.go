package netinit

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

// GetContractBin is NetworkInitializer contract genesis implementation bin code
// Has to be compiled with flag bin-runtime
func GetContractBin() []byte {
	return hexutil.MustDecode("0x608060405234801561001057600080fd5b506004361061002b5760003560e01c8063c80e151314610030575b600080fd5b610091600480360360e081101561004657600080fd5b5080359060208101359073ffffffffffffffffffffffffffffffffffffffff60408201358116916060810135821691608082013581169160a081013582169160c09091013516610093565b005b604080517f485cc95500000000000000000000000000000000000000000000000000000000815273ffffffffffffffffffffffffffffffffffffffff8681166004830152848116602483015291519185169163485cc9559160448082019260009290919082900301818387803b15801561010c57600080fd5b505af1158015610120573d6000803e3d6000fd5b5050604080517fc0c53b8b00000000000000000000000000000000000000000000000000000000815273ffffffffffffffffffffffffffffffffffffffff8981166004830152878116602483015285811660448301529151918816935063c0c53b8b925060648082019260009290919082900301818387803b1580156101a557600080fd5b505af11580156101b9573d6000803e3d6000fd5b5050604080517f019e2729000000000000000000000000000000000000000000000000000000008152600481018b9052602481018a905273ffffffffffffffffffffffffffffffffffffffff888116604483015285811660648301529151918916935063019e2729925060848082019260009290919082900301818387803b15801561024457600080fd5b505af1158015610258573d6000803e3d6000fd5b50600092505050fffea265627a7a72315820c58d5d6a94d19845dc66f26b63b3cb0322c27f8aae57fe90cd36b9b92c9dafd564736f6c634300050c0032")
}

// ContractAddress is the NetworkInitializer contract address
var ContractAddress = common.HexToAddress("0xd1005eed00000000000000000000000000000000")
