// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"tezos_index/chain"
)

// ProposalsOp represents a proposal operation
type ProposalsOp struct {
	GenericOp
	Source    chain.Address        `json:"source"`
	Period    int                  `json:"period"`
	Proposals []chain.ProtocolHash `json:"proposals"`
}
