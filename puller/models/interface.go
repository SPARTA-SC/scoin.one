// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package models

import (
	"context"
	"github.com/jinzhu/gorm"
	"tezos_index/chain"
)

// BlockCrawler provides an interface to access information about the current
// state of blockchain crawling.
type BlockCrawler interface {

	// returns the blockchain params at specified block height
	ParamsByHeight(height int64) *chain.Params

	// returns the blockchain params for the specified protocol
	ParamsByProtocol(proto chain.ProtocolHash) *chain.Params

	// returns the current crawler chain tip
	Tip() *ChainTip

	// returns the crawler's most recently seen block height
	Height() int64

	// returns stored (main chain) block at specified height
	BlockByHeight(ctx context.Context, height int64) (*Block, error)

	// returns stored chain data at specified height
	ChainByHeight(ctx context.Context, height int64) (*Chain, error)

	// returns stored supply table data at specified height
	SupplyByHeight(ctx context.Context, height int64) (*Supply, error)
}

// BlockBuilder provides an interface to access information about the currently
// processed block.
type BlockBuilder interface {
	// resolves account from address, returns nil and false when not found
	AccountByAddress(chain.Address) (*Account, bool)

	// resolves account from id, returns nil and false when not found
	AccountById(AccountID) (*Account, bool)

	// returns a map of all accounts referenced in the current block
	Accounts() map[AccountID]*Account

	// returns a map of all delegates referenced in the current block
	Delegates() map[AccountID]*Account

	// returns block rights
	Rights(chain.RightType) []Right
}

// BlockIndexer provides a generic interface for an indexer that is managed by an
// etl.Indexer.
type BlockIndexer interface {

	// Key returns the key of the index as a string.
	Key() string

	// ConnectBlock is invoked when the table manager is notified that a new
	// block has been connected to the main chain.
	ConnectBlock(ctx context.Context, block *Block, builder BlockBuilder, tx *gorm.DB) error

	// DisconnectBlock is invoked when the table manager is notified that a
	// block has been disconnected from the main chain.
	DisconnectBlock(ctx context.Context, block *Block, builder BlockBuilder, tx *gorm.DB) error

	// DeleteBlock is invoked when the table manager is notified that a
	// block must be rolled back after an error occured.
	DeleteBlock(ctx context.Context, height int64, tx *gorm.DB) error

	// returns the database storing all indexer tables
	DB() *gorm.DB
}
