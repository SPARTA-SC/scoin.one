// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"github.com/jinzhu/gorm"
	"github.com/zyjblockchain/sandy_log/log"
	"tezos_index/puller/models"
)

const ChainIndexKey = "chain"

type ChainIndex struct {
	db *gorm.DB
}

func NewChainIndex(db *gorm.DB) *ChainIndex {
	return &ChainIndex{db}
}

func (idx *ChainIndex) DB() *gorm.DB {
	return idx.db
}

func (idx *ChainIndex) Key() string {
	return ChainIndexKey
}

func (idx *ChainIndex) ConnectBlock(ctx context.Context, block *models.Block, _ models.BlockBuilder, tx *gorm.DB) error {
	return tx.Create(block.Chain).Error
}

func (idx *ChainIndex) DisconnectBlock(ctx context.Context, block *models.Block, _ models.BlockBuilder, tx *gorm.DB) error {
	return idx.DeleteBlock(ctx, block.Height, tx)
}

func (idx *ChainIndex) DeleteBlock(ctx context.Context, height int64, tx *gorm.DB) error {
	log.Debugf("Rollback deleting chain state at height %d", height)
	err := tx.Where("height = ?", height).Delete(&models.Chain{}).Error
	return err
}
