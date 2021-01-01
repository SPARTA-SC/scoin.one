// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"github.com/jinzhu/gorm"
	"github.com/zyjblockchain/sandy_log/log"
	"tezos_index/puller/models"
)

const SupplyIndexKey = "supply"

type SupplyIndex struct {
	db *gorm.DB
}

func NewSupplyIndex(db *gorm.DB) *SupplyIndex {
	return &SupplyIndex{db}
}

func (idx *SupplyIndex) DB() *gorm.DB {
	return idx.db
}

func (idx *SupplyIndex) Key() string {
	return SupplyIndexKey
}

func (idx *SupplyIndex) ConnectBlock(ctx context.Context, block *models.Block, _ models.BlockBuilder, tx *gorm.DB) error {
	return tx.Model(&models.Supply{}).Create(block.Supply).Error
}

func (idx *SupplyIndex) DisconnectBlock(ctx context.Context, block *models.Block, _ models.BlockBuilder, tx *gorm.DB) error {
	return idx.DeleteBlock(ctx, block.Height, tx)
}

func (idx *SupplyIndex) DeleteBlock(ctx context.Context, height int64, tx *gorm.DB) error {
	log.Debugf("Rollback deleting supply state at height %d", height)
	return tx.Where("height = ?", height).Delete(&models.Supply{}).Error
}
