// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"github.com/jinzhu/gorm"
	"github.com/zyjblockchain/sandy_log/log"
	"tezos_index/puller/models"
)

const FlowIndexKey = "flow"

type FlowIndex struct {
	db *gorm.DB
}

func NewFlowIndex(db *gorm.DB) *FlowIndex {
	return &FlowIndex{db}
}

func (idx *FlowIndex) DB() *gorm.DB {
	return idx.db
}

func (idx *FlowIndex) Key() string {
	return FlowIndexKey
}

func (idx *FlowIndex) ConnectBlock(ctx context.Context, block *models.Block, _ models.BlockBuilder, tx *gorm.DB) error {
	flows := make([]*models.Flow, 0, len(block.Flows))
	for _, f := range block.Flows {
		flows = append(flows, f)
	}
	// todo batch insert
	for _, f := range block.Flows {
		if err := tx.Create(f).Error; err != nil {
			return err
		}
	}
	return nil
}

func (idx *FlowIndex) DisconnectBlock(ctx context.Context, block *models.Block, _ models.BlockBuilder, tx *gorm.DB) error {
	return idx.DeleteBlock(ctx, block.Height, tx)
}

func (idx *FlowIndex) DeleteBlock(ctx context.Context, height int64, tx *gorm.DB) error {
	log.Debugf("Rollback deleting flows at height %d", height)
	return tx.Where("height = ? ", height).Delete(&models.Flow{}).Error
}
