// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"errors"
	"github.com/jinzhu/gorm"
	"github.com/zyjblockchain/sandy_log/log"
	"tezos_index/puller/models"
)

const (
	SnapshotPackSizeLog2    = 15 // =32k packs
	SnapshotJournalSizeLog2 = 17 // =128k entries
	SnapshotCacheSize       = 2  // minimum
	SnapshotFillLevel       = 100
	SnapshotIndexKey        = "snapshot"
	SnapshotTableKey        = "snapshot"
)

var (
	ErrNoSnapshotEntry = errors.New("snapshot not indexed")
)

type SnapshotIndex struct {
	db *gorm.DB
}

func NewSnapshotIndex(db *gorm.DB) *SnapshotIndex {
	return &SnapshotIndex{db}
}

func (idx *SnapshotIndex) DB() *gorm.DB {
	return idx.db
}

func (idx *SnapshotIndex) Key() string {
	return SnapshotIndexKey
}

func (idx *SnapshotIndex) ConnectBlock(ctx context.Context, block *models.Block, builder models.BlockBuilder, tx *gorm.DB) error {
	// handle snapshot index
	if block.TZ.Snapshot != nil {
		if err := idx.UpdateCycleSnapshot(ctx, block, tx); err != nil {
			return err
		}
	}

	// skip non-snapshot blocks
	if block.Height == 0 || block.Height%block.Params.BlocksPerRollSnapshot != 0 {
		return nil
	}

	// first snapshot (0 based) is block 255 (0 based index) in a cycle
	// for governance, snapshot 15 (at end of cycle == start of voting period is used)
	sn := ((block.Height - block.Params.BlocksPerRollSnapshot) % block.Params.BlocksPerCycle) / block.Params.BlocksPerRollSnapshot

	rollOwners := make([]uint64, 0, block.Chain.RollOwners)
	ins := make([]*models.Snapshot, 0, int(block.Chain.FundedAccounts)) // hint

	var accs []*models.Account
	err := tx.Model(&models.Account{}).Select("row_id, delegate_id, is_delegate, "+
		"is_active_delegate, spendable_balance, frozen_deposits, frozen_fees, "+
		"delegated_balance, active_delegations, delegate_since").Where("is_active_delegate = ?", true).Find(&accs).Error
	if err != nil {
		return err
	}

	for _, a := range accs {
		// check account owns at least one roll
		ownbalance := a.SpendableBalance + a.FrozenDeposits + a.FrozenFees
		stakingBalance := ownbalance + a.DelegatedBalance
		if stakingBalance < block.Params.TokensPerRoll {
			continue
		}
		snap := models.NewSnapshot()
		snap.Height = block.Height
		snap.Cycle = block.Cycle
		snap.Timestamp = block.Timestamp
		snap.Index = sn
		snap.Rolls = int64(stakingBalance / block.Params.TokensPerRoll)
		snap.AccountId = a.RowId
		snap.DelegateId = a.DelegateId
		snap.IsDelegate = a.IsDelegate
		snap.IsActive = a.IsActiveDelegate
		snap.Balance = ownbalance
		snap.Delegated = a.DelegatedBalance
		snap.NDelegations = a.ActiveDelegations
		snap.Since = a.DelegateSince
		ins = append(ins, snap)
		rollOwners = append(rollOwners, a.RowId.Value())
	}

	// todo batch insert
	for _, in := range ins {
		if err := tx.Create(in).Error; err != nil {
			return err
		}
	}

	for _, v := range ins {
		v.Free()
	}
	ins = ins[:0]

	// snapshot all delegating accounts with non-zero balance that reference one of the
	// roll owners
	accs = make([]*models.Account, 0)
	err = tx.Model(&models.Account{}).Select("row_id, delegate_id, spendable_balance, "+
		"delegated_since, unclaimed_balance, is_vesting").Where("is_funded = ? and delegate_id in (?)", true, rollOwners).Find(&accs).Error
	if err != nil {
		return err
	}
	for _, a := range accs {
		// skip all self-delegations because the're already handled above
		if a.RowId == a.DelegateId {
			return nil
		}
		snap := models.NewSnapshot()
		snap.Height = block.Height
		snap.Cycle = block.Cycle
		snap.Timestamp = block.Timestamp
		snap.Index = sn
		snap.Rolls = 0
		snap.AccountId = a.RowId
		snap.DelegateId = a.DelegateId
		snap.IsDelegate = false
		snap.IsActive = false
		snap.Balance = a.SpendableBalance
		if a.IsVesting {
			snap.Balance += a.UnclaimedBalance
		}
		snap.Delegated = 0
		snap.NDelegations = 0
		snap.Since = a.DelegatedSince
		ins = append(ins, snap)
	}

	// todo batch insert
	log.Infof("start insert snapshot index; record num: %d", len(ins))
	for _, in := range ins {
		if err := tx.Create(in).Error; err != nil {
			return err
		}
	}
	for _, v := range ins {
		v.Free()
	}
	return err
}

func (idx *SnapshotIndex) DisconnectBlock(ctx context.Context, block *models.Block, _ models.BlockBuilder, tx *gorm.DB) error {
	// skip non-snapshot blocks
	if block.Height == 0 || block.Height%block.Params.BlocksPerRollSnapshot != 0 {
		return nil
	}
	return idx.DeleteBlock(ctx, block.Height, tx)
}

func (idx *SnapshotIndex) DeleteBlock(ctx context.Context, height int64, tx *gorm.DB) error {
	log.Debugf("Rollback deleting snapshots at height %d", height)
	err := tx.Where("height = ?", height).Delete(&models.Snapshot{}).Error
	return err
}

func (idx *SnapshotIndex) UpdateCycleSnapshot(ctx context.Context, block *models.Block, tx *gorm.DB) error {
	// update all snapshot rows at snapshot cycle & index
	snap := block.TZ.Snapshot
	// fetch all rows from table, updating contents
	rows := make([]*models.Snapshot, 0, 1024)

	var ss []*models.Snapshot
	err := tx.Where("cycle = ? and s_index = ?", snap.Cycle-(block.Params.PreservedCycles+2), snap.RollSnapshot).Find(&ss).Error
	if err != nil {
		return err
	}
	for _, s := range ss {
		s.IsSelected = true
		rows = append(rows, s)
	}

	// store update
	// todo batch update
	for _, row := range rows {
		if err := updateThisSnapshot(row, tx); err != nil {
			return err
		}
	}

	// FIXME: consider flushing the table for sorted results after update
	// if this becomes a problem

	return nil
}

func updateThisSnapshot(s *models.Snapshot, db *gorm.DB) error {
	data := make(map[string]interface{})
	data["is_selected"] = s.IsSelected
	return db.Model(&models.Snapshot{}).Where("row_id = ?", s.RowId).Updates(data).Error
}
