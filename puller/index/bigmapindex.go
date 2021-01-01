// Copyright (c) 2020 Blockwatch Data Inc.

package index

import (
	"context"
	"errors"
	"fmt"
	"github.com/jinzhu/gorm"
	"github.com/zyjblockchain/sandy_log/log"
	"tezos_index/chain"
	"tezos_index/micheline"
	"tezos_index/puller/models"
	"tezos_index/rpc"
	"tezos_index/utils"
)

var (
	ErrNoBigMapEntry = errors.New("bigmap not indexed")
)

const BigMapIndexKey = "bigmap"

type BigMapIndex struct {
	db *gorm.DB
}

func NewBigMapIndex(db *gorm.DB) *BigMapIndex {
	return &BigMapIndex{db}
}

func (idx *BigMapIndex) DB() *gorm.DB {
	return idx.db
}

func (idx *BigMapIndex) Key() string {
	return BigMapIndexKey
}

// asumes op ids are already set (must run after OpIndex)
// Note: zero is a valid bigmap id in all protocols
func (idx *BigMapIndex) ConnectBlock(ctx context.Context, block *models.Block, builder models.BlockBuilder, tx *gorm.DB) error {
	needClear := false
	contract := &models.Contract{}
	for _, op := range block.Ops {
		if len(op.BigMapDiff) == 0 || !op.IsSuccess {
			continue
		}
		// load rpc op
		o, ok := block.GetRPCOp(op.OpN, op.OpC)
		if !ok {
			return fmt.Errorf("missing bigmap transaction op [%d:%d]", op.OpN, op.OpC)
		}

		// clear temp bigmaps after a batch of internal ops has been processed
		if !op.IsInternal && needClear {
			err := tx.Where("bigmap_id < ?", int64(0)).Delete(&models.BigMapItem{}).Error
			if err != nil {
				return fmt.Errorf("clearing temp bigmaps for op [%d:%d] failed: %v", op.OpN, op.OpC, err)
			}
			needClear = false
		}

		// extract deserialized bigmap diff
		var bmd micheline.BigMapDiff
		switch op.Type {
		case chain.OpTypeTransaction:
			if op.IsInternal {
				// on internal tx, find corresponding internal op
				top, ok := o.(*rpc.TransactionOp)
				if !ok {
					return fmt.Errorf("internal bigmap transaction op [%d:%d]: unexpected type %T ", op.OpN, op.OpC, o)
				}
				bmd = top.Metadata.InternalResults[op.OpI].Result.BigMapDiff
			} else {
				top, ok := o.(*rpc.TransactionOp)
				if !ok {
					return fmt.Errorf("contract bigmap transaction op [%d:%d]: unexpected type %T ", op.OpN, op.OpC, o)
				}
				bmd = top.Metadata.Result.BigMapDiff
			}
		case chain.OpTypeOrigination:
			if op.IsInternal {
				// on internal tx, find corresponding internal op
				top, ok := o.(*rpc.TransactionOp)
				if !ok {
					return fmt.Errorf("internal bigmap origination op [%d:%d]: unexpected type %T ", op.OpN, op.OpC, o)
				}
				bmd = top.Metadata.InternalResults[op.OpI].Result.BigMapDiff
			} else {
				oop, ok := o.(*rpc.OriginationOp)
				if !ok {
					return fmt.Errorf("contract bigmap origination op [%d:%d]: unexpected type %T ", op.OpN, op.OpC, o)
				}
				bmd = oop.Metadata.Result.BigMapDiff
			}
		}

		// load corresponding contract
		if contract.AccountId != op.ReceiverId {
			err := tx.Where("account_id = ?", op.ReceiverId.Value()).First(contract).Error // Stream
			if err != nil {
				return fmt.Errorf("missing contract account %d: %v", op.ReceiverId, err)
			}
		}

		// process bigmapdiffs
		alloc := &models.BigMapItem{}
		last := &models.BigMapItem{}
		for _, v := range bmd {
			// find and update previous key if any
			prev := &models.BigMapItem{}
			switch v.Action {
			case micheline.BigMapDiffActionUpdate, micheline.BigMapDiffActionRemove:
				// find bigmap allocation (required for real key type)
				if alloc.RowId == 0 || alloc.BigMapId != v.Id {
					err := tx.Where("bigmap_id = ? and action = ? ", v.Id, uint64(micheline.BigMapDiffActionAlloc)).First(alloc).Error
					if err != nil && err != gorm.ErrRecordNotFound {
						return fmt.Errorf("etl.bigmap.alloc decode: %v", err)
					}
				}
				if last.RowId == 0 || last.BigMapId != alloc.BigMapId {
					err := tx.Where("bigmap_id = ?", v.Id).Last(last).Error
					if err != nil && err != gorm.ErrRecordNotFound {
						return fmt.Errorf("etl.bigmap.last decode: %v", err)
					}
				}

				// find the previuos entry at this key if exists
				err := tx.Where("bigmap_id = ? and key_hash = ? and is_replaced = ?", v.Id, v.KeyHash.Hash.Hash, false).First(prev).Error
				if err != nil && err != gorm.ErrRecordNotFound {
					return fmt.Errorf("etl.bigmap.update decode: %v", err)
				}

				// flush update immediately to allow sequence of updates
				if prev.RowId > 0 {
					prev.IsReplaced = true
					prev.Updated = block.Height

					if err := tx.Model(&models.BigMapItem{}).Updates(prev).Error; err != nil {
						return fmt.Errorf("etl.bigmap.update: %v", err)
					}
				}

				// update counters for next entry
				nkeys := last.NKeys
				if v.Action == micheline.BigMapDiffActionRemove {
					// ignore double remove (if that's even possible)
					if prev.RowId > 0 && !prev.IsDeleted {
						nkeys--
					}
				} else {
					// new keys are detected by non-existing prev
					if prev.RowId == 0 {
						nkeys++
					}
				}

				// insert immediately to allow sequence of updates
				item := models.NewBigMapItem(op, contract, v, prev.RowId, alloc.KeyType, last.Counter+1, nkeys)
				if err := tx.Create(item).Error; err != nil {
					return fmt.Errorf("etl.bigmap.insert: %v", err)
				}

				last = item

			case micheline.BigMapDiffActionAlloc:
				// insert immediately to allow sequence of updates
				needClear = needClear || v.DestId < 0
				item := models.NewBigMapItem(op, contract, v, prev.RowId, 0, 0, 0)
				if err := tx.Create(item).Error; err != nil {
					return fmt.Errorf("etl.bigmap.insert: %v", err)
				}
				last = item

			case micheline.BigMapDiffActionCopy:
				// copy the alloc and all current keys to new entries, set is_copied
				ins := make([]*models.BigMapItem, 0)
				needClear = needClear || v.DestId < 0

				// find the source alloc and all current bigmap entries
				var counter int64

				var sources []*models.BigMapItem
				err := tx.Where("bigmap_id = ? and is_replaced = ? and is_deleted = ?", v.SourceId, false, false).Find(&sources).Error
				if err != nil && err != gorm.ErrRecordNotFound {
					return fmt.Errorf("etl.bigmap.copy: %v", err)
				}
				for _, source := range sources {
					// copy the item
					if source.Action == micheline.BigMapDiffActionAlloc {
						item := models.CopyBigMapAlloc(source, op, contract, v.DestId, counter+1, 0)
						ins = append(ins, item)
						last = item
					} else {
						item := models.CopyBigMapValue(source, op, contract, v.DestId, counter+1, counter)
						ins = append(ins, item)
						last = item
					}
					counter++
				}

				// todo batch insert
				for _, val := range ins {
					if err := tx.Create(val).Error; err != nil {
						return fmt.Errorf("etl.bigmap.insert: %v", err)
					}
				}
			}
		}
	}

	// clear temp bigmaps after all ops have been processed
	if needClear {
		err := tx.Where("bigmap_id < ?", int64(0)).Delete(&models.BigMapItem{}).Error
		if err != nil && err != gorm.ErrRecordNotFound {
			return fmt.Errorf("clearing temp bigmaps for block %d failed: %v", block.Height, err)
		}
	}
	return nil
}

func (idx *BigMapIndex) DisconnectBlock(ctx context.Context, block *models.Block, _ models.BlockBuilder, tx *gorm.DB) error {
	return idx.DeleteBlock(ctx, block.Height, tx)
}

func (idx *BigMapIndex) DeleteBlock(ctx context.Context, height int64, tx *gorm.DB) error {
	log.Debugf("Rollback deleting bigmap updates at height %d", height)

	// need to unset IsReplaced flags in prior rows for every entry we find here
	upd := make([]*models.BigMapItem, 0)
	del := make([]uint64, 0)
	ids := make([]uint64, 0)

	// find all items to delete and all items to update
	var items []*models.BigMapItem
	err := tx.Select("row_id, prev_id, is_copied").Where("height = ? ", height).Find(&items).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return err
	}
	for _, item := range items {
		if !item.IsCopied {
			ids = append(ids, item.PrevId)
		}
		del = append(ids, item.RowId)
	}

	// load update items
	// 去重
	ids = util.UniqueUint64Slice(ids)
	if len(ids) > 0 && ids[0] == 0 {
		ids = ids[1:]
	}
	if len(ids) > 0 {
		var items []*models.BigMapItem
		err := tx.Where("row_id in (?)", ids).Find(&items).Error
		if err != nil && err != gorm.ErrRecordNotFound {
			return err
		}
		for _, item := range items {
			item.IsReplaced = false
			upd = append(upd, item)
		}
	}
	// and run update
	if len(upd) > 0 {
		// todo batch update
		for _, val := range upd {
			err := models.UpdatesBigMapItem(val, tx)
			if err != nil {
				return err
			}
		}
	}

	// now delete the original items
	if len(del) > 0 {
		return tx.Where("row_id in (?)", del).Delete(&models.BigMapItem{}).Error
	}
	return nil
}
