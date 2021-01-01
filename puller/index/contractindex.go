// Copyright (c) 2020 Blockwatch Data Inc.

package index

import (
	"context"
	"errors"
	"fmt"
	"github.com/jinzhu/gorm"
	"github.com/zyjblockchain/sandy_log/log"
	"tezos_index/chain"
	"tezos_index/puller/models"
	"tezos_index/rpc"
)

const ContractIndexKey = "contract"

var (
	ErrNoContractEntry = errors.New("contract not indexed")
)

type ContractIndex struct {
	db *gorm.DB
}

func NewContractIndex(db *gorm.DB) *ContractIndex {
	return &ContractIndex{db}
}

func (idx *ContractIndex) DB() *gorm.DB {
	return idx.db
}

func (idx *ContractIndex) Key() string {
	return ContractIndexKey
}

func (idx *ContractIndex) ConnectBlock(ctx context.Context, block *models.Block, builder models.BlockBuilder, tx *gorm.DB) error {
	ct := make([]*models.Contract, 0, block.NewContracts)
	for _, op := range block.Ops {
		if op.Type != chain.OpTypeOrigination {
			continue
		}
		if !op.IsSuccess {
			continue
		}
		if !op.IsContract {
			continue
		}
		// load rpc origination or transaction op
		o, ok := block.GetRPCOp(op.OpN, op.OpC)
		if !ok {
			return fmt.Errorf("missing contract origination op [%d:%d]", op.OpN, op.OpC)
		}
		// load corresponding account
		acc, ok := builder.AccountById(op.ReceiverId)
		if !ok {
			return fmt.Errorf("missing contract account %d", op.ReceiverId)
		}
		// skip when contract is not new (unlikely because every origination creates a
		// new account, but need to check invariant here)
		if !acc.IsNew {
			continue
		}
		if op.IsInternal {
			// on internal originations, find corresponding internal op
			top, ok := o.(*rpc.TransactionOp)
			if !ok {
				return fmt.Errorf("internal contract origination op [%d:%d]: unexpected type %T ", op.OpN, op.OpC, o)
			}
			iop := top.Metadata.InternalResults[op.OpI]
			ct = append(ct, models.NewInternalContract(acc, iop))
		} else {
			oop, ok := o.(*rpc.OriginationOp)
			if !ok {
				return fmt.Errorf("contract origination op [%d:%d]: unexpected type %T ", op.OpN, op.OpC, o)
			}
			ct = append(ct, models.NewContract(acc, oop))
		}
	}
	// insert, will generate unique row ids
	if len(ct) > 0 {
		for _, contract := range ct {
			if err := tx.Create(contract).Error; err != nil {
				return err
			}
		}
		return nil
	} else {
		return nil
	}
}

// func BatchInsertContracts(records []*models.Contract, db *gorm.DB) error {
// 	if len(records) == 0 {
// 		return nil
// 	}
// 	sql := "INSERT INTO `contracts` (`hash`, `account_id`, `manager_id`, `height`, `fee`, `gas_limit`, `gas_used`, `gas_price`, `storage_limit`," +
// 		" `storage_size`, `storage_paid`, `script`, `is_spendable`, `is_delegatable`) VALUES "
// 	// 循环data数组,组合sql语句
// 	for key, value := range records {
// 		if len(records)-1 == key {
// 			// 最后一条数据 以分号结尾
// 			sql += fmt.Sprintf("('%s','%d','%d','%d','%d','%d','%d','%f','%d','%d','%d','%s','%t','%t');",
// 				value.Hash, value.AccountId, value.ManagerId, value.Height, value.Fee, value.GasLimit, value.GasUsed,
// 				value.GasPrice, value.StorageLimit, value.StorageSize, value.StoragePaid, value.Script, value.IsSpendable,
// 				value.IsDelegatable)
// 		} else {
// 			sql += fmt.Sprintf("('%s','%d','%d','%d','%d','%d','%d','%f','%d','%d','%d','%s','%t','%t'),",
// 				value.Hash, value.AccountId, value.ManagerId, value.Height, value.Fee, value.GasLimit, value.GasUsed,
// 				value.GasPrice, value.StorageLimit, value.StorageSize, value.StoragePaid, value.Script, value.IsSpendable,
// 				value.IsDelegatable)
// 		}
// 	}
// 	err := db.Exec(sql).Error
// 	return err
// }

func (idx *ContractIndex) DisconnectBlock(ctx context.Context, block *models.Block, _ models.BlockBuilder, tx *gorm.DB) error {
	return idx.DeleteBlock(ctx, block.Height, tx)
}

func (idx *ContractIndex) DeleteBlock(ctx context.Context, height int64, tx *gorm.DB) error {
	log.Debugf("Rollback deleting contracts at height %d", height)
	err := tx.Where("height = ?", height).Delete(&models.Contract{}).Error
	return err
}
