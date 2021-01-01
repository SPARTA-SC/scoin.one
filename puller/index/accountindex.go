// Copyright (c) 2020 Blockwatch Data Inc.

package index

import (
	"context"
	"errors"
	"fmt"
	"github.com/jinzhu/gorm"
	"github.com/zyjblockchain/sandy_log/log"
	"tezos_index/puller/models"
	"tezos_index/utils"
)

const AccountIndexKey = "account"

var (
	ErrNoAccountEntry = errors.New("account not indexed")
)

type AccountIndex struct {
	db *gorm.DB
}

func NewAccountIndex(db *gorm.DB) *AccountIndex {
	return &AccountIndex{db}
}

func (idx *AccountIndex) DB() *gorm.DB {
	return idx.db
}

func (idx *AccountIndex) Key() string {
	return AccountIndexKey
}

func (idx *AccountIndex) ConnectBlock(ctx context.Context, block *models.Block, builder models.BlockBuilder, tx *gorm.DB) error {
	upd := make([]*models.Account, 0, len(builder.Accounts()))
	// regular accounts
	for _, acc := range builder.Accounts() {
		if acc.IsDirty {
			upd = append(upd, acc)
		}
	}
	// delegate accounts
	for _, acc := range builder.Delegates() {
		if acc.IsDirty {
			upd = append(upd, acc)
		}
	}

	// todo batch update
	for _, upAcc := range upd {
		if err := UpdateAccount(upAcc, tx); err != nil {
			log.Errorf("update account record error: %v; upAccount: %v", err, upAcc)
			return err
		}
	}
	return nil
}

func UpdateAccount(acc *models.Account, db *gorm.DB) error {
	if acc.RowId.Value() <= 0 {
		return errors.New(fmt.Sprintf("This record (hash: %s) has not row_id,cannot update record ", acc.String()))
	}
	data := make(map[string]interface{})
	data["hash"] = acc.Hash
	data["delegate_id"] = acc.DelegateId
	data["manager_id"] = acc.ManagerId
	data["pubkey_hash"] = acc.PubkeyHash
	data["pubkey_type"] = acc.PubkeyType
	data["address_type"] = acc.Type
	data["first_in"] = acc.FirstIn
	data["first_out"] = acc.FirstOut
	data["last_in"] = acc.LastIn
	data["last_out"] = acc.LastOut
	data["first_seen"] = acc.FirstSeen
	data["last_seen"] = acc.LastSeen
	data["delegated_since"] = acc.DelegatedSince
	data["delegate_since"] = acc.DelegateSince
	data["total_received"] = acc.TotalReceived
	data["total_sent"] = acc.TotalSent
	data["total_burned"] = acc.TotalBurned
	data["total_fees_paid"] = acc.TotalFeesPaid
	data["total_rewards_earned"] = acc.TotalRewardsEarned
	data["total_fees_earned"] = acc.TotalFeesEarned
	data["total_lost"] = acc.TotalLost
	data["frozen_deposits"] = acc.FrozenDeposits
	data["frozen_rewards"] = acc.FrozenRewards
	data["frozen_fees"] = acc.FrozenFees
	data["unclaimed_balance"] = acc.UnclaimedBalance
	data["spendable_balance"] = acc.SpendableBalance
	data["delegated_balance"] = acc.DelegatedBalance
	data["total_delegations"] = acc.TotalDelegations
	data["active_delegations"] = acc.ActiveDelegations
	data["is_funded"] = acc.IsFunded
	data["is_activated"] = acc.IsActivated
	data["is_vesting"] = acc.IsVesting
	data["is_spendable"] = acc.IsSpendable
	data["is_delegatable"] = acc.IsDelegatable
	data["is_delegated"] = acc.IsDelegated
	data["is_revealed"] = acc.IsRevealed
	data["is_delegate"] = acc.IsDelegate
	data["is_active_delegate"] = acc.IsActiveDelegate
	data["is_contract"] = acc.IsContract
	data["blocks_baked"] = acc.BlocksBaked
	data["blocks_missed"] = acc.BlocksMissed
	data["blocks_stolen"] = acc.BlocksStolen
	data["blocks_endorsed"] = acc.BlocksEndorsed
	data["slots_endorsed"] = acc.SlotsEndorsed
	data["slots_missed"] = acc.SlotsMissed
	data["n_ops"] = acc.NOps
	data["n_ops_failed"] = acc.NOpsFailed
	data["n_tx"] = acc.NTx
	data["n_delegation"] = acc.NDelegation
	data["n_origination"] = acc.NOrigination
	data["n_proposal"] = acc.NProposal
	data["n_ballot"] = acc.NBallot
	data["token_gen_min"] = acc.TokenGenMin
	data["token_gen_max"] = acc.TokenGenMax
	data["grace_period"] = acc.GracePeriod

	return db.Model(&models.Account{}).Where("row_id = ?", acc.RowId.Value()).Updates(data).Error
}

func (idx *AccountIndex) DisconnectBlock(ctx context.Context, block *models.Block, builder models.BlockBuilder, tx *gorm.DB) error {
	// accounts to delete
	del := make([]uint64, 0)
	// accounts to update
	upd := make([]*models.Account, 0, len(builder.Accounts()))

	// regular accounts
	for _, acc := range builder.Accounts() {
		if acc.MustDelete {
			del = append(del, acc.RowId.Value())
		} else if acc.IsDirty {
			upd = append(upd, acc)
		}
	}

	// delegate accounts
	for _, acc := range builder.Delegates() {
		if acc.MustDelete {
			del = append(del, acc.RowId.Value())
		} else if acc.IsDirty {
			upd = append(upd, acc)
		}
	}

	// delete account
	if len(del) > 0 {
		// remove duplicates and sort; returns new slice
		del = util.UniqueUint64Slice(del)
		log.Debugf("Rollback removing accounts %#v", del)
		if err := tx.Where("row_id in (?)", del).Delete(&models.Account{}).Error; err != nil {
			log.Errorf("batch delete account error: %v", err)
			return err
		}
	}

	// Note on rebuild:
	// we don't rebuild last in/out counters since we assume
	// after reorg completes these counters are set properly again

	// todo batch update
	for _, upAcc := range upd {
		if err := UpdateAccount(upAcc, tx); err != nil {
			log.Errorf("update account record error: %v; upAccount: %v", err, upAcc)
			return err
		}
	}
	return nil
}

// DeleteBlock
func (idx *AccountIndex) DeleteBlock(ctx context.Context, height int64, tx *gorm.DB) error {
	log.Debugf("Rollback deleting accounts at height %d", height)
	err := tx.Where("first_seen = ?", height).Delete(&models.Account{}).Error
	return err
}
