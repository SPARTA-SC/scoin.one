// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package models

import (
	"fmt"
	"sync"
	"tezos_index/chain"
	"tezos_index/utils"
)

var accountPool = &sync.Pool{
	New: func() interface{} { return new(Account) },
}

type AccountID uint64

func (id AccountID) Value() uint64 {
	return uint64(id)
}

// we could differentiate rewards into
// - baking
// - endorsement
// - denounciation
// - revelation
//
// likewise we could differentiate burn into
// - denounciation
// - origination
// - transaction (storage fees)

// Account is an up-to-date snapshot of the current status. For history look at Flow (balance updates).
type Account struct {
	RowId              AccountID         `gorm:"primary_key;index;column:row_id" json:"row_id"`
	Hash               []byte            `gorm:"column:hash;index:addr"  json:"hash"`
	DelegateId         AccountID         `gorm:"column:delegate_id;index:acc"   json:"delegate_id"`
	ManagerId          AccountID         `gorm:"column:manager_id;index:acc"  json:"manager_id"`
	PubkeyHash         []byte            `gorm:"column:pubkey_hash"       json:"pubkey_hash"`
	PubkeyType         chain.HashType    `gorm:"column:pubkey_type"    json:"pubkey_type"`
	Type               chain.AddressType `gorm:"column:address_type;index:addr"   json:"address_type"`
	Addr               string            `gorm:"column:address;index:raw_address" json:"-"`
	FirstIn            int64             `gorm:"column:first_in"   json:"first_in"`
	FirstOut           int64             `gorm:"column:first_out"   json:"first_out"`
	LastIn             int64             `gorm:"column:last_in" json:"last_in"`
	LastOut            int64             `gorm:"column:last_out"  json:"last_out"`
	FirstSeen          int64             `gorm:"column:first_seen"   json:"first_seen"` // genesis account block height
	LastSeen           int64             `gorm:"column:last_seen"   json:"last_seen"`
	DelegatedSince     int64             `gorm:"column:delegated_since"  json:"delegated_since"`
	DelegateSince      int64             `gorm:"column:delegate_since"   json:"delegate_since"`
	TotalReceived      int64             `gorm:"column:total_received"  json:"total_received"`
	TotalSent          int64             `gorm:"column:total_sent"  json:"total_sent"`
	TotalBurned        int64             `gorm:"column:total_burned"  json:"total_burned"`
	TotalFeesPaid      int64             `gorm:"column:total_fees_paid"   json:"total_fees_paid"`
	TotalRewardsEarned int64             `gorm:"column:total_rewards_earned"  json:"total_rewards_earned"`
	TotalFeesEarned    int64             `gorm:"column:total_fees_earned"    json:"total_fees_earned"`
	TotalLost          int64             `gorm:"column:total_lost"   json:"total_lost"` // lost due to denounciation
	FrozenDeposits     int64             `gorm:"column:frozen_deposits"   json:"frozen_deposits"`
	FrozenRewards      int64             `gorm:"column:frozen_rewards"   json:"frozen_rewards"`
	FrozenFees         int64             `gorm:"column:frozen_fees"   json:"frozen_fees"`
	UnclaimedBalance   int64             `gorm:"column:unclaimed_balance"   json:"unclaimed_balance"` // vesting or not activated
	SpendableBalance   int64             `gorm:"column:spendable_balance"   json:"spendable_balance"`
	DelegatedBalance   int64             `gorm:"column:delegated_balance"   json:"delegated_balance"`
	TotalDelegations   int64             `gorm:"column:total_delegations"   json:"total_delegations"`    // from delegate ops
	ActiveDelegations  int64             `gorm:"column:active_delegations"    json:"active_delegations"` // with non-zero balance
	IsFunded           bool              `gorm:"column:is_funded"    json:"is_funded"`
	IsActivated        bool              `gorm:"column:is_activated"    json:"is_activated"` // bc: fundraiser account
	IsVesting          bool              `gorm:"column:is_vesting"   json:"is_vesting"`      // bc: vesting contract account
	IsSpendable        bool              `gorm:"column:is_spendable"    json:"is_spendable"` // manager can move funds without running any code
	IsDelegatable      bool              `gorm:"column:is_delegatable"    json:"is_delegatable"`
	IsDelegated        bool              `gorm:"column:is_delegated"    json:"is_delegated"`
	IsRevealed         bool              `gorm:"column:is_revealed"   json:"is_revealed"`
	IsDelegate         bool              `gorm:"column:is_delegate;index:dlg"   json:"is_delegate"`
	IsActiveDelegate   bool              `gorm:"column:is_active_delegate;index:act_del"    json:"is_active_delegate"`
	IsContract         bool              `gorm:"column:is_contract"  json:"is_contract"` // smart contract with code
	BlocksBaked        int               `gorm:"column:blocks_baked"  json:"blocks_baked"`
	BlocksMissed       int               `gorm:"column:blocks_missed"   json:"blocks_missed"`
	BlocksStolen       int               `gorm:"column:blocks_stolen"   json:"blocks_stolen"`
	BlocksEndorsed     int               `gorm:"column:blocks_endorsed"    json:"blocks_endorsed"`
	SlotsEndorsed      int               `gorm:"column:slots_endorsed"   json:"slots_endorsed"`
	SlotsMissed        int               `gorm:"column:slots_missed"   json:"slots_missed"`
	NOps               int               `gorm:"column:n_ops"   json:"n_ops"`                  // stats: successful operation count
	NOpsFailed         int               `gorm:"column:n_ops_failed"  json:"n_ops_failed"`     // stats: failed operation coiunt
	NTx                int               `gorm:"column:n_tx"   json:"n_tx"`                    // stats: number of Tx operations
	NDelegation        int               `gorm:"column:n_delegation"   json:"n_delegation"`    // stats: number of Delegations operations
	NOrigination       int               `gorm:"column:n_origination"   json:"n_origination"`  // stats: number of Originations operations
	NProposal          int               `gorm:"column:n_proposal"  json:"n_proposal"`         // stats: number of Proposals operations
	NBallot            int               `gorm:"column:n_ballot"    json:"n_ballot"`           // stats: number of Ballots operations
	TokenGenMin        int64             `gorm:"column:token_gen_min"    json:"token_gen_min"` // hops
	TokenGenMax        int64             `gorm:"column:token_gen_max"    json:"token_gen_max"` // hops
	GracePeriod        int64             `gorm:"column:grace_period"    json:"grace_period"`   // deactivation cycle

	// used during block processing, not stored in DB
	IsNew      bool `gorm:"-" json:"-"` // first seen this block
	WasFunded  bool `gorm:"-" json:"-"` // true if account was funded before processing this block
	IsDirty    bool `gorm:"-" json:"-"` // indicates an update happened
	MustDelete bool `gorm:"-" json:"-"` // indicates the account should be deleted (during rollback)
}

func NewAccount(addr chain.Address) *Account {
	acc := AllocAccount()
	acc.Type = addr.Type
	acc.Hash = addr.Hash
	acc.IsNew = true
	acc.IsDirty = true
	acc.IsSpendable = addr.Type != chain.AddressTypeContract // tz1/2/3 spendable by default
	return acc
}

func AllocAccount() *Account {
	return accountPool.Get().(*Account)
}

func (a *Account) Free() {
	// skip for delegates because we keep them out of cache
	if a.IsDelegate {
		return
	}
	a.Reset()
	accountPool.Put(a)
}

func (a Account) ID() uint64 {
	return uint64(a.RowId)
}

func (a *Account) SetID(id uint64) {
	a.RowId = AccountID(id)
}

func (a Account) String() string {
	s, _ := chain.EncodeAddress(a.Type, a.Hash)
	return s
}

func (a Account) Address() chain.Address {
	return chain.NewAddress(a.Type, a.Hash)
}

func (a *Account) ManagerContract() (*Contract, error) {
	if a.Type != chain.AddressTypeContract {
		return nil, fmt.Errorf("account is not a contract")
	}
	c := AllocContract()
	c.Hash = make([]byte, len(a.Hash))
	copy(c.Hash, a.Hash)
	c.AccountId = a.RowId
	c.ManagerId = a.ManagerId
	c.IsSpendable = a.IsSpendable
	c.IsDelegatable = a.IsDelegatable
	return c, nil
}

func (a Account) Balance() int64 {
	b := a.FrozenBalance() + a.SpendableBalance
	if a.IsVesting {
		b += a.UnclaimedBalance
	}
	return b
}

func (a Account) FrozenBalance() int64 {
	return a.FrozenDeposits + a.FrozenFees + a.FrozenRewards
}

// own balance plus frozen deposits+fees (NOT REWARDS!) plus
// all delegated balances (this is self-delegation safe)
func (a Account) StakingBalance() int64 {
	return a.FrozenDeposits + a.FrozenFees + a.SpendableBalance + a.DelegatedBalance
}

func (a Account) Rolls(p *chain.Params) int64 {
	if p.TokensPerRoll == 0 {
		return 0
	}
	return a.StakingBalance() / p.TokensPerRoll
}

func (a *Account) Reset() {
	a.RowId = 0
	a.Hash = nil
	a.DelegateId = 0
	a.ManagerId = 0
	a.PubkeyHash = nil
	a.PubkeyType = 0
	a.Type = 0
	a.FirstIn = 0
	a.FirstOut = 0
	a.LastIn = 0
	a.LastOut = 0
	a.FirstSeen = 0
	a.LastSeen = 0
	a.DelegatedSince = 0
	a.DelegateSince = 0
	a.TotalReceived = 0
	a.TotalSent = 0
	a.TotalBurned = 0
	a.TotalFeesPaid = 0
	a.TotalRewardsEarned = 0
	a.TotalFeesEarned = 0
	a.TotalLost = 0
	a.FrozenDeposits = 0
	a.FrozenRewards = 0
	a.FrozenFees = 0
	a.UnclaimedBalance = 0
	a.SpendableBalance = 0
	a.DelegatedBalance = 0
	a.TotalDelegations = 0
	a.ActiveDelegations = 0
	a.IsFunded = false
	a.IsActivated = false
	a.IsVesting = false
	a.IsSpendable = false
	a.IsDelegatable = false
	a.IsDelegated = false
	a.IsRevealed = false
	a.IsDelegate = false
	a.IsActiveDelegate = false
	a.IsContract = false
	a.BlocksBaked = 0
	a.BlocksMissed = 0
	a.BlocksStolen = 0
	a.BlocksEndorsed = 0
	a.SlotsEndorsed = 0
	a.SlotsMissed = 0
	a.NOps = 0
	a.NOpsFailed = 0
	a.NTx = 0
	a.NDelegation = 0
	a.NOrigination = 0
	a.NProposal = 0
	a.NBallot = 0
	a.TokenGenMin = 0
	a.TokenGenMax = 0
	a.GracePeriod = 0
	a.IsNew = false
	a.WasFunded = false
	a.IsDirty = false
	a.MustDelete = false
}

func (a *Account) UpdateBalanceN(flows []*Flow) error {
	for _, f := range flows {
		if err := a.UpdateBalance(f); err != nil {
			return err
		}
	}
	return nil
}

func (a *Account) UpdateBalance(f *Flow) error {
	a.IsDirty = true

	switch f.Category {
	case FlowCategoryRewards:
		if a.FrozenRewards < f.AmountOut {
			return fmt.Errorf("acc.update id %d %s frozen rewards %d is smaller than "+
				"outgoing amount %d", a.RowId, a, a.FrozenRewards, f.AmountOut)
		}
		a.TotalRewardsEarned += f.AmountIn
		a.FrozenRewards += f.AmountIn - f.AmountOut
		if f.Operation == FlowTypeDenounciation {
			a.TotalLost += f.AmountOut
			a.TotalRewardsEarned -= f.AmountOut
		}
	case FlowCategoryDeposits:
		if a.FrozenDeposits < f.AmountOut {
			return fmt.Errorf("acc.update id %d %s frozen deposits %d is smaller than "+
				"outgoing amount %d", a.RowId, a, a.FrozenDeposits, f.AmountOut)
		}
		a.FrozenDeposits += f.AmountIn - f.AmountOut
		if f.Operation == FlowTypeDenounciation {
			a.TotalLost += f.AmountOut
		}
	case FlowCategoryFees:
		if a.FrozenFees < f.AmountOut {
			return fmt.Errorf("acc.update id %d %s frozen fees %d is smaller than "+
				"outgoing amount %d", a.RowId, a, a.FrozenFees, f.AmountOut)
		}
		if f.IsFrozen {
			a.TotalFeesEarned += f.AmountIn
		}
		if f.Operation == FlowTypeDenounciation {
			a.TotalLost += f.AmountOut
		}
		a.FrozenFees += f.AmountIn - f.AmountOut
	case FlowCategoryBalance:
		if a.SpendableBalance < f.AmountOut {
			// todo 由于隔空空头的1 个tzx没有记录进去，所以这里过滤掉
			if f.AmountOut-a.SpendableBalance > 1 {
				return fmt.Errorf("acc.update id %d %s balance %d is smaller than "+
					"outgoing amount %d", a.RowId, a, a.SpendableBalance, f.AmountOut)
			} else {
				a.SpendableBalance = f.AmountOut
			}
		}
		if f.IsFee {
			a.TotalFeesPaid += f.AmountOut
		}
		if f.IsBurned {
			a.TotalBurned += f.AmountOut
		}
		switch f.Operation {
		case FlowTypeTransaction, FlowTypeOrigination, FlowTypeAirdrop:
			// both transactions and originations can send funds
			// count send/received only for non-fee and non-burn flows
			if !f.IsBurned && !f.IsFee {
				a.TotalReceived += f.AmountIn
				a.TotalSent += f.AmountOut
				// update generation for in-flows
				if f.AmountIn > 0 {
					a.TokenGenMax = util.Max64(a.TokenGenMax, f.TokenGenMax+1)
					a.TokenGenMin = util.NonZeroMin64(a.TokenGenMin, f.TokenGenMin+1)
				}
			}
		case FlowTypeVest, FlowTypeActivation:
			if a.UnclaimedBalance < f.AmountIn {
				return fmt.Errorf("acc.update id %d %s unclaimed balance %d is smaller than "+
					"activated amount %d", a.RowId, a, a.UnclaimedBalance, f.AmountIn)
			}
			a.UnclaimedBalance -= f.AmountIn
		}
		a.SpendableBalance += f.AmountIn - f.AmountOut

	case FlowCategoryDelegation:
		if a.DelegatedBalance < f.AmountOut {
			return fmt.Errorf("acc.update id %d %s delegated balance %d is smaller than "+
				"outgoing amount %d", a.RowId, a, a.DelegatedBalance, f.AmountOut)
		}
		a.DelegatedBalance += f.AmountIn - f.AmountOut
	}

	// any flow except delegation balance updates and internal unfreeze/payouts
	// count towards account activity
	if f.Category != FlowCategoryDelegation && f.Operation != FlowTypeInternal {
		if f.AmountIn > 0 {
			a.LastIn = f.Height
			if a.FirstIn == 0 {
				a.FirstIn = a.LastIn
			}
		}
		if f.AmountOut > 0 {
			a.LastOut = f.Height
			if a.FirstOut == 0 {
				a.FirstOut = a.LastOut
			}
		}
	}

	a.IsFunded = (a.FrozenBalance() + a.SpendableBalance + a.UnclaimedBalance) > 0
	a.LastSeen = util.Max64N(a.LastSeen, a.LastIn, a.LastOut)

	// reset token generation
	if !a.IsFunded {
		a.IsRevealed = false
		a.TokenGenMin = 0
		a.TokenGenMax = 0
	}

	return nil
}

func (a *Account) RollbackBalanceN(flows []*Flow) error {
	for _, f := range flows {
		if err := a.RollbackBalance(f); err != nil {
			return err
		}
	}
	return nil
}

func (a *Account) RollbackBalance(f *Flow) error {
	a.IsDirty = true
	a.IsNew = a.FirstSeen == f.Height

	switch f.Category {
	case FlowCategoryRewards:
		if a.FrozenRewards < f.AmountIn {
			return fmt.Errorf("acc.update id %d %s frozen rewards %d is smaller than "+
				"reversed incoming amount %d", a.RowId, a, a.FrozenRewards, f.AmountIn)
		}
		a.TotalRewardsEarned -= f.AmountIn
		a.FrozenRewards -= f.AmountIn - f.AmountOut
		if f.Operation == FlowTypeDenounciation {
			a.TotalLost -= f.AmountOut
			a.TotalRewardsEarned += f.AmountOut
		}

	case FlowCategoryDeposits:
		if a.FrozenDeposits < f.AmountIn {
			return fmt.Errorf("acc.update id %d %s frozen deposits %d is smaller than "+
				"reversed incoming amount %d", a.RowId, a, a.FrozenDeposits, f.AmountIn)
		}
		a.FrozenDeposits -= f.AmountIn - f.AmountOut
		if f.Operation == FlowTypeDenounciation {
			a.TotalLost -= f.AmountOut
		}

	case FlowCategoryFees:
		if a.FrozenFees < f.AmountIn {
			return fmt.Errorf("acc.update id %d %s frozen fees %d is smaller than "+
				"reversed incoming amount %d", a.RowId, a, a.FrozenFees, f.AmountIn)
		}
		if f.IsFrozen {
			a.TotalFeesEarned -= f.AmountIn
		}
		if f.Operation == FlowTypeDenounciation {
			a.TotalLost -= f.AmountOut
		}
		a.FrozenFees -= f.AmountIn - f.AmountOut

	case FlowCategoryBalance:
		if a.SpendableBalance < f.AmountIn-f.AmountOut {
			return fmt.Errorf("acc.update id %d %s balance %d is smaller than "+
				"reversed incoming amount %d", a.RowId, a, a.SpendableBalance, f.AmountIn)
		}
		if f.IsFee {
			a.TotalFeesPaid -= f.AmountOut
		}
		if f.IsBurned {
			a.TotalBurned -= f.AmountOut
		}
		switch f.Operation {
		case FlowTypeTransaction, FlowTypeOrigination, FlowTypeAirdrop:
			a.TotalReceived -= f.AmountIn
			a.TotalSent -= f.AmountOut
			// FIXME: reverse generation update for in-flows lacks previous info
			// if f.AmountIn > 0 {
			// 	a.TokenGenMax = util.Max64(a.TokenGenMax, f.TokenGenMax+1)
			// 	a.TokenGenMin = util.NonZeroMin64(a.TokenGenMin, f.TokenGenMin+1)
			// }
		case FlowTypeVest, FlowTypeActivation:
			a.UnclaimedBalance += f.AmountIn
		}
		a.SpendableBalance -= f.AmountIn - f.AmountOut

	case FlowCategoryDelegation:
		if a.DelegatedBalance < f.AmountIn {
			return fmt.Errorf("acc.update id %d %s delegated balance %d is smaller than "+
				"reversed incoming amount %d", a.RowId, a, a.DelegatedBalance, f.AmountIn)
		}
		a.DelegatedBalance -= f.AmountIn - f.AmountOut
	}

	// skip activity updates (too complex to track previous in/out heights)
	// and rely on subsequent block updates, we still set LastSeen to the current
	// block
	a.IsFunded = (a.FrozenBalance() + a.SpendableBalance + a.UnclaimedBalance) > 0
	a.LastSeen = util.Min64(f.Height, util.Max64N(a.LastIn, a.LastOut))
	return nil
}

// init 11 cycles ahead of current cycle
func (a *Account) InitGracePeriod(cycle int64, params *chain.Params) {
	a.GracePeriod = cycle + 2*params.PreservedCycles + 1 // (11)
}

// keep initial (+11) max grace period, otherwise cycle + 6
func (a *Account) UpdateGracePeriod(cycle int64, params *chain.Params) {
	a.GracePeriod = util.Max64(cycle+params.PreservedCycles+1, a.GracePeriod)
}
