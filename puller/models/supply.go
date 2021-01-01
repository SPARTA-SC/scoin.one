// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package models

import (
	"tezos_index/chain"
	"time"
)

// 全网的token 供应量info
type Supply struct {
	RowId               uint64    `gorm:"primary_key;column:row_id" json:"row_id"`                // unique id
	Height              int64     `gorm:"column:height"    json:"height"`                         // bc: block height (also for orphans)
	Cycle               int64     `gorm:"column:cycle"    json:"cycle"`                           // bc: block cycle (tezos specific)
	Timestamp           time.Time `gorm:"column:time"    json:"time"`                             // bc: block creation time
	Total               int64     `gorm:"column:total"    json:"total"`                           // total available supply (including unclaimed)
	Activated           int64     `gorm:"column:activated"    json:"activated"`                   // activated fundraiser supply
	Unclaimed           int64     `gorm:"column:unclaimed"    json:"unclaimed"`                   // all non-activated fundraiser supply
	Vested              int64     `gorm:"column:vested"    json:"vested"`                         // foundation vested supply
	Unvested            int64     `gorm:"column:unvested"    json:"unvested"`                     // remaining unvested supply
	Circulating         int64     `gorm:"column:circulating"    json:"circulating"`               // able to move next block floating (total - unvested)
	Delegated           int64     `gorm:"column:delegated"    json:"delegated"`                   // all delegated balances
	Staking             int64     `gorm:"column:staking"    json:"staking"`                       // all delegated + delegate's own balances
	ActiveDelegated     int64     `gorm:"column:active_delegated"    json:"active_delegated"`     // delegated  balances to active delegates
	ActiveStaking       int64     `gorm:"column:active_staking"    json:"active_staking"`         // delegated + delegate's own balances for active delegates
	InactiveDelegated   int64     `gorm:"column:inactive_delegated"    json:"inactive_delegated"` // delegated  balances to inactive delegates
	InactiveStaking     int64     `gorm:"column:inactive_staking"    json:"inactive_staking"`     // delegated + delegate's own balances for inactive delegates
	Minted              int64     `gorm:"column:minted"    json:"minted"`
	MintedBaking        int64     `gorm:"column:minted_baking"    json:"minted_baking"`
	MintedEndorsing     int64     `gorm:"column:minted_endorsing"    json:"minted_endorsing"`
	MintedSeeding       int64     `gorm:"column:minted_seeding"    json:"minted_seeding"`
	MintedAirdrop       int64     `gorm:"column:minted_airdrop"    json:"minted_airdrop"`
	Burned              int64     `gorm:"column:burned"    json:"burned"`
	BurnedDoubleBaking  int64     `gorm:"column:burned_double_baking"    json:"burned_double_baking"`
	BurnedDoubleEndorse int64     `gorm:"column:burned_double_endorse"    json:"burned_double_endorse"`
	BurnedOrigination   int64     `gorm:"column:burned_origination"    json:"burned_origination"`
	BurnedImplicit      int64     `gorm:"column:burned_implicit"    json:"burned_implicit"`
	BurnedSeedMiss      int64     `gorm:"column:burned_seed_miss"    json:"burned_seed_miss"`
	Frozen              int64     `gorm:"column:frozen"    json:"frozen"`
	FrozenDeposits      int64     `gorm:"column:frozen_deposits"    json:"frozen_deposits"`
	FrozenRewards       int64     `gorm:"column:frozen_rewards"    json:"frozen_rewards"`
	FrozenFees          int64     `gorm:"column:frozen_fees"    json:"frozen_fees"`
}

func (s *Supply) ID() uint64 {
	return s.RowId
}

func (s *Supply) SetID(id uint64) {
	s.RowId = id
}

func (s *Supply) Update(b *Block, delegates map[AccountID]*Account) {
	s.RowId = 0 // force allocating new id
	s.Height = b.Height
	s.Cycle = b.Cycle
	s.Timestamp = b.Timestamp
	s.Total += b.Rewards - b.BurnedSupply
	s.Minted += b.Rewards
	s.Burned += b.BurnedSupply
	s.FrozenDeposits += b.Deposits - b.UnfrozenDeposits
	s.FrozenRewards += b.Rewards - b.UnfrozenRewards
	s.FrozenFees += b.Fees - b.UnfrozenFees
	s.Frozen = s.FrozenDeposits + s.FrozenFees + s.FrozenRewards

	// activated/unclaimed, vested/unvested, invoice/airdrop from flows
	for _, f := range b.Flows {
		switch f.Operation {
		case FlowTypeActivation:
			s.Activated += f.AmountIn
			s.Unclaimed -= f.AmountIn
		case FlowTypeVest:
			s.Vested += f.AmountIn
			s.Unvested -= f.AmountIn
		case FlowTypeNonceRevelation:
			// adjust different supply types because block.BurnedSupply contains
			// seed burn already, but it comes from frozen supply and not from
			// circulating supply
			if f.IsBurned {
				s.BurnedSeedMiss += f.AmountOut
				s.Frozen -= f.AmountOut
				switch f.Category {
				case FlowCategoryRewards:
					s.FrozenRewards -= f.AmountOut
				case FlowCategoryFees:
					s.FrozenFees -= f.AmountOut
				}
			}
		case FlowTypeInvoice, FlowTypeAirdrop:
			s.Total += f.AmountIn
			s.MintedAirdrop += f.AmountIn
			s.Minted += f.AmountIn
		}
	}

	// use ops to update bake and burn details
	for _, op := range b.Ops {
		switch op.Type {
		case chain.OpTypeSeedNonceRevelation:
			s.MintedSeeding += op.Reward
		case chain.OpTypeEndorsement:
			s.MintedEndorsing += op.Reward
		case chain.OpTypeDoubleBakingEvidence:
			s.BurnedDoubleBaking += op.Burned
		case chain.OpTypeDoubleEndorsementEvidence:
			s.BurnedDoubleEndorse += op.Burned
		case chain.OpTypeOrigination:
			s.BurnedOrigination += op.Burned
		case chain.OpTypeTransaction:
			s.BurnedImplicit += op.Burned
		}
	}
	// update supply totals across all delegates
	s.Staking = 0
	s.Delegated = 0
	s.ActiveStaking = 0
	s.ActiveDelegated = 0
	s.InactiveStaking = 0
	s.InactiveDelegated = 0
	for _, acc := range delegates {
		sb, db := acc.StakingBalance(), acc.DelegatedBalance
		s.Staking += sb
		s.Delegated += db
		if acc.IsActiveDelegate {
			s.ActiveStaking += sb
			s.ActiveDelegated += db
		} else {
			s.InactiveStaking += sb
			s.InactiveDelegated += db
		}
	}

	// we don't explicitly count baking and there's also no explicit op, but
	// we can calculate total baking rewards as difference to total rewards
	s.MintedBaking = s.Minted - s.MintedSeeding - s.MintedEndorsing - s.MintedAirdrop

	// unanimous consent that unclaimed can move at next block and frozen is
	// generally considered as part of circulating
	s.Circulating = s.Total - s.Unvested
}

func (s *Supply) Rollback(b *Block) {
	// update identity only
	s.Height = b.Height
	s.Cycle = b.Cycle
}
