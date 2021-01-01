// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package models

import (
	"time"
)

type Chain struct {
	RowId              uint64    `gorm:"primary_key;column:row_id"   json:"row_id"`                                                // unique id
	Height             int64     `gorm:"column:height"      json:"height"`                                                         // bc: block height (also for orphans)
	Cycle              int64     `gorm:"column:cycle"      json:"cycle"`                                                           // bc: block cycle (tezos specific)
	Timestamp          time.Time `gorm:"column:time"      json:"time"`                                                             // bc: block creation time
	TotalAccounts      int64     `gorm:"column:total_accounts"      json:"total_accounts"`                                         // default accounts
	TotalImplicit      int64     `gorm:"column:total_implicit"      json:"total_implicit"`                                         // implicit accounts
	TotalManaged       int64     `gorm:"column:total_managed"      json:"total_managed"`                                           // managed/originated accounts for delegation (KT1 without code)
	TotalContracts     int64     `gorm:"column:total_contracts"      json:"total_contracts"`                                       // smart contracts (KT1 with code)
	TotalOps           int64     `gorm:"column:total_ops"      json:"total_ops"`                                                   //
	TotalContractOps   int64     `gorm:"column:total_contract_ops"      json:"total_contract_ops"`                                 // ops on KT1 contracts
	TotalActivations   int64     `gorm:"column:total_activations"      json:"total_activations"`                                   // fundraiser accounts activated
	TotalSeedNonces    int64     `gorm:"column:total_seed_nonce_revelations"      json:"total_seed_nonce_revelations"`             //
	TotalEndorsements  int64     `gorm:"column:total_endorsements"      json:"total_endorsements"`                                 //
	TotalDoubleBake    int64     `gorm:"column:total_double_baking_evidences"      json:"total_double_baking_evidences"`           //
	TotalDoubleEndorse int64     `gorm:"column:total_double_endorsement_evidences"      json:"total_double_endorsement_evidences"` //
	TotalDelegations   int64     `gorm:"column:total_delegations"      json:"total_delegations"`                                   //
	TotalReveals       int64     `gorm:"column:total_reveals"      json:"total_reveals"`                                           //
	TotalOriginations  int64     `gorm:"column:total_originations"      json:"total_originations"`                                 //
	TotalTransactions  int64     `gorm:"column:total_transactions"      json:"total_transactions"`                                 //
	TotalProposals     int64     `gorm:"column:total_proposals"      json:"total_proposals"`                                       //
	TotalBallots       int64     `gorm:"column:total_ballots"      json:"total_ballots"`                                           //
	TotalStorageBytes  int64     `gorm:"column:total_storage_bytes"      json:"total_storage_bytes"`                               //
	TotalPaidBytes     int64     `gorm:"column:total_paid_bytes"      json:"total_paid_bytes"`                                     // storage paid for KT1 and contract store
	TotalUsedBytes     int64     `gorm:"column:total_used_bytes"     json:"total_used_bytes"`                                      // ? (used by txscan.io)
	TotalOrphans       int64     `gorm:"column:total_orphans"      json:"total_orphans"`                                           // alternative block headers
	FundedAccounts     int64     `gorm:"column:funded_accounts"      json:"funded_accounts"`                                       // total number of accounts qith non-zero balance
	UnclaimedAccounts  int64     `gorm:"column:unclaimed_accounts"      json:"unclaimed_accounts"`                                 // fundraiser accounts unclaimed
	TotalDelegators    int64     `gorm:"column:total_delegators"      json:"total_delegators"`                                     // count of all non-zero balance delegators
	ActiveDelegators   int64     `gorm:"column:active_delegators"      json:"active_delegators"`                                   // KT1 delegating to active delegates
	InactiveDelegators int64     `gorm:"column:inactive_delegators"      json:"inactive_delegators"`                               // KT1 delegating to inactive delegates
	TotalDelegates     int64     `gorm:"column:total_delegates"      json:"total_delegates"`                                       // count of all delegates (active and inactive)
	ActiveDelegates    int64     `gorm:"column:active_delegates"      json:"active_delegates"`                                     // tz* active delegates
	InactiveDelegates  int64     `gorm:"column:inactive_delegates"      json:"inactive_delegates"`                                 // tz* inactive delegates
	ZeroDelegates      int64     `gorm:"column:zero_delegates"      json:"zero_delegates"`                                         // tz* delegate with zero staking balance
	SelfDelegates      int64     `gorm:"column:self_delegates"      json:"self_delegates"`                                         // tz* delegate with no incoming delegation
	SingleDelegates    int64     `gorm:"column:single_delegates"      json:"single_delegates"`                                     // tz* delegate with 1 incoming delegation
	MultiDelegates     int64     `gorm:"column:multi_delegates"      json:"multi_delegates"`                                       // tz* delegate with >1 incoming delegations (delegation services)
	Rolls              int64     `gorm:"column:rolls"      json:"rolls"`                                                           // total sum of rolls (delegated_balance/10,000)
	RollOwners         int64     `gorm:"column:roll_owners"      json:"roll_owners"`                                               // distinct delegates
}

func (c *Chain) ID() uint64 {
	return c.RowId
}

func (c *Chain) SetID(id uint64) {
	c.RowId = id
}

func (c *Chain) Update(b *Block, delegates map[AccountID]*Account) {
	c.RowId = 0 // force allocating new id
	c.Height = b.Height
	c.Cycle = b.Cycle
	c.Timestamp = b.Timestamp
	c.TotalAccounts += int64(b.NewAccounts)
	c.TotalImplicit += int64(b.NewImplicitAccounts)
	c.TotalManaged += int64(b.NewManagedAccounts)
	c.TotalContracts += int64(b.NewContracts)
	c.TotalOps += int64(b.NOps)
	c.TotalContractOps += int64(b.NOpsContract)
	c.TotalActivations += int64(b.NActivation)
	c.UnclaimedAccounts -= int64(b.NActivation)
	c.TotalSeedNonces += int64(b.NSeedNonce)
	c.TotalEndorsements += int64(b.NEndorsement)
	c.TotalDoubleBake += int64(b.N2Baking)
	c.TotalDoubleEndorse += int64(b.N2Endorsement)
	c.TotalDelegations += int64(b.NDelegation)
	c.TotalReveals += int64(b.NReveal)
	c.TotalOriginations += int64(b.NOrigination)
	c.TotalTransactions += int64(b.NTx)
	c.TotalProposals += int64(b.NProposal)
	c.TotalBallots += int64(b.NBallot)
	c.TotalStorageBytes += int64(b.StorageSize)
	for _, op := range b.Ops {
		c.TotalPaidBytes += op.StoragePaid
	}
	c.FundedAccounts += int64(b.FundedAccounts - b.ClearedAccounts)

	// from delegates
	c.TotalDelegators = 0
	c.ActiveDelegators = 0
	c.InactiveDelegators = 0
	c.TotalDelegates = 0
	c.ActiveDelegates = 0
	c.InactiveDelegates = 0
	c.ZeroDelegates = 0
	c.SelfDelegates = 0
	c.SingleDelegates = 0
	c.MultiDelegates = 0
	c.Rolls = 0
	c.RollOwners = 0
	for _, acc := range delegates {
		// sanity checks
		if !acc.IsDelegate {
			continue
		}

		// all delegators, except when they have zero balance
		c.TotalDelegators += int64(acc.ActiveDelegations)
		c.TotalDelegates++

		// count only active delegates going forward
		if !acc.IsActiveDelegate {
			c.InactiveDelegates++
			c.InactiveDelegators += int64(acc.ActiveDelegations)
			continue
		}
		c.ActiveDelegates++
		c.ActiveDelegators += int64(acc.ActiveDelegations)

		switch acc.ActiveDelegations {
		case 0:
			c.SelfDelegates++
		case 1:
			c.SingleDelegates++
		default:
			c.MultiDelegates++
		}

		// only active delegates get rolls
		bal := acc.StakingBalance()
		if bal == 0 {
			c.ZeroDelegates++
		} else if bal >= b.Params.TokensPerRoll {
			c.Rolls += bal / b.Params.TokensPerRoll
			c.RollOwners++
		}
	}

	if b.IsOrphan {
		c.TotalOrphans++
	}

	// TODO
	// s.TotalUsedBytes +=
}

func (c *Chain) Rollback(b *Block) {
	// update identity only
	c.Height = b.Height
	c.Cycle = b.Cycle
}
