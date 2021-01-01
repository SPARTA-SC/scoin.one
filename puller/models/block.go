// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package models

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/jinzhu/gorm"
	"math/bits"
	"sync"
	"tezos_index/chain"
	"tezos_index/rpc"
	"tezos_index/utils"
	"time"
)

const BlockCacheLineSize = 320

var (
	blockPool *sync.Pool
)

func init() {
	blockPool = &sync.Pool{
		New: func() interface{} { return new(Block) },
	}
}

// Block contains extracted and translated data describing a Tezos block. Block also
// contains raw data and translations for related types such as operations, chain totals
// rights, etc. that is used by indexers
type Block struct {
	RowId               uint64                 `gorm:"primary_key;unique_index;column:row_id"   json:"row_id"`                         // internal: id, not height!
	ParentId            uint64                 `gorm:"column:parent_id"      json:"parent_id"`                                         // internal: parent block id
	Hash                chain.StrBHash         `gorm:"column:hash;index:hash"             json:"hash"`                                 // bc: block hash
	IsOrphan            bool                   `gorm:"column:is_orphan"      json:"is_orphan,omitempty"`                               // internal: valid or orphan state
	Height              int64                  `gorm:"column:height;unique;index:height"      json:"height"`                           // bc: block height (also for orphans)
	Cycle               int64                  `gorm:"column:cycle"      json:"cycle"`                                                 // bc: block cycle (tezos specific)
	IsCycleSnapshot     bool                   `gorm:"column:is_cycle_snapshot"      json:"is_cycle_snapshot"`                         // will be set when snapshot index is determined
	Timestamp           time.Time              `gorm:"column:time"      json:"time"`                                                   // bc: block creation time
	Solvetime           int                    `gorm:"column:solvetime"      json:"solvetime"`                                         // stats: time to solve this block in seconds
	Version             int                    `gorm:"column:version"      json:"version"`                                             // bc: block version (mapped from tezos protocol version)
	Validation          int                    `gorm:"column:validation_pass"      json:"validation_pass"`                             // bc: tezos validation pass
	Fitness             uint64                 `gorm:"column:fitness"      json:"fitness"`                                             // bc: block fitness bits
	Priority            int                    `gorm:"column:priority"      json:"priority"`                                           // bc: baker priority
	Nonce               uint64                 `gorm:"column:nonce"      json:"nonce"`                                                 // bc: block nonce
	VotingPeriodKind    chain.VotingPeriodKind `gorm:"column:voting_period_kind"      json:"voting_period_kind"`                       // bc: tezos voting period (enum)
	BakerId             AccountID              `gorm:"column:baker_id"      json:"baker_id"`                                           // bc: block baker (address id)
	SlotsEndorsed       uint32                 `gorm:"column:endorsed_slots"      json:"endorsed_slots"`                               // bc: slots that were endorsed by the following block
	NSlotsEndorsed      int                    `gorm:"column:n_endorsed_slots"      json:"n_endorsed_slots"`                           // stats: successful endorsed slots
	NOps                int                    `gorm:"column:n_ops"      json:"n_ops"`                                                 // stats: successful operation count
	NOpsFailed          int                    `gorm:"column:n_ops_failed"      json:"n_ops_failed"`                                   // stats: failed operation coiunt
	NOpsContract        int                    `gorm:"column:n_ops_contract"      json:"n_ops_contract"`                               // stats: successful contract operation count
	NTx                 int                    `gorm:"column:n_tx"     json:"n_tx"`                                                    // stats: number of Tx operations
	NActivation         int                    `gorm:"column:n_activation"      json:"n_activation"`                                   // stats: number of Activations operations
	NSeedNonce          int                    `gorm:"column:n_seed_nonce_revelation"      json:"n_seed_nonce_revelation"`             // stats: number of Nonce Revelations operations
	N2Baking            int                    `gorm:"column:n_double_baking_evidence"      json:"n_double_baking_evidence"`           // stats: number of 2Baking operations
	N2Endorsement       int                    `gorm:"column:n_double_endorsement_evidence"      json:"n_double_endorsement_evidence"` // stats: number of 2Endorsement operations
	NEndorsement        int                    `gorm:"column:n_endorsement"      json:"n_endorsement"`                                 // stats: number of Endorsements operations
	NDelegation         int                    `gorm:"column:n_delegation"      json:"n_delegation"`                                   // stats: number of Delegations operations
	NReveal             int                    `gorm:"column:n_reveal"      json:"n_reveal"`                                           // stats: number of Reveals operations
	NOrigination        int                    `gorm:"column:n_origination"      json:"n_origination"`                                 // stats: number of Originations operations
	NProposal           int                    `gorm:"column:n_proposal"      json:"n_proposal"`                                       // stats: number of Proposals operations
	NBallot             int                    `gorm:"column:n_ballot"      json:"n_ballot"`                                           // stats: number of Ballots operations
	Volume              int64                  `gorm:"column:volume"      json:"volume"`                                               // stats: sum of transacted amount
	Fees                int64                  `gorm:"column:fees"      json:"fees"`                                                   // stats: sum of transaction fees
	Rewards             int64                  `gorm:"column:rewards"      json:"rewards"`                                             // stats: baking and endorsement rewards
	Deposits            int64                  `gorm:"column:deposits"      json:"deposits"`                                           // stats: bond deposits for baking and endorsement
	UnfrozenFees        int64                  `gorm:"column:unfrozen_fees"      json:"unfrozen_fees"`                                 // stats: total unfrozen fees
	UnfrozenRewards     int64                  `gorm:"column:unfrozen_rewards"      json:"unfrozen_rewards"`                           // stats: total unfrozen rewards
	UnfrozenDeposits    int64                  `gorm:"column:unfrozen_deposits"      json:"unfrozen_deposits"`                         // stats: total unfrozen deposits
	ActivatedSupply     int64                  `gorm:"column:activated_supply"      json:"activated_supply"`                           // stats: new activated supply
	BurnedSupply        int64                  `gorm:"column:burned_supply"      json:"burned_supply"`                                 // stats: burned tezos
	SeenAccounts        int                    `gorm:"column:n_accounts"      json:"n_accounts"`                                       // stats: count of unique accounts
	NewAccounts         int                    `gorm:"column:n_new_accounts"      json:"n_new_accounts"`                               // stats: count of new accounts
	NewImplicitAccounts int                    `gorm:"column:n_new_implicit"      json:"n_new_implicit"`                               // stats: count of new implicit tz1,2,3 accounts
	NewManagedAccounts  int                    `gorm:"column:n_new_managed"      json:"n_new_managed"`                                 // stats: count of new managed (KT1 without code) accunts
	NewContracts        int                    `gorm:"column:n_new_contracts"      json:"n_new_contracts"`                             // stats: count of new contracts (KT1 with code)
	ClearedAccounts     int                    `gorm:"column:n_cleared_accounts"      json:"n_cleared_accounts"`                       // stats: count of zero balance acc at end of block that were funded before
	FundedAccounts      int                    `gorm:"column:n_funded_accounts"      json:"n_funded_accounts"`                         // stats: count of (re)funded acc at end of block (new or previously cleared aacc)
	GasLimit            int64                  `gorm:"column:gas_limit"      json:"gas_limit"`                                         // stats: total gas limit this block
	GasUsed             int64                  `gorm:"column:gas_used"      json:"gas_used"`                                           // stats: total gas used this block
	GasPrice            float64                `gorm:"column:gas_price"      json:"gas_price"`                                         // stats: gas price in tezos per unit gas
	StorageSize         int64                  `gorm:"column:storage_size"      json:"storage_size"`                                   // stats: total new storage size allocated
	TDD                 float64                `gorm:"column:days_destroyed"  json:"days_destroyed"`                                   // stats: token days destroyed (from last-in time to spend)

	// other tz or extracted/translated data for processing
	TZ     *Bundle       `gorm:"-" json:"-"`
	Params *chain.Params `gorm:"-" json:"-"`
	Chain  *Chain        `gorm:"-" json:"-"`
	Supply *Supply       `gorm:"-" json:"-"`
	Ops    []*Op         `gorm:"-" json:"-"`
	Flows  []*Flow       `gorm:"-" json:"-"`
	Baker  *Account      `gorm:"-" json:"-"`
	Parent *Block        `gorm:"-" json:"-"`
}

func UpdateBlock(b *Block, db *gorm.DB) error {
	data := make(map[string]interface{})
	data["parent_id"] = b.ParentId
	data["hash"] = b.Hash
	data["is_orphan"] = b.IsOrphan
	data["height"] = b.Height
	data["cycle"] = b.Cycle
	data["is_cycle_snapshot"] = b.IsCycleSnapshot
	data["time"] = b.Timestamp
	data["solvetime"] = b.Solvetime
	data["version"] = b.Version
	data["validation_pass"] = b.Validation
	data["fitness"] = b.Fitness
	data["priority"] = b.Priority
	data["nonce"] = b.Nonce
	data["voting_period_kind"] = b.VotingPeriodKind
	data["baker_id"] = b.BakerId
	data["endorsed_slots"] = b.SlotsEndorsed
	data["n_endorsed_slots"] = b.NSlotsEndorsed
	data["n_ops"] = b.NOps
	data["n_ops_failed"] = b.NOpsFailed
	data["n_ops_contract"] = b.NOpsContract
	data["n_tx"] = b.NTx
	data["n_activation"] = b.NActivation
	data["n_seed_nonce_revelation"] = b.NSeedNonce
	data["n_double_baking_evidence"] = b.N2Baking
	data["n_double_endorsement_evidence"] = b.N2Endorsement
	data["n_endorsement"] = b.NEndorsement
	data["n_delegation"] = b.NDelegation
	data["n_reveal"] = b.NReveal
	data["n_origination"] = b.NOrigination
	data["n_proposal"] = b.NProposal
	data["n_ballot"] = b.NBallot
	data["volume"] = b.Volume
	data["fees"] = b.Fees
	data["rewards"] = b.Rewards
	data["deposits"] = b.Deposits
	data["unfrozen_fees"] = b.UnfrozenFees
	data["unfrozen_rewards"] = b.UnfrozenRewards
	data["unfrozen_deposits"] = b.UnfrozenDeposits
	data["activated_supply"] = b.ActivatedSupply
	data["burned_supply"] = b.BurnedSupply
	data["n_accounts"] = b.SeenAccounts
	data["n_new_accounts"] = b.NewAccounts
	data["n_new_implicit"] = b.NewImplicitAccounts
	data["n_new_managed"] = b.NewManagedAccounts
	data["n_new_contracts"] = b.NewContracts
	data["n_cleared_accounts"] = b.ClearedAccounts
	data["n_funded_accounts"] = b.FundedAccounts
	data["gas_limit"] = b.GasLimit
	data["gas_used"] = b.GasUsed
	data["gas_price"] = b.GasPrice
	data["storage_size"] = b.StorageSize
	data["days_destroyed"] = b.TDD

	return db.Model(&Block{}).Where("row_id = ?", b.RowId).Updates(data).Error
}

func (b Block) ID() uint64 {
	return b.RowId
}

func (b *Block) SetID(id uint64) {
	b.RowId = id
}

func AllocBlock() *Block {
	return blockPool.Get().(*Block)
}

func NewBlock(tz *Bundle, parent *Block) (*Block, error) {
	b := AllocBlock()
	if tz == nil || tz.Block == nil {
		return nil, fmt.Errorf("block init: missing rpc block")
	}

	b.TZ = tz
	b.Params = tz.Params
	b.Chain = &Chain{}
	b.Supply = &Supply{}
	if b.Ops == nil {
		b.Ops = make([]*Op, 0)
	}
	if b.Flows == nil {
		b.Flows = make([]*Flow, 0)
	}

	// init block model from rpc block and local data (expecing defaults for unset fields)
	b.Height = tz.Block.Header.Level
	b.Cycle = tz.Block.Metadata.Level.Cycle
	b.Timestamp = tz.Block.Header.Timestamp
	b.Hash = chain.StrBHash(tz.Block.Hash.String())
	b.Version = tz.Block.Header.Proto
	b.Validation = tz.Block.Header.ValidationPass
	b.Priority = tz.Block.Header.Priority
	if len(tz.Block.Header.ProofOfWorkNonce) >= 8 {
		b.Nonce = binary.BigEndian.Uint64(tz.Block.Header.ProofOfWorkNonce)
	}

	// adjust protocol version number for genesis and bootstrap blocks
	if b.Height <= 1 {
		b.Version--
	}

	// be robust against missing voting period (like on block 0 and 1)
	b.VotingPeriodKind = tz.Block.Metadata.VotingPeriodKind
	if !b.VotingPeriodKind.IsValid() {
		if parent != nil {
			b.VotingPeriodKind = parent.VotingPeriodKind
		} else {
			b.VotingPeriodKind = chain.VotingPeriodProposal
		}
	}

	// take the longest and highest hex value which represents the max height + #(endorsements) + 1
	if l := len(tz.Block.Header.Fitness); l > 0 {
		b.Fitness = binary.BigEndian.Uint64(tz.Block.Header.Fitness[l-1])
	}

	// parent info
	if parent != nil {
		b.ParentId = parent.RowId
		b.Parent = parent
		b.Solvetime = util.Max(0, int(b.Timestamp.Sub(parent.Timestamp)/time.Second))
		if parent.Chain != nil {
			*b.Chain = *parent.Chain // copy
		}
		if parent.Supply != nil {
			*b.Supply = *parent.Supply // copy
		}
	}

	return b, nil
}

func (b *Block) FetchRPC(ctx context.Context, c *rpc.Client) error {
	bHash, _ := chain.ParseBlockHash(b.Hash.String())
	if !bHash.IsValid() {
		return fmt.Errorf("invalid block hash on block id %d", b.RowId)
	}
	var err error
	if b.TZ == nil {
		b.TZ = &Bundle{}
	}
	if b.TZ.Block == nil {
		b.TZ.Block, err = c.GetBlock(ctx, bHash)
		if err != nil {
			return err
		}
	}
	if b.Params == nil {
		// fetch params from chain
		if b.Height > 0 {
			cons, err := c.GetConstantsHeight(ctx, b.Height)
			if err != nil {
				return fmt.Errorf("block init: %v", err)
			}
			b.Params = cons.MapToChainParams()
		} else {
			b.Params = chain.NewParams()
		}
		b.Params = b.Params.
			ForProtocol(b.TZ.Block.Protocol).
			ForNetwork(b.TZ.Block.ChainId)
		b.Params.Deployment = b.TZ.Block.Header.Proto
	}
	b.TZ.Params = b.Params
	// start fetching more rights at cycle 2 (look-ahead is 5)
	if b.Height >= b.Params.CycleStartHeight(2) && b.Params.IsCycleStart(b.Height) {
		// snapshot index and rights for future cycle N; the snapshot index
		// refers to a snapshot block taken in cycle N-7 and randomness
		// collected from seed_nonce_revelations during cycle N-6; N is the
		// farthest future cycle that exists.
		//
		// Note that for consistency and due to an off-by-one error in Tezos RPC
		// nodes we fetch snapshot index and rights at the start of cycle N-5 even
		// though they are created at the end of N-6!
		cycle := b.Cycle + b.Params.PreservedCycles
		b.TZ.Baking, err = c.GetBakingRightsCycle(ctx, b.Height, cycle)
		if err != nil {
			return fmt.Errorf("fetching baking rights for cycle %d: %v", cycle, err)
		}
		b.TZ.Endorsing, err = c.GetEndorsingRightsCycle(ctx, b.Height, cycle)
		if err != nil {
			return fmt.Errorf("fetching endorsing rights for cycle %d: %v", cycle, err)
		}
		b.TZ.Snapshot, err = c.GetSnapshotIndexCycle(ctx, b.Height, cycle)
		if err != nil {
			return fmt.Errorf("fetching snapshot for cycle %d: %v", cycle, err)
		}
	}
	return nil
}

func (b *Block) IsProtocolUpgrade() bool {
	if b.Parent == nil || b.Parent.TZ == nil || b.TZ == nil {
		return false
	}
	return !b.Parent.TZ.Block.Metadata.Protocol.IsEqual(b.TZ.Block.Metadata.Protocol)
}

func (b *Block) GetRPCOp(opn, opc int) (rpc.Operation, bool) {
	for _, ol := range b.TZ.Block.Operations {
		for _, o := range ol {
			if opn == 0 {
				if len(o.Contents) > opc {
					return o.Contents[opc], true
				}
				return nil, false
			}
			opn--
		}
	}
	return nil, false
}

func (b *Block) Age(height int64) int64 {
	// instead of real time we use block offsets and the target time
	// between blocks as time diff
	return (b.Height - height) * int64(b.Params.TimeBetweenBlocks[0]/time.Second)
}

func (b *Block) BlockReward(p *chain.Params) int64 {
	blockReward := p.BlockReward
	if b.Cycle < p.NoRewardCycles {
		blockReward = 0
	}

	if p.Version < 5 {
		return blockReward
	}

	// count number of included endorsements
	var nEndorsements int
	for _, op := range b.Ops {
		if op.Type != chain.OpTypeEndorsement {
			continue
		}
		eop, _ := b.GetRPCOp(op.OpN, op.OpC)
		nEndorsements += len(eop.(*rpc.EndorsementOp).Metadata.Slots)
	}

	if p.Version == 5 {
		// in v5
		// The baking reward is now calculated w.r.t a given priority [p] and a
		// number [e] of included endorsements as follows:
		endorseFactor := 0.8 + 0.2*float64(nEndorsements)/float64(p.EndorsersPerBlock)
		blockReward = int64(float64(blockReward) / float64(b.Priority+1) * endorseFactor)

	} else {
		// starting at v6
		// baking_reward_per_endorsement represent the reward you get (per endorsement
		// included) for baking at priority 0 (1st elem of the list) and the reward
		// for prio 1 and more (2nd elem). endorsement_reward is the same : reward for
		// endorsing blocks of prio 0 and 1+
		baseReward := p.BlockRewardV6[0]
		if b.Priority > 0 {
			baseReward = p.BlockRewardV6[1]
		}
		blockReward = int64(nEndorsements) * baseReward
	}
	return blockReward
}

func (b *Block) Free() {
	b.Reset()
	blockPool.Put(b)
}

func (b *Block) Reset() {
	b.RowId = 0
	b.ParentId = 0
	b.Hash = chain.StrBHash(chain.ZeroHash.String())
	b.IsOrphan = false
	b.Height = 0
	b.Cycle = 0
	b.IsCycleSnapshot = false
	b.Timestamp = time.Time{}
	b.Solvetime = 0
	b.Version = 0
	b.Validation = 0
	b.Fitness = 0
	b.Priority = 0
	b.Nonce = 0
	b.VotingPeriodKind = 0
	b.BakerId = 0
	b.SlotsEndorsed = 0
	b.NSlotsEndorsed = 0
	b.NOps = 0
	b.NOpsFailed = 0
	b.NOpsContract = 0
	b.NTx = 0
	b.NActivation = 0
	b.NSeedNonce = 0
	b.N2Baking = 0
	b.N2Endorsement = 0
	b.NEndorsement = 0
	b.NDelegation = 0
	b.NReveal = 0
	b.NOrigination = 0
	b.NProposal = 0
	b.NBallot = 0
	b.Volume = 0
	b.Fees = 0
	b.Rewards = 0
	b.Deposits = 0
	b.UnfrozenFees = 0
	b.UnfrozenRewards = 0
	b.UnfrozenDeposits = 0
	b.ActivatedSupply = 0
	b.BurnedSupply = 0
	b.SeenAccounts = 0
	b.NewAccounts = 0
	b.NewImplicitAccounts = 0
	b.NewManagedAccounts = 0
	b.NewContracts = 0
	b.ClearedAccounts = 0
	b.FundedAccounts = 0
	b.GasLimit = 0
	b.GasUsed = 0
	b.GasPrice = 0
	b.StorageSize = 0
	b.TDD = 0
	b.TZ = nil
	b.Params = nil
	b.Chain = nil
	b.Supply = nil
	b.Baker = nil
	b.Parent = nil
	if b.Ops != nil {
		for _, op := range b.Ops {
			op.Free()
		}
		b.Ops = b.Ops[:0]
	}
	if b.Flows != nil {
		for _, f := range b.Flows {
			f.Free()
		}
		b.Flows = b.Flows[:0]
	}
}

func (b *Block) Update(accounts, delegates map[AccountID]*Account) {
	// initial state
	// b.IsOrphan = false // don't set here to reuse this function for side-chain blocks
	b.NOps = 0
	b.NOpsFailed = 0
	b.NTx = 0
	b.NActivation = 0
	b.NReveal = 0
	b.N2Baking = 0
	b.N2Endorsement = 0
	b.NEndorsement = 0
	b.NDelegation = 0
	b.NReveal = 0
	b.NOrigination = 0
	b.NProposal = 0
	b.NBallot = 0
	b.Volume = 0
	b.Fees = 0
	b.Rewards = 0
	b.Deposits = 0
	b.UnfrozenFees = 0
	b.UnfrozenRewards = 0
	b.UnfrozenDeposits = 0
	b.ActivatedSupply = 0
	b.BurnedSupply = 0
	b.SeenAccounts = 0
	b.NewAccounts = 0
	b.NewImplicitAccounts = 0
	b.NewManagedAccounts = 0
	b.NewContracts = 0
	b.ClearedAccounts = 0
	b.FundedAccounts = 0
	b.GasLimit = 0
	b.GasUsed = 0
	b.GasPrice = 0
	b.StorageSize = 0
	b.TDD = 0

	var slotsEndorsed uint32

	for _, op := range b.Ops {
		if op.IsSuccess {
			b.Volume += op.Volume
		}
		b.Fees += op.Fee
		b.Rewards += op.Reward
		b.Deposits += op.Deposit
		b.BurnedSupply += op.Burned
		b.TDD += op.TDD
		b.GasLimit += op.GasLimit
		b.GasUsed += op.GasUsed
		b.StorageSize += op.StorageSize

		if op.IsContract {
			b.NOpsContract++
		}
		if !op.IsSuccess {
			b.NOpsFailed++
		}
		switch op.Type {
		case chain.OpTypeActivateAccount:
			b.NActivation++
			b.ActivatedSupply += op.Volume
		case chain.OpTypeDoubleBakingEvidence:
			b.N2Baking++
		case chain.OpTypeDoubleEndorsementEvidence:
			b.N2Endorsement++
		case chain.OpTypeSeedNonceRevelation:
			b.NSeedNonce++
		case chain.OpTypeTransaction:
			b.NTx++
		case chain.OpTypeOrigination:
			b.NOrigination++
		case chain.OpTypeDelegation:
			b.NDelegation++
		case chain.OpTypeReveal:
			b.NReveal++
		case chain.OpTypeEndorsement:
			b.NEndorsement++
			eop, _ := b.GetRPCOp(op.OpN, op.OpC)
			for _, v := range eop.(*rpc.EndorsementOp).Metadata.Slots {
				slotsEndorsed |= 1 << uint(v)
			}
		case chain.OpTypeProposals:
			b.NProposal++
		case chain.OpTypeBallot:
			b.NBallot++
		}
	}

	if b.Parent != nil {
		b.Parent.SlotsEndorsed = slotsEndorsed
		b.Parent.NSlotsEndorsed = bits.OnesCount32(slotsEndorsed)
	}

	b.NOps = len(b.Ops)
	if b.GasUsed > 0 && b.Fees > 0 {
		b.GasPrice = float64(b.Fees) / float64(b.GasUsed)
	}

	// some updates are not reflected in operations (e.g. baking, airdrops) so we
	// have to look at flows too
	for _, f := range b.Flows {
		switch f.Operation {
		case FlowTypeBaking:
			switch f.Category {
			case FlowCategoryRewards:
				b.Rewards += f.AmountIn
			case FlowCategoryDeposits:
				b.Deposits += f.AmountIn
			}
		case FlowTypeInternal:
			if f.IsUnfrozen {
				switch f.Category {
				case FlowCategoryDeposits:
					b.UnfrozenDeposits += f.AmountOut
				case FlowCategoryRewards:
					b.UnfrozenRewards += f.AmountOut
				case FlowCategoryFees:
					b.UnfrozenFees += f.AmountOut
				}
			}
		case FlowTypeNonceRevelation:
			// seed nonce burn is no operation, but still creates a flow
			if f.IsBurned {
				// technically this happends at the end of parent block
				// but we have no process in place to run updates after
				// all indexers have run
				b.BurnedSupply += f.AmountOut
			}
		case FlowTypeVest:
			b.ActivatedSupply += f.AmountIn
		}
	}

	// count account changes
	for _, acc := range accounts {
		if acc.IsNew && acc.IsDirty {
			if acc.IsDelegate {
				// see below
				continue
			}
			b.NewAccounts++
			switch true {
			case acc.IsContract:
				b.NewContracts++
			case acc.ManagerId != 0:
				b.NewManagedAccounts++
			default:
				b.NewImplicitAccounts++
			}
		}
		if acc.LastSeen == b.Height {
			b.SeenAccounts++
			if !acc.IsFunded {
				if acc.WasFunded {
					b.ClearedAccounts++
				}
			} else {
				if !acc.WasFunded {
					b.FundedAccounts++
				}
			}
		}
	}

	// handle (new/updated) delegates separate
	// delegates are not in accMap even if they are part of a regular op
	for _, acc := range delegates {
		if acc.IsNew {
			b.NewAccounts++
			b.NewImplicitAccounts++
		}
		if acc.LastSeen == b.Height {
			b.SeenAccounts++
			if !acc.IsFunded {
				if acc.WasFunded {
					b.ClearedAccounts++
				}
			} else {
				if !acc.WasFunded {
					b.FundedAccounts++
				}
			}
		}
	}
}

func (b *Block) Rollback(accounts, delegates map[AccountID]*Account) {
	// block will be stored as orphan
	b.Update(accounts, delegates)
	b.IsOrphan = true
}
