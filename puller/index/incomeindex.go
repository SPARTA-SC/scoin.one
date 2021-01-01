// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"errors"
	"fmt"
	"github.com/jinzhu/gorm"
	"github.com/zyjblockchain/sandy_log/log"
	"math"
	"math/bits"
	"sort"
	"strconv"
	"tezos_index/chain"
	"tezos_index/puller/models"
)

const (
	IncomePackSizeLog2    = 15 // =32k packs ~ 3M unpacked
	IncomeJournalSizeLog2 = 17 // =128k entries for busy blockchains
	IncomeCacheSize       = 2  // minimum
	IncomeFillLevel       = 100
	IncomeIndexKey        = "income"
	IncomeTableKey        = "income"
)

var (
	ErrNoIncomeEntry = errors.New("income not indexed")
)

type IncomeIndex struct {
	db *gorm.DB
}

func NewIncomeIndex(db *gorm.DB) *IncomeIndex {
	return &IncomeIndex{db}
}

func (idx *IncomeIndex) DB() *gorm.DB {
	return idx.db
}

func (idx *IncomeIndex) Key() string {
	return IncomeIndexKey
}

func (idx *IncomeIndex) ConnectBlock(ctx context.Context, block *models.Block, builder models.BlockBuilder, tx *gorm.DB) error {
	// ignore genesis block
	if block.Height == 0 {
		return nil
	}

	// bootstrap first cycles on first block using all genesis bakers as snapshot proxy
	// block 1 contains all initial rights, this number is fixed at crawler.go
	if block.Height == 1 {
		return idx.BootstrapIncome(ctx, block, builder, tx)
	}

	// update expected income/deposits and luck on cycle start when params are known
	if block.Params.IsCycleStart(block.Height) {
		if err := idx.UpdateCycleIncome(ctx, block, builder, tx); err != nil {
			return err
		}
	}

	// update income from flows and rights
	if err := idx.UpdateBlockIncome(ctx, block, builder, false, tx); err != nil {
		return err
	}

	// update burn from nonce revelations, if any
	if err := idx.UpdateNonceRevelations(ctx, block, builder, false, tx); err != nil {
		return err
	}

	// skip when no new rights are defined
	if len(block.TZ.Baking) == 0 || len(block.TZ.Endorsing) == 0 {
		return nil
	}

	return idx.CreateCycleIncome(ctx, block, builder, tx)
}

func (idx *IncomeIndex) DisconnectBlock(ctx context.Context, block *models.Block, builder models.BlockBuilder, tx *gorm.DB) error {
	// rollback current income
	if err := idx.UpdateBlockIncome(ctx, block, builder, true, tx); err != nil {
		return err
	}

	// update burn from nonce revelations, if any
	if err := idx.UpdateNonceRevelations(ctx, block, builder, true, tx); err != nil {
		return err
	}

	// new rights are fetched in cycles
	if block.Params.IsCycleStart(block.Height) {
		return idx.DeleteCycle(ctx, block.Height, tx)
	}
	return nil
}

func (idx *IncomeIndex) DeleteBlock(ctx context.Context, height int64, tx *gorm.DB) error {
	return nil
}

func (idx *IncomeIndex) BootstrapIncome(ctx context.Context, block *models.Block, builder models.BlockBuilder, tx *gorm.DB) error {
	// on bootstrap use the initial params from block 1
	p := block.Params

	// get sorted list of foundation bakers
	accs := make([]*models.Account, 0)
	for _, v := range builder.Delegates() {
		accs = append(accs, v)
	}
	sort.Slice(accs, func(i, j int) bool { return accs[i].RowId < accs[j].RowId })

	for cycle := int64(0); cycle < p.PreservedCycles+1; cycle++ {

		// new income data for each cycle
		var totalRolls int64
		incomeMap := make(map[models.AccountID]*models.Income)
		for _, v := range accs {
			rolls := v.StakingBalance() / p.TokensPerRoll
			totalRolls += rolls
			incomeMap[v.RowId] = &models.Income{
				Cycle:        cycle,
				AccountId:    v.RowId,
				Rolls:        rolls,
				Balance:      v.Balance(),
				Delegated:    v.DelegatedBalance,
				NDelegations: v.ActiveDelegations,
				LuckPct:      10000,
			}
		}

		log.Debugf("New bootstrap income for cycle %d from no snapshot with %d delegates",
			cycle, len(incomeMap))

		// pre-calculate deposit and reward amounts
		blockDeposit, endorseDeposit := p.BlockSecurityDeposit, p.EndorsementSecurityDeposit
		if cycle < p.SecurityDepositRampUpCycles-1 {
			blockDeposit = blockDeposit * cycle / p.SecurityDepositRampUpCycles
			endorseDeposit = endorseDeposit * cycle / p.SecurityDepositRampUpCycles
		}
		blockReward, endorseReward := p.BlockReward, p.EndorsementReward
		if cycle < p.NoRewardCycles {
			blockReward, endorseReward = 0, 0
		}

		for _, v := range block.TZ.Baking {
			if p.CycleFromHeight(v.Level) != cycle || v.Priority > 0 {
				continue
			}
			acc, ok := builder.AccountByAddress(v.Delegate)
			if !ok {
				return fmt.Errorf("income: missing bootstrap baker %s", v.Delegate)
			}
			ic, ok := incomeMap[acc.RowId]
			if !ok {
				return fmt.Errorf("income: missing bootstrap income data for baker %s %d", v.Delegate, acc.RowId)
			}
			ic.NBakingRights++
			ic.ExpectedIncome += blockReward
			ic.ExpectedBonds += blockDeposit
		}

		// set correct expectations about endorsement rewards for the last block in a cycle:
		// endorsement income for a cycle is left-shifted by 1 (the last block in a cycle
		// is endorsed in the next cycle and this shifts income from rights into this cycle too)
		endorseStartBlock := p.CycleEndHeight(cycle - 1)
		endorseEndBlock := p.CycleEndHeight(cycle) - 1
		for _, v := range block.TZ.Endorsing {
			if v.Level < endorseStartBlock || v.Level > endorseEndBlock {
				continue
			}
			acc, ok := builder.AccountByAddress(v.Delegate)
			if !ok {
				return fmt.Errorf("income: missing bootstrap endorser %s", v.Delegate)
			}
			ic, ok := incomeMap[acc.RowId]
			if !ok {
				return fmt.Errorf("income: missing bootstrap income data for endorser %s %d", v.Delegate, acc.RowId)
			}
			ic.NEndorsingRights += int64(len(v.Slots))
			ic.ExpectedIncome += endorseReward * int64(len(v.Slots))
			ic.ExpectedBonds += endorseDeposit * int64(len(v.Slots))
		}

		// calculate luck and append for insert
		inc := make([]*models.Income, 0, len(incomeMap))
		for _, v := range incomeMap {
			v.UpdateLuck(totalRolls, p)
			inc = append(inc, v)
		}
		// sort by account id
		sort.Slice(inc, func(i, j int) bool { return inc[i].AccountId < inc[j].AccountId })

		// cast into insertion slice
		ins := make([]*models.Income, len(inc))
		for i, v := range inc {
			ins[i] = v
		}
		// todo batch insert
		for _, v := range ins {
			if err := tx.Create(v).Error; err != nil {
				return err
			}
		}
	}
	return nil
}

// use to update cycles 0..14 expected income and deposits because ramp-up constants
// are only available at start of cycle and not when the income rows are created
//
// also used to update income after upgrade to v006 for all remaining cycles due
// to changes in rewards
func (idx *IncomeIndex) UpdateCycleIncome(ctx context.Context, block *models.Block, builder models.BlockBuilder, tx *gorm.DB) error {
	p := block.Params

	// check pre-conditon and pick cycles to update
	var updateCycles []int64
	switch true {
	case block.Cycle <= 2*(p.PreservedCycles+2):
		// during ramp-up cycles
		log.Debugf("Updating expected income for cycle %d during ramp-up.", block.Cycle)
		updateCycles = []int64{block.Cycle}

	case block.Height == p.StartHeight && p.Version == 6:
		// on upgrade to v6, update all future reward expectations
		log.Debug("Updating expected income after v006 activation.")
		updateCycles = make([]int64, 0)
		for i := int64(0); i < p.PreservedCycles; i++ {
			updateCycles = append(updateCycles, block.Cycle+i)
		}

	default:
		// no update required on
		return nil
	}

	for _, v := range updateCycles {
		incomes := make([]*models.Income, 0)
		var totalRolls int64

		var ins []*models.Income
		err := tx.Where("cycle = ?", v).Find(&ins).Error
		if err != nil {
			return err
		}

		for _, in := range ins {
			blockDeposit, endorseDeposit := p.BlockSecurityDeposit, p.EndorsementSecurityDeposit
			blockReward, endorseReward := p.BlockReward, p.EndorsementReward
			in.ExpectedIncome += blockReward * in.NBakingRights
			in.ExpectedBonds += blockDeposit * in.NBakingRights
			in.ExpectedIncome += endorseReward * in.NEndorsingRights
			in.ExpectedBonds += endorseDeposit * in.NEndorsingRights
			totalRolls += in.Rolls
			incomes = append(incomes, in)
		}

		// update luck and convert type
		upd := make([]*models.Income, len(incomes))
		for i, v := range incomes {
			v.UpdateLuck(totalRolls, p)
			upd[i] = v
		}
		// todo batch update
		for _, v := range upd {
			if err := updateThisInc(v, tx); err != nil {
				return err
			}
		}
	}
	return nil
}

func updateThisInc(upc *models.Income, db *gorm.DB) error {
	data := make(map[string]interface{})
	data["expected_income"] = upc.ExpectedIncome
	data["expected_bonds"] = upc.ExpectedBonds
	data["luck"] = upc.Luck
	data["luck_percent"] = upc.LuckPct

	return db.Model(&models.Income{}).Where("row_id = ?", upc.RowId).Updates(data).Error
}

func (idx *IncomeIndex) CreateCycleIncome(ctx context.Context, block *models.Block, builder models.BlockBuilder, tx *gorm.DB) error {
	p := block.Params
	sn := block.TZ.Snapshot
	incomeMap := make(map[models.AccountID]*models.Income)
	var totalRolls int64

	if sn.Cycle < p.PreservedCycles+2 {
		// build income from delegators because there is no snapshot yet
		accs := make([]*models.Account, 0)
		for _, v := range builder.Delegates() {
			accs = append(accs, v)
		}
		sort.Slice(accs, func(i, j int) bool { return accs[i].RowId < accs[j].RowId })

		for _, v := range accs {
			rolls := v.StakingBalance() / p.TokensPerRoll
			totalRolls += rolls
			incomeMap[v.RowId] = &models.Income{
				Cycle:        sn.Cycle,
				AccountId:    v.RowId,
				Rolls:        rolls,
				Balance:      v.Balance(),
				Delegated:    v.DelegatedBalance,
				NDelegations: v.ActiveDelegations,
				LuckPct:      10000,
			}
		}
		log.Debugf("New bootstrap income for cycle %d from no snapshot with %d delegates",
			sn.Cycle, len(incomeMap))

	} else {
		// build income from snapshot

		// FIXME: params should come from the future cycle
		// p := builder.Registry().GetParamsByHeight(block.Params.CycleStartHeight(sn.Cycle))
		var ss []*models.Snapshot
		err := tx.Where("cycle = ? and s_index = ? and is_active = ?",
			sn.Cycle-(p.PreservedCycles+2), sn.RollSnapshot, true).Find(&ss).Error
		if err != nil {
			return err
		}
		for _, s := range ss {
			incomeMap[s.AccountId] = &models.Income{
				Cycle:        sn.Cycle, // the future cycle
				AccountId:    s.AccountId,
				Rolls:        s.Rolls,
				Balance:      s.Balance,
				Delegated:    s.Delegated,
				NDelegations: s.NDelegations,
				LuckPct:      10000,
			}
			totalRolls += s.Rolls
		}

		log.Debugf("New income for cycle %d from snapshot [%d/%d] with %d delegates [%d/%d] rights",
			sn.Cycle, sn.Cycle-(p.PreservedCycles+2), sn.RollSnapshot, len(incomeMap), len(block.TZ.Baking), len(block.TZ.Endorsing))
	}

	// pre-calculate deposit and reward amounts
	blockDeposit, endorseDeposit := p.BlockSecurityDeposit, p.EndorsementSecurityDeposit
	if sn.Cycle < p.SecurityDepositRampUpCycles-1 {
		blockDeposit = blockDeposit * sn.Cycle / p.SecurityDepositRampUpCycles
		endorseDeposit = endorseDeposit * sn.Cycle / p.SecurityDepositRampUpCycles
	}
	blockReward, endorseReward := p.BlockReward, p.EndorsementReward

	// assign from rights
	for _, v := range block.TZ.Baking {
		if v.Priority > 0 {
			continue
		}
		acc, ok := builder.AccountByAddress(v.Delegate)
		if !ok {
			return fmt.Errorf("income: missing baker %s", v.Delegate)
		}
		ic, ok := incomeMap[acc.RowId]
		if !ok {
			return fmt.Errorf("income: missing snapshot data for baker %s [%d] at snapshot %d[%d]",
				v.Delegate, acc.RowId, sn.Cycle, sn.RollSnapshot)
		}
		ic.NBakingRights++
		ic.ExpectedIncome += blockReward
		ic.ExpectedBonds += blockDeposit
	}

	// set correct expectations about endorsement rewards for the last block in a cycle:
	// endorsement income for a cycle is left-shifted by 1 (the last block in a cycle
	// is endorsed in the next cycle and this shifts income from rights into this cycle too)
	endorseStartBlock := p.CycleEndHeight(sn.Cycle - 1)
	endorseEndBlock := p.CycleEndHeight(sn.Cycle) - 1

	for _, v := range block.TZ.Endorsing {
		if v.Level > endorseEndBlock {
			// log.Infof("Skipping end of cycle height %d > %d", v.Level, endorseEndBlock)
			continue
		}
		acc, ok := builder.AccountByAddress(v.Delegate)
		if !ok {
			return fmt.Errorf("income: missing endorser %s", v.Delegate)
		}
		ic, ok := incomeMap[acc.RowId]
		if !ok {
			return fmt.Errorf("income: missing income data for endorser %s %d at %d[%d]",
				v.Delegate, acc.RowId, sn.Cycle, sn.RollSnapshot)
		}
		ic.NEndorsingRights += int64(len(v.Slots))
		ic.ExpectedIncome += endorseReward * int64(len(v.Slots))
		ic.ExpectedBonds += endorseDeposit * int64(len(v.Slots))
	}
	// load endorse rights for last block of previous cycle
	var rights []*models.Right
	err := tx.Where("height = ? and type = ?", endorseStartBlock, int64(chain.RightTypeEndorsing)).Find(&rights).Error
	if err != nil {
		return err
	}
	for _, right := range rights {
		// the previous cycle could have more delegates which still get some trailing
		// endorsement rewards here even though they may not have any more rights
		ic, ok := incomeMap[right.AccountId]
		if !ok {
			// load prev data
			ic, err = idx.loadIncome(ctx, right.Cycle, right.AccountId, tx)
			if err != nil {
				return fmt.Errorf("income: missing income data for prev cycle endorser %d at %d[%d]",
					right.AccountId, sn.Cycle, sn.RollSnapshot)
			}
			// copy to new income struct
			ic = &models.Income{
				Cycle:        sn.Cycle, // the future cycle
				AccountId:    right.AccountId,
				Rolls:        ic.Rolls,
				Balance:      ic.Balance,
				Delegated:    ic.Delegated,
				NDelegations: ic.NDelegations,
				LuckPct:      10000,
			}
			incomeMap[ic.AccountId] = ic
		}
		ic.NEndorsingRights++
		ic.ExpectedIncome += endorseReward
		ic.ExpectedBonds += endorseDeposit
	}

	// calculate luck and append for insert
	inc := make([]*models.Income, 0, len(incomeMap))
	for _, v := range incomeMap {
		v.UpdateLuck(totalRolls, p)
		inc = append(inc, v)
	}

	// sort by account id
	sort.Slice(inc, func(i, j int) bool { return inc[i].AccountId < inc[j].AccountId })

	// cast into insertion slice
	// todo batch insert
	for _, v := range inc {
		if err := tx.Create(v).Error; err != nil {
			return err
		}
	}
	return nil
}

func (idx *IncomeIndex) UpdateBlockIncome(ctx context.Context, block *models.Block, builder models.BlockBuilder, isRollback bool, tx *gorm.DB) error {
	var err error
	p := block.Params
	incomeMap := make(map[models.AccountID]*models.Income)
	var mul int64 = 1
	if isRollback {
		mul = -1
	}
	blockReward := block.BlockReward(p)

	// handle flows from (baking, endorsing, seed nonce, double baking, double endorsement)
	for _, f := range block.Flows {
		// all income is frozen, so ignore any other flow right away
		if !f.IsFrozen {
			continue
		}
		// fetch baker from map
		in, ok := incomeMap[f.AccountId]
		if !ok {
			in, err = idx.loadIncome(ctx, block.Cycle, f.AccountId, tx)
			if err != nil {
				return fmt.Errorf("income: unknown baker %d", f.AccountId)
			}
			incomeMap[in.AccountId] = in
		}

		switch f.Operation {
		case models.FlowTypeBaking:
			switch f.Category {
			case models.FlowCategoryDeposits:
				in.TotalBonds += f.AmountIn * mul

			case models.FlowCategoryRewards:
				in.TotalIncome += f.AmountIn * mul
				in.BakingIncome += f.AmountIn * mul
				if block.Priority > 0 {
					// the real baker stole this income
					in.StolenBakingIncome += f.AmountIn * mul

					// the original prio 0 baker lost it
					for _, v := range builder.Rights(chain.RightTypeBaking) {
						if v.Priority > 0 {
							continue
						}
						loser, ok := incomeMap[v.AccountId]
						if !ok {
							loser, err = idx.loadIncome(ctx, block.Cycle, v.AccountId, tx)
							if err != nil {
								return fmt.Errorf("income: unknown losing baker %d", v.AccountId)
							}
							incomeMap[loser.AccountId] = loser
						}
						loser.MissedBakingIncome += blockReward * mul
					}
				}
			}
		case models.FlowTypeEndorsement:
			switch f.Category {
			case models.FlowCategoryDeposits:
				in.TotalBonds += f.AmountIn * mul
			case models.FlowCategoryRewards:
				in.TotalIncome += f.AmountIn * mul
				in.EndorsingIncome += f.AmountIn * mul
			}

		case models.FlowTypeNonceRevelation:
			// note: this does not process losses
			if !f.IsBurned {
				if f.Category == models.FlowCategoryRewards {
					in.TotalIncome += f.AmountIn * mul
					in.SeedIncome += f.AmountIn * mul
				}
			}

		case models.FlowTypeDenounciation:
			// there's only one flow type here, so we cannot split 2bake and 2endorse
			// income (will be handled using ops below), but we debit the offender

			// debit receiver
			in, ok = incomeMap[f.AccountId]
			if !ok {
				in, err = idx.loadIncome(ctx, block.Cycle, f.AccountId, tx)
				if err != nil {
					return fmt.Errorf("income: unknown 2bake/2endorse offender %d", f.AccountId)
				}
				incomeMap[f.AccountId] = in
			}
			// sum individual losses
			switch f.Category {
			case models.FlowCategoryDeposits:
				in.LostAccusationDeposits += f.AmountOut * mul
			case models.FlowCategoryRewards:
				in.LostAccusationRewards += f.AmountOut * mul
			case models.FlowCategoryFees:
				in.LostAccusationFees += f.AmountOut * mul
			}

			// sum overall loss
			in.TotalLost += f.AmountOut * mul

		default:
			// fee flows from all kinds of operations
			if f.Category == models.FlowCategoryFees {
				in.FeesIncome += f.AmountIn * mul
			}
		}
	}

	// update bake counters separate from flow
	if block.BakerId > 0 {
		baker, ok := incomeMap[block.BakerId]
		if !ok {
			baker, err = idx.loadIncome(ctx, block.Cycle, block.BakerId, tx)
			if err != nil {
				return fmt.Errorf("income: unknown baker %d", block.BakerId)
			}
			incomeMap[baker.AccountId] = baker
		}
		baker.NBlocksBaked += mul
		if block.Priority > 0 {
			// the real baker stole this block
			baker.NBlocksStolen += mul

			// the original prio 0 baker lost it
			for _, v := range builder.Rights(chain.RightTypeBaking) {
				if v.Priority > 0 {
					continue
				}
				loser, ok := incomeMap[v.AccountId]
				if !ok {
					loser, err = idx.loadIncome(ctx, block.Cycle, v.AccountId, tx)
					if err != nil {
						return fmt.Errorf("income: unknown losing baker %d", v.AccountId)
					}
					incomeMap[loser.AccountId] = loser
				}
				loser.NBlocksLost += mul
			}
		}
	}

	// for counters and creditor denounciations we parse operations
	for _, op := range block.Ops {
		switch op.Type {
		case chain.OpTypeSeedNonceRevelation:
			// credit sender
			in, ok := incomeMap[op.SenderId]
			if !ok {
				in, err = idx.loadIncome(ctx, block.Cycle, op.SenderId, tx)
				if err != nil {
					return fmt.Errorf("income: unknown seeder %d", op.SenderId)
				}
				incomeMap[in.AccountId] = in
			}
			in.NSeedsRevealed += mul

		case chain.OpTypeEndorsement:
			// credit sender
			in, ok := incomeMap[op.SenderId]
			if !ok {
				in, err = idx.loadIncome(ctx, block.Cycle, op.SenderId, tx)
				if err != nil {
					return fmt.Errorf("income: unknown endorser %d", op.SenderId)
				}
				incomeMap[in.AccountId] = in
			}
			slots, _ := strconv.ParseUint(op.Data, 10, 32)
			in.NSlotsEndorsed += mul * int64(bits.OnesCount32(uint32(slots)))

		case chain.OpTypeDoubleBakingEvidence:
			// credit sender
			in, ok := incomeMap[op.SenderId]
			if !ok {
				in, err = idx.loadIncome(ctx, block.Cycle, op.SenderId, tx)
				if err != nil {
					return fmt.Errorf("income: unknown 2bake accuser %d", op.SenderId)
				}
				incomeMap[in.AccountId] = in
			}
			in.DoubleBakingIncome += op.Reward * mul

			// offender is debited above

		case chain.OpTypeDoubleEndorsementEvidence:
			// credit sender
			in, ok := incomeMap[op.SenderId]
			if !ok {
				in, err = idx.loadIncome(ctx, block.Cycle, op.SenderId, tx)
				if err != nil {
					return fmt.Errorf("income: unknown 2endorse accuser %d", op.SenderId)
				}
				incomeMap[in.AccountId] = in
			}
			in.DoubleEndorsingIncome += op.Reward * mul

			// offender is debited above
		}
	}

	// missed endorsements require an idea about how much an endorsement is worth
	endorseReward := p.EndorsementReward
	if block.Cycle < p.NoRewardCycles {
		endorseReward = 0
	}

	// handle missed endorsements
	if block.Parent != nil && block.Parent.SlotsEndorsed != math.MaxUint32 {
		for _, v := range builder.Rights(chain.RightTypeEndorsing) {
			if !v.IsMissed {
				continue
			}
			in, ok := incomeMap[v.AccountId]
			if !ok {
				in, err = idx.loadIncome(ctx, block.Cycle, v.AccountId, tx)
				if err != nil {
					return fmt.Errorf("income: unknown missed endorser %d", v.AccountId)
				}
				incomeMap[in.AccountId] = in
			}
			in.MissedEndorsingIncome += endorseReward * mul
			in.NSlotsMissed += mul
		}
	}

	if len(incomeMap) == 0 {
		return nil
	}

	upd := make([]*models.Income, 0, len(incomeMap))
	for _, v := range incomeMap {
		// absolute performance as expected vs actual income where 100% is the ideal case
		// =100%: total == expected
		// <100%: total < expected (may be <0 if slashed)
		// >100%: total > expected
		totalGain := v.TotalIncome - v.TotalLost - v.ExpectedIncome
		if v.ExpectedIncome > 0 {
			v.PerformancePct = 10000 + totalGain*10000/v.ExpectedIncome
		}
		// contribution performance base calculation on rights
		totalRights := v.NBakingRights + v.NEndorsingRights
		totalWork := v.NBlocksBaked + v.NSlotsEndorsed
		totalGain = totalWork - totalRights
		if totalRights > 0 {
			v.ContributionPct = 10000 + totalGain*10000/totalRights
		}
		upd = append(upd, v)
	}
	// todo batch update
	for _, v := range upd {
		if err := updateIncome(v, tx); err != nil {
			return err
		}
	}
	return nil
}

// updateIncome
func updateIncome(inc *models.Income, db *gorm.DB) error {
	data := make(map[string]interface{})
	data["cycle"] = inc.Cycle
	data["account_id"] = inc.AccountId
	data["rolls"] = inc.Rolls
	data["balance"] = inc.Balance
	data["delegated"] = inc.Delegated
	data["n_delegations"] = inc.NDelegations
	data["n_baking_rights"] = inc.NBakingRights
	data["n_endorsing_rights"] = inc.NEndorsingRights
	data["luck"] = inc.Luck
	data["luck_percent"] = inc.LuckPct
	data["contribution_percent"] = inc.ContributionPct
	data["performance_percent"] = inc.PerformancePct
	data["n_blocks_baked"] = inc.NBlocksBaked
	data["n_blocks_lost"] = inc.NBlocksLost
	data["n_blocks_stolen"] = inc.NBlocksStolen
	data["n_slots_endorsed"] = inc.NSlotsEndorsed
	data["n_slots_missed"] = inc.NSlotsMissed
	data["n_seeds_revealed"] = inc.NSeedsRevealed
	data["expected_income"] = inc.ExpectedIncome
	data["expected_bonds"] = inc.ExpectedBonds
	data["total_income"] = inc.TotalIncome
	data["total_bonds"] = inc.TotalBonds
	data["baking_income"] = inc.BakingIncome
	data["endorsing_income"] = inc.EndorsingIncome
	data["double_baking_income"] = inc.DoubleBakingIncome
	data["double_endorsing_income"] = inc.DoubleEndorsingIncome
	data["seed_income"] = inc.SeedIncome
	data["fees_income"] = inc.FeesIncome
	data["missed_baking_income"] = inc.MissedBakingIncome
	data["missed_endorsing_income"] = inc.MissedEndorsingIncome
	data["stolen_baking_income"] = inc.StolenBakingIncome
	data["total_lost"] = inc.TotalLost
	data["lost_accusation_fees"] = inc.LostAccusationFees
	data["lost_accusation_rewards"] = inc.LostAccusationRewards
	data["lost_accusation_deposits"] = inc.LostAccusationDeposits
	data["lost_revelation_fees"] = inc.LostRevelationFees
	data["lost_revelation_rewards"] = inc.LostRevelationRewards

	return db.Model(&models.Income{}).Where("row_id = ?", inc.RowId).Updates(data).Error
}

func (idx *IncomeIndex) UpdateNonceRevelations(ctx context.Context, block *models.Block, builder models.BlockBuilder, isRollback bool, tx *gorm.DB) error {
	cycle := block.Cycle - 1
	if cycle < 0 {
		return nil
	}
	var err error
	incomeMap := make(map[models.AccountID]*models.Income)
	var mul int64 = 1
	if isRollback {
		mul = -1
	}

	for _, f := range block.Flows {
		// skip irrelevant flows
		if f.Operation != models.FlowTypeNonceRevelation || !f.IsBurned {
			continue
		}
		// find and update the income row
		in, ok := incomeMap[f.AccountId]
		if !ok {
			in, err = idx.loadIncome(ctx, cycle, f.AccountId, tx)
			if err != nil {
				return fmt.Errorf("income: unknown seed nonce baker %d", f.AccountId)
			}
			incomeMap[in.AccountId] = in
		}
		in.TotalLost += f.AmountOut * mul
		switch f.Category {
		case models.FlowCategoryRewards:
			in.LostRevelationRewards += f.AmountOut * mul
		case models.FlowCategoryFees:
			in.LostRevelationFees += block.Fees * mul
		}
	}

	if len(incomeMap) == 0 {
		return nil
	}

	upd := make([]*models.Income, 0, len(incomeMap))
	for _, v := range incomeMap {
		// absolute performance as expected vs actual income where 100% is the ideal case
		// =100%: total == expected
		// <100%: total < expected (may be <0 if slashed)
		// >100%: total > expected
		totalGain := v.TotalIncome - v.TotalLost - v.ExpectedIncome
		if v.ExpectedIncome > 0 {
			v.PerformancePct = 10000 + totalGain*10000/v.ExpectedIncome
		}
		// contribution performance base calculation on rights
		totalRights := v.NBakingRights + v.NEndorsingRights
		totalWork := v.NBlocksBaked + v.NSlotsEndorsed
		totalGain = totalWork - totalRights
		if totalRights > 0 {
			v.ContributionPct = 10000 + totalGain*10000/totalRights
		}
		upd = append(upd, v)
	}
	// todo batch update
	for _, v := range upd {
		if err := updateThisIncome(v, tx); err != nil {
			return err
		}
	}
	return nil
}

func updateThisIncome(inc *models.Income, db *gorm.DB) error {
	data := make(map[string]interface{})
	data["total_lost"] = inc.TotalLost
	data["lost_revelation_rewards"] = inc.LostRevelationRewards
	data["lost_revelation_fees"] = inc.LostRevelationFees
	data["performance_percent"] = inc.PerformancePct
	data["contribution_percent"] = inc.ContributionPct

	return db.Model(&models.Income{}).Where("row_id = ?", inc.RowId).Updates(data).Error
}

func (idx *IncomeIndex) DeleteCycle(ctx context.Context, cycle int64, tx *gorm.DB) error {
	log.Debugf("Rollback deleting income for cycle %d", cycle)

	err := tx.Where("cycle = ?", cycle).Delete(&models.Income{}).Error
	return err
}

func (idx *IncomeIndex) loadIncome(ctx context.Context, cycle int64, id models.AccountID, tx *gorm.DB) (*models.Income, error) {
	if cycle < 0 && id <= 0 {
		return nil, ErrNoIncomeEntry
	}

	in := &models.Income{}
	err := tx.Where("cycle = ? and account_id = ?", cycle, id.Value()).Last(in).Error
	if err != nil {
		return nil, ErrNoIncomeEntry
	}
	return in, nil
}
