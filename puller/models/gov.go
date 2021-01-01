// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package models

import (
	"github.com/jinzhu/gorm"
	"tezos_index/chain"
	"time"
)

// An Election represents a unique voting cycle which may be between 1 and 4
// voting periods in length. Elections finish with the activation of the winning
// proposal protocol after period 4 or abort at the end of period 1 when no
// proposal was published, at the end of periods 2 and 4 when no quorum or
// supermajority was reached. Period 3 always ends successfully.
type ElectionID uint64

func (id ElectionID) Value() uint64 {
	return uint64(id)
}

type Election struct {
	RowId        ElectionID `gorm:"primary_key;column:row_id"   json:"row_id"`      // unique id
	ProposalId   ProposalID `gorm:"column:proposal_id"      json:"proposal_id"`     // winning proposal id (after first vote)
	NumPeriods   int        `gorm:"column:num_periods"      json:"num_periods"`     // number of periods processed (so far)
	NumProposals int        `gorm:"column:num_proposals"      json:"num_proposals"` // number of sumbitted proposals
	VotingPeriod int64      `gorm:"column:voting_perid"      json:"voting_perid"`   // protocol (proposal) voting period starting the election
	StartTime    time.Time  `gorm:"column:start_time"      json:"start_time"`       // proposal voting period start
	EndTime      time.Time  `gorm:"column:end_time"      json:"end_time"`           // last voting perid end, estimate when open
	StartHeight  int64      `gorm:"column:start_height"      json:"start_height"`   // proposal voting period start block
	EndHeight    int64      `gorm:"column:end_height"      json:"end_height"`       // last voting perid end block, estimate when open
	IsEmpty      bool       `gorm:"column:is_empty"     json:"is_empty"`            // no proposal published during this period
	IsOpen       bool       `gorm:"column:is_open"      json:"is_open"`             // flag, election in progress
	IsFailed     bool       `gorm:"column:is_failed"      json:"is_failed"`         // flag, election aborted du to missing proposal or missed quorum/supermajority
	NoQuorum     bool       `gorm:"column:no_quorum"      json:"no_quorum"`         // flag, quorum not reached
	NoMajority   bool       `gorm:"column:no_majority"      json:"no_majority"`     // flag, supermajority not reached
}

func UpdateElection(up *Election, db *gorm.DB) error {
	data := make(map[string]interface{})
	data["proposal_id"] = up.ProposalId
	data["num_periods"] = up.NumPeriods
	data["num_proposals"] = up.NumProposals
	data["voting_perid"] = up.VotingPeriod
	data["start_time"] = up.StartTime
	data["end_time"] = up.EndTime
	data["start_height"] = up.StartHeight
	data["end_height"] = up.EndHeight
	data["is_empty"] = up.IsEmpty
	data["is_open"] = up.IsOpen
	data["is_failed"] = up.IsFailed
	data["no_quorum"] = up.NoQuorum
	data["no_majority"] = up.NoMajority

	return db.Model(&Election{}).Where("row_id = ?", up.RowId).Updates(data).Error
}

func (e *Election) ID() uint64 {
	return uint64(e.RowId)
}

func (e *Election) SetID(id uint64) {
	e.RowId = ElectionID(id)
}

// Proposal implements unique individual proposals, a baker can choose to publish
// multiple proposals in one operation, which results in multiple rows been created.
type ProposalID uint64

func (id ProposalID) Value() uint64 {
	return uint64(id)
}

type Proposal struct {
	RowId        ProposalID     `gorm:"primary_key;column:row_id"   json:"row_id"`      // unique id
	Hash         chain.StrPHash `gorm:"column:hash"      json:"hash"`                   // unique proposal hash
	Height       int64          `gorm:"column:height"      json:"height"`               // proposal publishing block
	Time         time.Time      `gorm:"column:time"      json:"time"`                   // proposal publishing time
	SourceId     AccountID      `gorm:"column:source_id"      json:"source_id"`         // proposal publisher
	OpId         OpID           `gorm:"column:op_id"      json:"op_id"`                 // operation publishing this proposal
	ElectionId   ElectionID     `gorm:"column:election_id"      json:"election_id"`     // custom: election sequence number (same for all voting periods)
	VotingPeriod int64          `gorm:"column:voting_period"      json:"voting_period"` // protocol: proposal period sequence number
	Rolls        int64          `gorm:"column:rolls"      json:"rolls"`                 // number of rolls accumulated by this proposal
	Voters       int64          `gorm:"column:voters"      json:"voters"`               // number of voters who voted for this proposal
}

func UpdateProposal(p *Proposal, db *gorm.DB) error {
	data := make(map[string]interface{})
	data["hash"] = p.Hash
	data["height"] = p.Height
	data["time"] = p.Time
	data["source_id"] = p.SourceId
	data["op_id"] = p.OpId
	data["election_id"] = p.ElectionId
	data["voting_period"] = p.VotingPeriod
	data["rolls"] = p.Rolls
	data["voters"] = p.Voters
	return db.Model(&Proposal{}).Where("row_id = ?", p.RowId).Updates(data).Error
}

func (p *Proposal) ID() uint64 {
	return uint64(p.RowId)
}

func (p *Proposal) SetID(id uint64) {
	p.RowId = ProposalID(id)
}

// Vote represent the most recent state of a voting period during elections
// or, when closed, the final result of a voting period. Votes contain the
// cummulative aggregate state at the current block.
type Vote struct {
	RowId            uint64                 `gorm:"primary_key;column:row_id" json:"row_id"`                  // unique id
	ElectionId       ElectionID             `gorm:"column:election_id"    json:"election_id"`                 // related election id
	ProposalId       ProposalID             `gorm:"column:proposal_id"    json:"proposal_id"`                 // related proposal id
	VotingPeriod     int64                  `gorm:"column:voting_period"    json:"voting_period"`             // on-chain sequence number
	VotingPeriodKind chain.VotingPeriodKind `gorm:"column:voting_period_kind"    json:"voting_period_kind"`   // on-chain period
	StartTime        time.Time              `gorm:"column:period_start_time"    json:"period_start_time"`     // start time (block time) of voting period
	EndTime          time.Time              `gorm:"column:period_end_time"    json:"period_end_time"`         // end time (block time), estimate when polls are open
	StartHeight      int64                  `gorm:"column:period_start_height"    json:"period_start_height"` // start block height of voting period
	EndHeight        int64                  `gorm:"column:period_end_height"    json:"period_end_height"`     // end block height
	EligibleRolls    int64                  `gorm:"column:eligible_rolls"    json:"eligible_rolls"`           // total number of rolls at start of perid
	EligibleVoters   int64                  `gorm:"column:eligible_voters"    json:"eligible_voters"`         // total number of roll owners at start of period
	QuorumPct        int64                  `gorm:"column:quorum_pct"    json:"quorum_pct"`                   // required quorum in percent (store as integer with 2 digits)
	QuorumRolls      int64                  `gorm:"column:quorum_rolls"    json:"quorum_rolls"`               // required quorum in rolls (0 for proposal_period)
	TurnoutRolls     int64                  `gorm:"column:turnout_rolls"    json:"turnout_rolls"`             // actual participation in rolls
	TurnoutVoters    int64                  `gorm:"column:turnout_voters"    json:"turnout_voters"`           // actual participation in voters
	TurnoutPct       int64                  `gorm:"column:turnout_pct"    json:"turnout_pct"`                 // actual participation in percent
	TurnoutEma       int64                  `gorm:"column:turnout_ema"    json:"turnout_ema"`                 // EMA (80/20) of participation in percent
	YayRolls         int64                  `gorm:"column:yay_rolls"    json:"yay_rolls"`
	YayVoters        int64                  `gorm:"column:yay_voters"    json:"yay_voters"`
	NayRolls         int64                  `gorm:"column:nay_rolls"    json:"nay_rolls"`
	NayVoters        int64                  `gorm:"column:nay_voters"    json:"nay_voters"`
	PassRolls        int64                  `gorm:"column:pass_rolls"    json:"pass_rolls"`
	PassVoters       int64                  `gorm:"column:pass_voters"    json:"pass_voters"`
	IsOpen           bool                   `gorm:"column:is_open"    json:"is_open"`         // flag, polls are open (only current period)
	IsFailed         bool                   `gorm:"column:is_failed"    json:"is_failed"`     // flag, failed reaching quorum or supermajority
	IsDraw           bool                   `gorm:"column:is_draw"   json:"is_draw"`          // flag, draw between at least two proposals
	NoProposal       bool                   `gorm:"column:no_proposal"    json:"no_proposal"` // flag, no proposal submitted
	NoQuorum         bool                   `gorm:"column:no_quorum"    json:"no_quorum"`     // flag, quorum not reached
	NoMajority       bool                   `gorm:"column:no_majority"    json:"no_majority"` // flag, supermajority not reached
}

func (v *Vote) ID() uint64 {
	return v.RowId
}

func (v *Vote) SetID(id uint64) {
	v.RowId = id
}

// Ballot represent a single vote cast by a baker during a voting period.
// Only periods 1, 2 and 4 support casting votes, period 1 uses `proposals`
// operations to vote on up to 20 proposals, periods 2 and 4 use `ballot`
// operations to vote on progressing with a single winning proposal.
type Ballot struct {
	RowId            uint64                 `gorm:"primary_key;column:row_id"  json:"row_id"`                // unique id
	ElectionId       ElectionID             `gorm:"column:election_id"     json:"election_id"`               // related election id
	ProposalId       ProposalID             `gorm:"column:proposal_id"     json:"proposal_id"`               // related proposal id
	VotingPeriod     int64                  `gorm:"column:voting_period"     json:"voting_period"`           // on-chain sequence number
	VotingPeriodKind chain.VotingPeriodKind `gorm:"column:voting_period_kind"     json:"voting_period_kind"` // on-chain period
	Height           int64                  `gorm:"column:height"     json:"height"`                         // proposal/ballot operation block height
	Time             time.Time              `gorm:"column:time"     json:"time"`                             // proposal/ballot operation block time
	SourceId         AccountID              `gorm:"column:source_id"     json:"source_id"`                   // voting account
	OpId             OpID                   `gorm:"column:op_id"     json:"op_id"`                           // proposal/ballot operation id
	Rolls            int64                  `gorm:"column:rolls"     json:"rolls"`                           // number of rolls for voter (at beginning of voting period)
	Ballot           chain.BallotVote       `gorm:"column:ballot"     json:"ballot"`                         // yay, nay, pass; proposal period uses yay only
}

func (b *Ballot) ID() uint64 {
	return b.RowId
}

func (b *Ballot) SetID(id uint64) {
	b.RowId = id
}
