// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package models

import (
	"sync"
	"tezos_index/chain"
	"time"
)

var flowPool = &sync.Pool{
	New: func() interface{} { return new(Flow) },
}

// Flow captures individual account balance updates.
//
// Example 1
//
// A transaction that pays fees and moves funds from one account to another generates
// four distinct balance updates (1) fees paid, (2) fees received by baker, (3) funds sent,
// (4) funds received.
//
// If sender and/or receiver delegate their balance, 2 more updates adjusting the delegated
// amounts for the respective delegate accounts are created.
//
//
// Example 2
//
// A block baker receives (frozen) rewards and fees, and pays a deposit, generating four
// flows: (1) frozen rewards received, (2) frozen fees received, (3) deposit paid from
// balance, (4) deposit paid to frozen deposits.
//
// Example 2
//
// Unfreeze of deposits, fees and rewards flows from a freezer category to the balance
// category on the same account (i.e. an internal operation). This creates 2 distinct
// flows for each freezer category, one out-flow and a second in-flow to the balance category.

type Flow struct {
	RowId       uint64            `gorm:"primary_key;column:row_id"   json:"row_id"`          // internal: id, not height!
	Height      int64             `gorm:"column:height"      json:"height"`                   // bc: block height (also for orphans)
	Cycle       int64             `gorm:"column:cycle"      json:"cycle"`                     // bc: block cycle (tezos specific)
	Timestamp   time.Time         `gorm:"column:time"      json:"time"`                       // bc: block creation time
	AccountId   AccountID         `gorm:"column:account_id;index:acc"      json:"account_id"` // unique account id
	OriginId    AccountID         `gorm:"column:origin_id;index:acc"      json:"origin_id"`   // origin account that initiated the flow
	AddressType chain.AddressType `gorm:"column:address_type"      json:"address_type"`       // address type, usable as filter
	Category    FlowCategory      `gorm:"column:category;index:type"      json:"category"`    // sub-account that received the update
	Operation   FlowType          `gorm:"column:operation;index:type"      json:"operation"`  // op type that caused this update
	AmountIn    int64             `gorm:"column:amount_in"      json:"amount_in"`             // sum flowing in to the account
	AmountOut   int64             `gorm:"column:amount_out"      json:"amount_out"`           // sum flowing out of the account
	IsFee       bool              `gorm:"column:is_fee"      json:"is_fee"`                   // flag to indicate this out-flow paid a fee
	IsBurned    bool              `gorm:"column:is_burned"      json:"is_burned"`             // flag to indicate this out-flow was burned
	IsFrozen    bool              `gorm:"column:is_frozen"      json:"is_frozen"`             // flag to indicate this in-flow is frozen
	IsUnfrozen  bool              `gorm:"column:is_unfrozen"      json:"is_unfrozen"`         // flag to indicate this flow (rewards -> balance) was unfrozen
	TokenGenMin int64             `gorm:"column:token_gen_min"      json:"token_gen_min"`     // hops
	TokenGenMax int64             `gorm:"column:token_gen_max"      json:"token_gen_max"`     // hops
	TokenAge    int64             `gorm:"column:token_age"      json:"token_age"`             // time since last move in seconds
}

func (f *Flow) ID() uint64 {
	return f.RowId
}

func (f *Flow) SetID(id uint64) {
	f.RowId = id
}

func NewFlow(b *Block, acc *Account, org *Account) *Flow {
	f := flowPool.Get().(*Flow)
	f.Height = b.Height
	f.Cycle = b.Cycle
	f.Timestamp = b.Timestamp
	f.AccountId = acc.RowId
	f.AddressType = acc.Type
	if org != nil {
		f.OriginId = org.RowId
	}
	return f
}

func (f *Flow) Free() {
	f.Reset()
	flowPool.Put(f)
}

func (f *Flow) Reset() {
	f.RowId = 0
	f.Height = 0
	f.Cycle = 0
	f.Timestamp = time.Time{}
	f.AccountId = 0
	f.AddressType = 0
	f.OriginId = 0
	f.Category = 0
	f.Operation = 0
	f.AmountIn = 0
	f.AmountOut = 0
	f.IsFee = false
	f.IsBurned = false
	f.IsFrozen = false
	f.IsUnfrozen = false
	f.TokenGenMin = 0
	f.TokenGenMax = 0
	f.TokenAge = 0
}
