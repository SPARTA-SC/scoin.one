// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package models

import (
	"sync"
	"time"
)

var snapshotPool = &sync.Pool{
	New: func() interface{} { return new(Snapshot) },
}

// Snapshot is an account balance snapshot made at a snapshot block.
type Snapshot struct {
	RowId        uint64    `gorm:"primary_key;column:row_id" json:"row_id"`
	Height       int64     `gorm:"column:height"    json:"height"`
	Cycle        int64     `gorm:"column:cycle"    json:"cycle"`
	IsSelected   bool      `gorm:"column:is_selected"    json:"is_selected"`
	Timestamp    time.Time `gorm:"column:time"    json:"time"`
	Index        int64     `gorm:"column:s_index"    json:"index"`
	Rolls        int64     `gorm:"column:rolls"   json:"rolls"`
	AccountId    AccountID `gorm:"column:account_id"    json:"account_id"`
	DelegateId   AccountID `gorm:"column:delegate_id"   json:"delegate_id"`
	IsDelegate   bool      `gorm:"column:is_delegate"  json:"is_delegate"`
	IsActive     bool      `gorm:"column:is_active"    json:"is_active"`
	Balance      int64     `gorm:"column:balance"    json:"balance"`
	Delegated    int64     `gorm:"column:delegated"    json:"delegated"`
	NDelegations int64     `gorm:"column:n_delegations"    json:"n_delegations"`
	Since        int64     `gorm:"column:since"    json:"since"`
}

func NewSnapshot() *Snapshot {
	return allocSnapshot()
}

func allocSnapshot() *Snapshot {
	return snapshotPool.Get().(*Snapshot)
}

func (s *Snapshot) Free() {
	s.Reset()
	snapshotPool.Put(s)
}

func (s Snapshot) ID() uint64 {
	return uint64(s.RowId)
}

func (s *Snapshot) SetID(id uint64) {
	s.RowId = id
}

func (s *Snapshot) Reset() {
	s.RowId = 0
	s.Height = 0
	s.Cycle = 0
	s.IsSelected = false
	s.Timestamp = time.Time{}
	s.Index = 0
	s.Rolls = 0
	s.AccountId = 0
	s.DelegateId = 0
	s.IsDelegate = false
	s.IsActive = false
	s.Balance = 0
	s.Delegated = 0
	s.NDelegations = 0
	s.Since = 0
}
