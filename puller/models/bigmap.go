// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package models

import (
	"errors"
	"fmt"
	"github.com/jinzhu/gorm"
	"sync"
	"tezos_index/chain"
	"tezos_index/micheline"
	"time"
)

var bigmapPool = &sync.Pool{
	New: func() interface{} { return new(BigMapItem) },
}

type BigMapItem struct {
	DeletedAt   *time.Time                 `sql:"index"`
	RowId       uint64                     `gorm:"primary_key;column:row_id"   json:"row_id"`    // internal: id
	PrevId      uint64                     `gorm:"column:prev_id"      json:"prev_id"`           // row_id of previous value that's updated
	AccountId   AccountID                  `gorm:"column:account_id"      json:"account_id"`     // account table entry for contract
	ContractId  uint64                     `gorm:"column:contract_id"      json:"contract_id"`   // contract table entry
	OpId        OpID                       `gorm:"column:op_id"      json:"op_id"`               // operation id that created/updated/deleted the entry
	Height      int64                      `gorm:"column:height"      json:"height"`             // creation/update/deletion time
	Timestamp   time.Time                  `gorm:"column:time"      json:"time"`                 // creation/update/deletion height
	BigMapId    int64                      `gorm:"column:bigmap_id"      json:"bigmap_id"`       // id of the bigmap
	Action      micheline.BigMapDiffAction `gorm:"column:action"      json:"action"`             // action
	KeyHash     []byte                     `gorm:"column:key_hash"             json:"key_hash"`  // not compressedn because random
	KeyEncoding micheline.PrimType         `gorm:"column:key_encoding"      json:"key_encoding"` // type of the key encoding
	KeyType     micheline.OpCode           `gorm:"column:key_type"      json:"key_type"`         // type of the key encoding
	Key         []byte                     `gorm:"column:key;type:BLOB"      json:"key"`         // key bytes: int: big.Int, string or []byte
	Value       []byte                     `gorm:"column:value;type:BLOB"      json:"value"`     // value bytes: binary encoded micheline.Prim
	IsReplaced  bool                       `gorm:"column:is_replaced"     json:"is_replaced"`    // flag to indicate this entry has been replaced by a newer entry
	IsDeleted   bool                       `gorm:"column:is_deleted"      json:"is_deleted"`     // flag to indicate this key has been deleted
	IsCopied    bool                       `gorm:"column:is_copied"      json:"is_copied"`       // flag to indicate this key has been copied
	Counter     int64                      `gorm:"column:counter"      json:"-"`                 // running update counter
	NKeys       int64                      `gorm:"column:n_keys"      json:"-"`                  // current number of active keys
	Updated     int64                      `gorm:"column:updated"      json:"-"`                 // height at which this entry was replaced
}

func UpdatesBigMapItem(upBigMap *BigMapItem, db *gorm.DB) error {
	if upBigMap.RowId <= 0 {
		return errors.New(fmt.Sprintf("Cannot update withnot row_id record; record:%v", *upBigMap))
	}
	data := make(map[string]interface{})
	data["prev_id"] = upBigMap.PrevId
	data["account_id"] = upBigMap.AccountId
	data["contract_id"] = upBigMap.ContractId
	data["op_id"] = upBigMap.OpId
	data["height"] = upBigMap.Height
	data["time"] = upBigMap.Timestamp
	data["bigmap_id"] = upBigMap.BigMapId
	data["action"] = upBigMap.Action
	data["key_hash"] = upBigMap.KeyHash
	data["key_encoding"] = upBigMap.KeyEncoding
	data["key_type"] = upBigMap.KeyType
	data["key"] = upBigMap.Key
	data["value"] = upBigMap.Value
	data["is_replaced"] = upBigMap.IsReplaced
	data["is_deleted"] = upBigMap.IsDeleted
	data["is_copied"] = upBigMap.IsCopied
	data["counter"] = upBigMap.Counter
	data["n_keys"] = upBigMap.NKeys
	data["updated"] = upBigMap.Updated

	return db.Model(&BigMapItem{}).Where("row_id = ?", upBigMap.RowId).Updates(data).Error
}

func (m *BigMapItem) ID() uint64 {
	return m.RowId
}

func (m *BigMapItem) SetID(id uint64) {
	m.RowId = id
}

func AllocBigMapItem() *BigMapItem {
	return bigmapPool.Get().(*BigMapItem)
}

func (b *BigMapItem) GetKey() (*micheline.BigMapKey, error) {
	return micheline.DecodeBigMapKey(b.KeyType, b.KeyEncoding, b.Key)
}

// assuming BigMapDiffElem.Action is update or remove (copy & alloc are handled below)
func NewBigMapItem(o *Op, cc *Contract, b micheline.BigMapDiffElem, prev uint64, keytype micheline.OpCode, counter, nkeys int64) *BigMapItem {
	m := AllocBigMapItem()
	m.PrevId = prev
	m.AccountId = cc.AccountId
	m.ContractId = cc.RowId
	m.OpId = o.RowId
	m.Height = o.Height
	m.Timestamp = o.Timestamp
	m.BigMapId = b.Id
	m.Action = b.Action
	m.Counter = counter
	m.NKeys = nkeys
	m.Updated = 0
	switch b.Action {
	case micheline.BigMapDiffActionUpdate, micheline.BigMapDiffActionRemove:
		m.KeyHash = b.KeyHash.Hash.Hash
		m.KeyType = keytype // from allocated bigmap type
		m.KeyEncoding = b.Encoding()
		m.Key = b.KeyBytes()
		m.IsDeleted = b.Action == micheline.BigMapDiffActionRemove
		if !m.IsDeleted {
			m.Value, _ = b.Value.MarshalBinary()
		}

	case micheline.BigMapDiffActionAlloc:
		// real key type (opcode) is in the bigmap update
		m.KeyType = b.KeyType
		m.KeyEncoding = b.Encoding()
		m.Value, _ = b.ValueType.MarshalBinary()

	case micheline.BigMapDiffActionCopy:
		// handled outside
	}
	return m
}

// assuming BigMapDiffElem.Action is copy
func CopyBigMapAlloc(b *BigMapItem, o *Op, cc *Contract, dst, counter, nkeys int64) *BigMapItem {
	m := AllocBigMapItem()
	m.PrevId = b.RowId
	m.AccountId = cc.AccountId
	m.ContractId = cc.RowId
	m.OpId = o.RowId
	m.Height = o.Height
	m.Timestamp = o.Timestamp
	m.BigMapId = dst
	m.Action = micheline.BigMapDiffActionAlloc
	m.KeyType = b.KeyType
	m.KeyEncoding = b.KeyEncoding
	m.Key = make([]byte, len(b.Key))
	copy(m.Key, b.Key)
	m.Value = make([]byte, len(b.Value))
	copy(m.Value, b.Value)
	m.IsCopied = true
	m.Counter = counter
	m.NKeys = nkeys
	m.Updated = 0
	return m
}

// assuming BigMapDiffElem.Action is copy
func CopyBigMapValue(b *BigMapItem, o *Op, cc *Contract, dst, counter, nkeys int64) *BigMapItem {
	m := AllocBigMapItem()
	m.PrevId = b.RowId
	m.AccountId = cc.AccountId
	m.ContractId = cc.RowId
	m.OpId = o.RowId
	m.Height = o.Height
	m.Timestamp = o.Timestamp
	m.BigMapId = dst
	m.Action = micheline.BigMapDiffActionUpdate
	m.KeyType = b.KeyType
	m.KeyEncoding = b.KeyEncoding
	m.KeyHash = make([]byte, len(b.KeyHash))
	copy(m.KeyHash, b.KeyHash)
	m.Key = make([]byte, len(b.Key))
	copy(m.Key, b.Key)
	m.Value = make([]byte, len(b.Value))
	copy(m.Value, b.Value)
	m.IsCopied = true
	m.Counter = counter
	m.NKeys = nkeys
	m.Updated = 0
	return m
}

func (m *BigMapItem) BigMapDiff() micheline.BigMapDiffElem {
	var d micheline.BigMapDiffElem
	d.Action = m.Action
	d.Id = m.BigMapId

	// unpack action-specific fields
	switch m.Action {
	case micheline.BigMapDiffActionUpdate, micheline.BigMapDiffActionRemove:
		d.KeyHash = chain.NewExprHash(m.KeyHash)
		d.KeyType = m.KeyEncoding.TypeCode()
		_ = d.DecodeKey(m.KeyEncoding, m.Key)
		if m.Action != micheline.BigMapDiffActionRemove {
			d.Value = &micheline.Prim{}
			_ = d.Value.UnmarshalBinary(m.Value)
		}

	case micheline.BigMapDiffActionAlloc:
		d.KeyType = m.KeyType
		d.ValueType = &micheline.Prim{}
		_ = d.ValueType.UnmarshalBinary(m.Value)

	case micheline.BigMapDiffActionCopy:
		d.DestId = m.BigMapId
		var z micheline.Z
		_ = z.UnmarshalBinary(m.Value)
		d.SourceId = z.Int64()
	}
	return d
}

func (m *BigMapItem) Free() {
	m.Reset()
	bigmapPool.Put(m)
}

func (m *BigMapItem) Reset() {
	m.RowId = 0
	m.AccountId = 0
	m.ContractId = 0
	m.OpId = 0
	m.Height = 0
	m.Timestamp = time.Time{}
	m.BigMapId = 0
	m.KeyHash = nil
	m.KeyType = 0
	m.KeyEncoding = 0
	m.Key = nil
	m.Value = nil
	m.IsReplaced = false
	m.IsDeleted = false
	m.IsCopied = false
	m.Counter = 0
	m.NKeys = 0
	m.Updated = 0
}
