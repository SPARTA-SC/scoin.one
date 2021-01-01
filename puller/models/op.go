// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package models

import (
	"sync"
	"tezos_index/chain"
	"tezos_index/rpc"
	"time"
)

var opPool = &sync.Pool{
	New: func() interface{} { return new(Op) },
}

type OpID uint64

func (id OpID) Value() uint64 {
	return uint64(id)
}

type Op struct {
	RowId        OpID            `gorm:"primary_key;column:row_id"   json:"row_id"`                 // internal: unique row id
	Timestamp    time.Time       `gorm:"column:time"      json:"time"`                              // bc: op block time
	Height       int64           `gorm:"column:height"      json:"height"`                          // bc: block height op was mined at
	Cycle        int64           `gorm:"column:cycle"      json:"cycle"`                            // bc: block cycle (tezos specific)
	Hash         chain.StrOpHash `gorm:"column:hash;index:hash"             json:"hash"`            // bc: unique op_id (op hash)
	Counter      int64           `gorm:"column:counter"     json:"counter"`                         // bc: counter
	OpN          int             `gorm:"column:op_n"      json:"op_n"`                              // bc: position in block (block.Operations.([][]*OperationHeader) list position)
	OpC          int             `gorm:"column:op_c"      json:"op_c"`                              // bc: position in OperationHeader.Contents.([]Operation) list
	OpI          int             `gorm:"column:op_i"      json:"op_i"`                              // bc: position in internal operation result list
	Type         chain.OpType    `gorm:"column:type;index:type_idx"      json:"type"`               // stats: operation type as defined byprotocol
	Status       chain.OpStatus  `gorm:"column:status"      json:"status"`                          // stats: operation status
	IsSuccess    bool            `gorm:"column:is_success"      json:"is_success"`                  // bc: operation succesful flag
	IsContract   bool            `gorm:"column:is_contract"     json:"is_contract"`                 // bc: operation succesful flag
	GasLimit     int64           `gorm:"column:gas_limit"      json:"gas_limit"`                    // stats: gas limit
	GasUsed      int64           `gorm:"column:gas_used"      json:"gas_used"`                      // stats: gas used
	GasPrice     float64         `gorm:"column:gas_price"      json:"gas_price"`                    // stats: gas price in tezos per unit gas, relative to tx fee
	StorageLimit int64           `gorm:"column:storage_limit"      json:"storage_limit"`            // stats: storage size limit
	StorageSize  int64           `gorm:"column:storage_size"      json:"storage_size"`              // stats: storage size used/allocated by this op
	StoragePaid  int64           `gorm:"column:storage_paid"      json:"storage_paid"`              // stats: extra storage size paid by this op
	Volume       int64           `gorm:"column:volume"      json:"volume"`                          // stats: sum of transacted tezos volume
	Fee          int64           `gorm:"column:fee"      json:"fee"`                                // stats: transaction fees
	Reward       int64           `gorm:"column:reward"      json:"reward"`                          // stats: baking and endorsement rewards
	Deposit      int64           `gorm:"column:deposit"      json:"deposit"`                        // stats: bonded deposits for baking and endorsement
	Burned       int64           `gorm:"column:burned"      json:"burned"`                          // stats: burned tezos
	SenderId     AccountID       `gorm:"column:sender_id;index:sender_idx"      json:"sender_id"`   // internal: op sender
	ReceiverId   AccountID       `gorm:"column:receiver_id;index:recv_idx"      json:"receiver_id"` // internal: op receiver
	ManagerId    AccountID       `gorm:"column:manager_id"      json:"manager_id"`                  // internal: op manager for originations
	DelegateId   AccountID       `gorm:"column:delegate_id"      json:"delegate_id"`                // internal: op delegate for originations and delegations
	IsInternal   bool            `gorm:"column:is_internal;index:acc_idx"      json:"is_internal"`  // bc: internal chain/funds management
	HasData      bool            `gorm:"column:has_data"      json:"has_data"`                      // internal: flag to signal if data is available
	Data         string          `gorm:"column:data;type:BLOB"      json:"data"`                    // bc: extra op data
	Parameters   []byte          `gorm:"column:parameters;type:BLOB"      json:"parameters"`        // bc: input params
	Storage      []byte          `gorm:"column:storage;type:BLOB"      json:"storage"`              // bc: result storage
	BigMapDiff   []byte          `gorm:"column:big_map_diff;type:BLOB"      json:"big_map_diff"`    // bc: result big map diff
	Errors       string          `gorm:"column:errors;type:BLOB"      json:"errors"`                // bc: result errors
	TDD          float64         `gorm:"column:days_destroyed"  json:"days_destroyed"`              // stats: token days destroyed
	BranchId     uint64          `gorm:"column:branch_id"      json:"branch_id"`                    // bc: branch block the op is based on
	BranchHeight int64           `gorm:"column:branch_height"      json:"branch_height"`            // bc: height of the branch block
	BranchDepth  int64           `gorm:"column:branch_depth"      json:"branch_depth"`              // stats: diff between branch block and current block
}

func AllocOp() *Op {
	return opPool.Get().(*Op)
}

func NewOp(block, branch *Block, head *rpc.OperationHeader, op_n, op_c, op_i int) *Op {
	o := AllocOp()
	o.RowId = 0
	o.Timestamp = block.Timestamp
	o.Height = block.Height
	o.Cycle = block.Cycle
	o.Hash = chain.StrOpHash(head.Hash.String())
	o.OpN = op_n
	o.OpC = op_c
	o.OpI = op_i
	o.Type = head.Contents[op_c].OpKind()
	if branch != nil {
		o.BranchId = branch.RowId
		o.BranchHeight = branch.Height
		o.BranchDepth = block.Height - branch.Height
	}
	// other fields are type specific and will be set by builder
	return o
}

func (o Op) ID() uint64 {
	return uint64(o.RowId)
}

func (o *Op) SetID(id uint64) {
	o.RowId = OpID(id)
}

func (o *Op) Free() {
	o.Reset()
	opPool.Put(o)
}

func (o *Op) Reset() {
	o.RowId = 0
	o.Timestamp = time.Time{}
	o.Height = 0
	o.Cycle = 0
	o.Hash = chain.StrOpHash(chain.ZeroHash.String())
	o.OpN = 0
	o.OpC = 0
	o.OpI = 0
	o.Counter = 0
	o.Type = 0
	o.Status = 0
	o.IsSuccess = false
	o.IsContract = false
	o.GasLimit = 0
	o.GasUsed = 0
	o.GasPrice = 0
	o.StorageLimit = 0
	o.StorageSize = 0
	o.StoragePaid = 0
	o.Volume = 0
	o.Fee = 0
	o.Reward = 0
	o.Deposit = 0
	o.Burned = 0
	o.SenderId = 0
	o.ReceiverId = 0
	o.ManagerId = 0
	o.DelegateId = 0
	o.IsInternal = false
	o.HasData = false
	o.Data = ""
	o.Parameters = nil
	o.Storage = nil
	o.BigMapDiff = nil
	o.Errors = ""
	o.TDD = 0
	o.BranchId = 0
	o.BranchHeight = 0
	o.BranchDepth = 0
}
