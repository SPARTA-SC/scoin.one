// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package models

import (
	"sync"
	"tezos_index/chain"
	"tezos_index/micheline"
	"tezos_index/rpc"
)

var contractPool = &sync.Pool{
	New: func() interface{} { return new(Contract) },
}

// Contract holds code and info about smart contracts on the Tezos blockchain.
type Contract struct {
	RowId         uint64    `gorm:"primary_key;column:row_id"   json:"row_id"`
	Hash          []byte    `gorm:"column:hash"       json:"hash"`
	AccountId     AccountID `gorm:"column:account_id"      json:"account_id"`
	ManagerId     AccountID `gorm:"column:manager_id"      json:"manager_id"`
	Height        int64     `gorm:"column:height"      json:"height"`
	Fee           int64     `gorm:"column:fee"      json:"fee"`
	GasLimit      int64     `gorm:"column:gas_limit"      json:"gas_limit"`
	GasUsed       int64     `gorm:"column:gas_used"      json:"gas_used"`
	GasPrice      float64   `gorm:"column:gas_price"      json:"gas_price"`
	StorageLimit  int64     `gorm:"column:storage_limit"      json:"storage_limit"`
	StorageSize   int64     `gorm:"column:storage_size"      json:"storage_size"`
	StoragePaid   int64     `gorm:"column:storage_paid"      json:"storage_paid"`
	Script        []byte    `gorm:"column:script;type:BLOB"      json:"script"`
	IsSpendable   bool      `gorm:"column:is_spendable"      json:"is_spendable"`     // manager can move funds without running any code
	IsDelegatable bool      `gorm:"column:is_delegatable"      json:"is_delegatable"` // manager can delegate funds
}

// assuming the op was successful!
func NewContract(acc *Account, oop *rpc.OriginationOp) *Contract {
	c := AllocContract()
	c.Hash = acc.Hash
	c.AccountId = acc.RowId
	c.ManagerId = acc.ManagerId
	c.Height = acc.FirstSeen
	c.Fee = oop.Fee
	c.GasLimit = oop.GasLimit
	c.StorageLimit = oop.StorageLimit
	res := oop.Metadata.Result
	c.GasUsed = res.ConsumedGas
	if c.GasUsed > 0 && c.Fee > 0 {
		c.GasPrice = float64(c.Fee) / float64(c.GasUsed)
	}
	c.StorageSize = res.StorageSize
	c.StoragePaid = res.PaidStorageSizeDiff
	if oop.Script != nil {
		c.Script, _ = oop.Script.MarshalBinary()
	}
	c.IsSpendable = acc.IsSpendable
	c.IsDelegatable = acc.IsDelegatable
	return c
}

func NewInternalContract(acc *Account, iop *rpc.InternalResult) *Contract {
	c := AllocContract()
	c.Hash = acc.Hash
	c.AccountId = acc.RowId
	c.ManagerId = acc.ManagerId
	c.Height = acc.FirstSeen
	res := iop.Result
	c.GasUsed = res.ConsumedGas
	c.StorageSize = res.StorageSize
	c.StoragePaid = res.PaidStorageSizeDiff
	if iop.Script != nil {
		c.Script, _ = iop.Script.MarshalBinary()
	}
	return c
}

func AllocContract() *Contract {
	return contractPool.Get().(*Contract)
}

func (c *Contract) Free() {
	c.Reset()
	contractPool.Put(c)
}

func (c Contract) ID() uint64 {
	return c.RowId
}

func (c *Contract) SetID(id uint64) {
	c.RowId = id
}

func (c Contract) String() string {
	s, _ := chain.EncodeAddress(chain.AddressTypeContract, c.Hash)
	return s
}

func (c *Contract) Reset() {
	c.RowId = 0
	c.Hash = nil
	c.AccountId = 0
	c.ManagerId = 0
	c.Height = 0
	c.GasLimit = 0
	c.GasUsed = 0
	c.GasPrice = 0
	c.StorageLimit = 0
	c.StorageSize = 0
	c.StoragePaid = 0
	c.Script = nil
	c.IsSpendable = false
	c.IsDelegatable = false
}

// loads script and upgrades to babylon on-the-fly if originated earlier
func (c *Contract) LoadScript(tip *ChainTip, height int64, manager []byte) (*micheline.Script, error) {
	script := micheline.NewScript()

	// patch empty manager.tz
	if len(c.Script) == 0 {
		if tip.ChainId.IsEqual(chain.Mainnet) && height >= 655361 && c.Height < 655361 {
			script, err := micheline.MakeManagerScript(manager)
			return script, err
		}
		// empty script before Babylon
		return script, nil
	}

	// unmarshal script
	if err := script.UnmarshalBinary(c.Script); err != nil {
		return nil, err
	}

	// must upgrade?
	// - only applies to mainnet and contracts originated before babylon
	// - don't upgrade when requested height is < babylon so we can handle
	//   old params/storage properly
	if tip.ChainId.IsEqual(chain.Mainnet) && height >= 655361 && c.Height < 655361 {
		switch true {
		case c.IsSpendable:
			script.MigrateToBabylonAddDo()
		case !c.IsSpendable && c.IsDelegatable:
			script.MigrateToBabylonSetDelegate()
		}
	}
	return script, nil
}
