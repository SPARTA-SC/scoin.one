// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package puller

import (
	"context"
	"github.com/go-redis/redis"
	"github.com/jinzhu/gorm"
	"github.com/zyjblockchain/sandy_log/log"
	"sync"
	"sync/atomic"
	"tezos_index/chain"
	. "tezos_index/puller/models"
	util "tezos_index/utils"
)

type IndexerConfig struct {
	StateDB *gorm.DB
	CacheDB *redis.Client
	Indexes []BlockIndexer
}

// Indexer defines an index manager that manages and stores multiple indexes.
type Indexer struct {
	mu      sync.Mutex
	times   atomic.Value
	ranks   atomic.Value
	dbpath  string
	dbopts  interface{}
	statedb *gorm.DB
	cachedb *redis.Client
	reg     *Registry
	indexes []BlockIndexer
	tips    map[string]*IndexTip
}

func NewIndexer(cfg IndexerConfig) *Indexer {
	return &Indexer{
		statedb: cfg.StateDB,
		cachedb: cfg.CacheDB,
		indexes: cfg.Indexes,
		reg:     NewRegistry(),
		tips:    make(map[string]*IndexTip),
	}
}

func (m *Indexer) Init(ctx context.Context, tip *ChainTip) error {
	// Nothing to do when no indexes are enabled.
	if len(m.indexes) == 0 {
		return nil
	}

	// load tips
	var needCreate bool
	for _, t := range m.indexes {
		// load index tip
		key := t.Key()
		tip, err := dbLoadIndexTip(m.cachedb, key)
		if err == ErrNoTable {
			needCreate = true
			break
		} else if err != nil {
			return err
		} else {
			m.tips[key] = tip
		}
	}
	// load known protocol deployment parameters
	if !needCreate {
		deps, err := dbLoadDeployments(m.cachedb, tip)
		if err != nil {
			return err
		}
		for _, v := range deps {
			_ = m.reg.Register(v)
		}
	}

	// Create the initial state for the indexes as needed.
	if needCreate {
		// create buckets for index tips in the respecive databases
		for _, t := range m.indexes {
			if err := m.maybeCreateIndex(ctx, m.cachedb, t); err != nil {
				return err
			}
			key := t.Key()
			ttip, err := dbLoadIndexTip(m.cachedb, key)
			if err != nil {
				return err
			}
			m.tips[key] = ttip
		}
	}
	return nil
}

func (m *Indexer) Close() error {
	for _, idx := range m.indexes {
		log.Infof("Closing %s.", idx.Key())
		if err := m.storeTip(idx.Key()); err != nil {
			return err
		}
	}
	return nil
}

func (m *Indexer) ConnectProtocol(ctx context.Context, params *chain.Params) error {
	prev := m.reg.GetParamsLatest()
	if prev != nil {
		// update previous protocol end height
		prev.EndHeight = params.StartHeight - 1
		if err := dbStoreDeployment(m.cachedb, prev); err != nil {
			return err
		}
	}
	// save new params
	if err := dbStoreDeployment(m.cachedb, params); err != nil {
		return err
	}

	return m.reg.Register(params)
}

func (m *Indexer) ConnectBlock(ctx context.Context, block *Block, builder BlockBuilder) error {
	// update block time when synchronized
	if err := m.updateBlockTime(block); err != nil {
		return err
	}

	var err error
	tx := m.statedb.Begin()
	for _, t := range m.indexes {
		key := t.Key()
		tip, ok := m.tips[string(key)]
		if !ok {
			log.Errorf("missing tip for table %s", string(key))
			continue
		}

		// skip when the block is already known
		if tip.Hash != nil && tip.Hash.String() == block.Hash.String() {
			continue
		}

		err = t.ConnectBlock(ctx, block, builder, tx)
		if err != nil {
			break
		}
	}

	// 获取indexer 的事务，统一处理connectBlock 中的tx, 保证写入操作一致
	if err != nil {
		tx.Rollback()
		return err
	} else {
		tx.Commit()
		// 修改indexer 的tip
		for _, t := range m.indexes {
			key := t.Key()
			tip, ok := m.tips[string(key)]
			if !ok {
				continue
			}
			// Update the current tip.
			cHash, _ := chain.ParseBlockHash(block.Hash.String())
			cloned := cHash.Clone()
			tip.Hash = &cloned
			tip.Height = block.Height
		}
	}
	return nil
}

func (m *Indexer) DisconnectBlock(ctx context.Context, block *Block, builder BlockBuilder, ignoreErrors bool) error {
	var errs error
	tx := m.statedb.Begin()
	for _, t := range m.indexes {
		key := t.Key()
		tip, ok := m.tips[string(key)]
		if !ok {
			log.Errorf("missing tip for table %s", string(key))
			continue
		}
		if block.Height > 0 && (tip.Hash.String() != block.Hash.String()) {
			continue
		}
		if err := t.DisconnectBlock(ctx, block, builder, tx); err != nil && !ignoreErrors {
			errs = err
			break
		}
	}

	// 获取indexer 的事务，统一处理connectBlock 中的tx, 保证写入操作一致
	if errs != nil {
		tx.Rollback()
		return errs
	} else {
		tx.Commit()
		// 修改indexer 的tip
		for _, t := range m.indexes {
			key := t.Key()
			tip, ok := m.tips[string(key)]
			if !ok {
				continue
			}
			// Update the current tip.
			cloned := block.TZ.Parent().Clone()
			tip.Hash = &cloned
			tip.Height = block.Height - 1
		}
	}
	return nil
}

func (m *Indexer) DeleteBlock(ctx context.Context, tz *Bundle) error {
	var errs error
	tx := m.statedb.Begin()
	for _, t := range m.indexes {
		key := t.Key()
		tip, ok := m.tips[string(key)]
		if !ok {
			log.Errorf("missing tip for table %s", string(key))
			continue
		}
		if tz.Height() != tip.Height {
			continue
		}
		if err := t.DeleteBlock(ctx, tz.Height(), tx); err != nil {
			errs = err
			break
		}
	}

	// 获取indexer 的事务，统一处理connectBlock 中的tx, 保证写入操作一致
	if errs != nil {
		tx.Rollback()
		return errs
	} else {
		tx.Commit()
		// 修改indexer 的tip
		for _, t := range m.indexes {
			key := t.Key()
			tip, ok := m.tips[string(key)]
			if !ok {
				continue
			}
			// Update the current tip.
			cloned := tz.Parent().Clone()
			tip.Hash = &cloned
			tip.Height = tz.Height() - 1
		}
	}
	return nil
}

// maybeCreateIndex determines if each of the enabled index indexes has already
// been created and creates them if not.
func (m *Indexer) maybeCreateIndex(ctx context.Context, db *redis.Client, idx BlockIndexer) error {
	// Nothing to do if the tip already exists.
	key := idx.Key()
	if err := db.Get(key).Err(); err != redis.Nil {
		return nil
	}
	// start with zero hash on create (genesis is inserted next)
	return dbStoreIndexTip(db, key, &IndexTip{
		Hash:   nil,
		Height: 0,
	})
}

// Store idx tip
func (m *Indexer) storeTip(key string) error {
	tip, ok := m.tips[key]
	if !ok {
		return nil
	}
	log.Debugf("Storing %s idx tip.", key)
	return dbStoreIndexTip(m.cachedb, key, tip)
}

// FIXME: uint32 overflows in year 2083
// - first timestamp is actual time in unix secs, remainder are offsets in seconds
// - each entry uses 4 bytes per block, safe until June 2017 + 66 years;
func (m *Indexer) buildBlockTimes(ctx context.Context) ([]uint32, error) {
	times := make([]uint32, 0, 1<<20)
	var bbs []*Block
	if err := m.statedb.Select("time").Where("height >= ?", int64(0)).Find(&bbs).Error; err != nil {
		return nil, err
	}
	for _, b := range bbs {
		if len(times) == 0 {
			// safe, it's before 2036
			times = append(times, uint32(b.Timestamp.Unix()))
		} else {
			// make sure there's no rounding error
			tsdiff := b.Timestamp.Unix() - int64(times[0])
			times = append(times, uint32(tsdiff))
		}
	}
	return times, nil
}

// only called from single thread in crawler, no locking required
func (m *Indexer) updateBlockTime(block *Block) error {
	av := m.times.Load()
	if av == nil {
		// not initialized yet
		return nil
	}
	oldTimes := av.([]uint32)
	newTimes := make([]uint32, len(oldTimes), util.Max(cap(oldTimes), int(block.Height+1)))
	copy(newTimes, oldTimes)
	// extend slice and patch time-diff into position
	newTimes = newTimes[:int(block.Height+1)]
	tsdiff := block.Timestamp.Unix() - int64(newTimes[0])
	newTimes[int(block.Height)] = uint32(tsdiff)
	m.times.Store(newTimes)
	return nil
}
