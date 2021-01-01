// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package puller

import (
	"encoding/json"
	"github.com/go-redis/redis"
	"tezos_index/chain"
	. "tezos_index/puller/models"
)

var (

	// tipKey is the key of the chain tip serialized data in the db.
	tipKey = "tip"

	// tipsBucketName is the name of the bucket holding indexer tips.
	tipsBucketName = "tips"

	// deploymentsBucketName is the name of the bucket holding protocol deployment parameters.
	deploymentsBucketName = []byte("deployments")
)

func dbLoadChainTip(db *redis.Client) (*ChainTip, error) {
	tip := &ChainTip{}
	val, err := db.Get(tipKey).Bytes()
	if err == redis.Nil {
		return nil, ErrNoChainTip
	} else if err != nil {
		return nil, err
	} else {
		if err := json.Unmarshal(val, tip); err != nil {
			return nil, err
		}
	}
	return tip, nil
}

func dbStoreChainTip(db *redis.Client, tip *ChainTip) error {
	buf, err := json.Marshal(tip)
	if err != nil {
		return err
	}
	return db.Set(tipKey, buf, 0).Err()
}

type IndexTip struct {
	Hash   *chain.BlockHash `json:"hash,omitempty"`
	Height int64            `json:"height"`
}

func dbStoreIndexTip(db *redis.Client, key string, tip *IndexTip) error {
	buf, err := json.Marshal(tip)
	if err != nil {
		return err
	}
	return db.Set(key, buf, 0).Err()
}

func dbLoadIndexTip(db *redis.Client, key string) (*IndexTip, error) {
	tip := &IndexTip{}
	val, err := db.Get(key).Bytes()
	if err == redis.Nil {
		return nil, ErrNoTable
	} else if err != nil {
		return nil, err
	} else {
		err := json.Unmarshal(val, tip)
		return tip, err
	}
}

func dbLoadDeployments(db *redis.Client, tip *ChainTip) ([]*chain.Params, error) {
	plist := make([]*chain.Params, 0, len(tip.Deployments))
	for _, v := range tip.Deployments {
		key := v.Protocol.Hash.String()
		buf, err := db.Get(key).Bytes()
		if err != nil {
			return nil, err
		}
		p := &chain.Params{}
		if err := json.Unmarshal(buf, p); err != nil {
			return nil, err
		}
		plist = append(plist, p)
	}
	return plist, nil
}

func dbStoreDeployment(db *redis.Client, p *chain.Params) error {
	buf, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return db.Set(p.Protocol.Hash.String(), buf, 0).Err()
}
