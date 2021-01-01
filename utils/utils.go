package util

import (
	"fmt"
	"sort"
	"tezos_index/micheline"
)

type Uint64Sorter []uint64

func (s Uint64Sorter) Sort() {
	if !sort.IsSorted(s) {
		sort.Sort(s)
	}
}

func (s Uint64Sorter) Len() int           { return len(s) }
func (s Uint64Sorter) Less(i, j int) bool { return s[i] < s[j] }
func (s Uint64Sorter) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func UniqueUint64Slice(a []uint64) []uint64 {
	if len(a) == 0 {
		return a
	}
	b := make([]uint64, len(a))
	copy(b, a)
	Uint64Sorter(b).Sort()
	j := 0
	for i := 1; i < len(b); i++ {
		if b[j] == b[i] {
			continue
		}
		j++
		b[j] = b[i]
	}
	return b[:j+1]
}

// Smart Contract Storage Access
var vestingContractBalancePath = []int{1, 0, 0, 0}

func GetVestingBalance(prim *micheline.Prim) (int64, error) {
	if prim == nil {
		return 0, nil
	}
	for i, v := range vestingContractBalancePath {
		if len(prim.Args) < v+1 {
			return 0, fmt.Errorf("non existing path at %v in vesting contract storage", vestingContractBalancePath[:i])
		}
		prim = prim.Args[v]
	}
	if prim.Type != micheline.PrimInt {
		return 0, fmt.Errorf("unexpected prim type %s for vesting contract balance", prim.Type)
	}
	return prim.Int.Int64(), nil
}
