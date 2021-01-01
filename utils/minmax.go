// Copyright (c) 2013-2018 KIDTSUNAMI
// Author: alex@kidtsunami.com

package util

import (
	"bytes"
	"math"
	"time"
)

func MinString(a, b string) string {
	if a < b {
		return a
	}
	return b
}

func MaxString(a, b string) string {
	if a > b {
		return a
	}
	return b
}

func MinBytes(a, b []byte) []byte {
	if bytes.Compare(a, b) < 0 {
		return a
	}
	return b
}

func MaxBytes(a, b []byte) []byte {
	if bytes.Compare(a, b) > 0 {
		return a
	}
	return b
}

func Max(x, y int) int {
	if x < y {
		return y
	} else {
		return x
	}
}

func Min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

func MaxN(nums ...int) int {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v > n {
				n = v
			}
		}
		return n
	}
}

func MinN(nums ...int) int {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v < n {
				n = v
			}
		}
		return n
	}
}

func NonZero(x ...int) int {
	for _, v := range x {
		if v != 0 {
			return v
		}
	}
	return 0
}

func NonZeroMin(x ...int) int {
	var min int
	for _, v := range x {
		if v != 0 {
			if min == 0 {
				min = v
			} else {
				min = Min(min, v)
			}
		}
	}
	return min
}

func NonZeroMin64(x ...int64) int64 {
	var min int64
	for _, v := range x {
		if v != 0 {
			if min == 0 {
				min = v
			} else {
				min = Min64(min, v)
			}
		}
	}
	return min
}

func Max64(x, y int64) int64 {
	if x < y {
		return y
	}
	return x
}

func Min64(x, y int64) int64 {
	if x > y {
		return y
	}
	return x
}

func Max64N(nums ...int64) int64 {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v > n {
				n = v
			}
		}
		return n
	}
}

func Min64N(nums ...int64) int64 {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v < n {
				n = v
			}
		}
		return n
	}
}

func MaxU64(x, y uint64) uint64 {
	if x < y {
		return y
	}
	return x
}

func MinU64(x, y uint64) uint64 {
	if x > y {
		return y
	}
	return x
}

func MinFloat64(x, y float64) float64 {
	return math.Min(x, y)
}

func MaxFloat64(x, y float64) float64 {
	return math.Max(x, y)
}

func MinFloat64N(nums ...float64) float64 {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v < n {
				n = v
			}
		}
		return n
	}
}

func MaxFloat64N(nums ...float64) float64 {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v > n {
				n = v
			}
		}
		return n
	}
}

func MaxTime(x, y time.Time) time.Time {
	if x.After(y) {
		return x
	}
	return y
}

func MinTime(x, y time.Time) time.Time {
	if x.Before(y) {
		return x
	}
	return y
}

func MaxDuration(a, b time.Duration) time.Duration {
	if int64(a) < int64(b) {
		return b
	}
	return a
}

func MinDuration(a, b time.Duration) time.Duration {
	if int64(a) > int64(b) {
		return b
	}
	return a
}
