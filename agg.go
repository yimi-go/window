package window

import (
	"math"
	"sync"
)

type Aggregator interface {
	Min() int64
	Max() int64
	Avg() float64
	Sum() int64
	Count() int64
	Reduce(func(data []int64))
}

type trackRangeAgg struct {
	state     windowState
	track     *track
	locker    sync.Locker
	leftSkip  uint
	rightSkip uint
}

func (t *trackRangeAgg) Min() int64 {
	min, hasValue := int64(math.MaxInt64), false
	t.Reduce(func(data []int64) {
		for _, val := range data {
			hasValue = true
			if min > val {
				min = val
			}
		}
	})
	if hasValue {
		return min
	}
	return 0
}

func (t *trackRangeAgg) Max() int64 {
	max, hasValue := int64(math.MinInt64), false
	t.Reduce(func(data []int64) {
		for _, val := range data {
			hasValue = true
			if max < val {
				max = val
			}
		}
	})
	if hasValue {
		return max
	}
	return 0
}

func (t *trackRangeAgg) Avg() float64 {
	sum, count := int64(0), 0
	t.Reduce(func(data []int64) {
		count += len(data)
		for _, val := range data {
			sum += val
		}
	})
	if count == 0 {
		return 0
	}
	return float64(sum) / float64(count)
}

func (t *trackRangeAgg) Sum() int64 {
	var sum int64
	t.Reduce(func(data []int64) {
		for _, val := range data {
			sum += val
		}
	})
	return sum
}

func (t *trackRangeAgg) Count() int64 {
	var count int64
	t.Reduce(func(data []int64) {
		count += int64(len(data))
	})
	return count
}

func (t *trackRangeAgg) Reduce(f func([]int64)) {
	mask := t.track.bucketPositionMask
	offset := t.state.position + len(t.track.buckets) + 1 - t.state.size
	t.locker.Lock()
	defer func() {
		t.locker.Unlock()
	}()
	leftSkip, rightSkip := int(t.leftSkip), int(t.rightSkip)
	rangeSize := t.state.size - rightSkip
	for i := 0; i < rangeSize; i++ {
		if i < leftSkip {
			continue
		}
		p := (offset + i) & mask
		data := t.track.buckets[p].readDataOfWindowState(t.state)
		f(data)
	}
}
