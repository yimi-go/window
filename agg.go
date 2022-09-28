package window

import (
	"math"
)

type Aggregator interface {
	Min() int64
	Max() int64
	Avg() float64
	Sum() int64
	Count() int64
	Reduce(func(bucket Bucket) (done bool))
}

type windowAgg struct {
	w          *window
	skipRecent uint
}

func (agg *windowAgg) Min() int64 {
	min, hasValue := int64(math.MaxInt64), false
	agg.Reduce(func(bucket Bucket) (done bool) {
		for _, val := range bucket.Data() {
			hasValue = true
			if min > val {
				min = val
			}
		}
		return
	})
	if hasValue {
		return min
	}
	return 0
}

func (agg *windowAgg) Max() int64 {
	max, hasValue := int64(math.MinInt64), false
	agg.Reduce(func(bucket Bucket) (done bool) {
		for _, val := range bucket.Data() {
			hasValue = true
			if max < val {
				max = val
			}
		}
		return
	})
	if hasValue {
		return max
	}
	return 0
}

func (agg *windowAgg) Avg() float64 {
	var count, sum int64
	agg.Reduce(func(bucket Bucket) (done bool) {
		count += bucket.Count()
		for _, val := range bucket.Data() {
			sum += val
		}
		return
	})
	if count == 0 {
		return 0
	}
	return float64(sum) / float64(count)
}

func (agg *windowAgg) Sum() int64 {
	var sum int64
	agg.Reduce(func(bucket Bucket) (done bool) {
		for _, val := range bucket.Data() {
			sum += val
		}
		return
	})
	return sum
}

func (agg *windowAgg) Count() int64 {
	var count int64
	agg.Reduce(func(bucket Bucket) (done bool) {
		count += bucket.Count()
		return
	})
	return count
}

func (agg *windowAgg) Reduce(f func(bucket Bucket) (done bool)) {
	agg.w.rwMu.RLock()
	defer agg.w.rwMu.RUnlock()
	position := agg.w.Position()
	lastPosition := agg.w.lastPosition
	if lastPosition == position {
		// 无滑动
		agg.reduce(f, position, 0)
		return
	}
	if lastPosition > position {
		// 倒滑，不允许。当作没有有效数据。
		return
	}
	// lastPosition < position
	span := position - lastPosition
	if span >= agg.w.size {
		// 窗口完全滑出。窗口中完全无数据
		return
	}
	// 除去最后 span 个数据，对窗口前部数据做 reduce
	agg.reduce(f, position, span)
}

func (agg *windowAgg) reduce(f func(bucket Bucket) (done bool), position, skip int64) {
	w := agg.w
	tr := w.track
	offset := uint64(position+int64(len(tr.buckets))-w.size+1) & tr.bucketIndexMask
	b := &tr.buckets[offset]
	if skip < int64(agg.skipRecent) {
		skip = int64(agg.skipRecent)
	}
	size := w.size - skip
	for i := int64(0); i < size; i++ {
		if f(b) {
			break
		}
		b = b.next
	}
}
