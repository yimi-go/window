package window

import (
	"time"

	"github.com/yimi-go/iter"
)

// A stat Window implementation. It uses power-of-two length stat buckets.
type po2Window struct {
	buckets         []*matrixPo2Bucket
	bucketMask      uint64
	bucketMillis    uint64
	bucketMilliBits int64
	initUnixMilli   int64
	roundMilliBits  int64
}

func newPo2Window(requireBuckets uint64, requireBucketMillis uint64, bucketSpanSize uint64) *po2Window {
	// buckets count must greater than or equal to ensure buckets.
	buckets := nextPo2(requireBuckets)
	// bucket measure length should not greater than required
	bucketMillis := lastPo2(requireBucketMillis)
	bucketMilliBits, tmp := int64(0), bucketMillis
	for tmp != 1 {
		tmp >>= 1
		bucketMilliBits++
	}
	bucketBits, tmp := int64(0), buckets
	for tmp != 1 {
		tmp >>= 1
		bucketBits++
	}
	now := NowFunc().Truncate(time.Millisecond).UnixMilli()
	w := &po2Window{
		buckets:         make([]*matrixPo2Bucket, buckets),
		bucketMask:      buckets - 1,
		bucketMillis:    bucketMillis,
		bucketMilliBits: bucketMilliBits,
		initUnixMilli:   now & ^(int64(buckets*bucketMillis) - 1),
		roundMilliBits:  bucketBits + bucketMilliBits,
	}
	for i := uint64(0); i < buckets; i++ {
		w.buckets[i] = newMatrixPo2Bucket(bucketSpanSize, i)
	}
	return w
}

// Iterate returns an Iterator from offset limit by count.
func (w *po2Window) Iterate(offset uint64) iter.Iterator[*matrixPo2Bucket] {
	return &po2Iter{
		cur: offset + 1,
		end: offset + w.bucketMask + 2,
		w:   w,
	}
}

// Append appends point value to the Window.
func (w *po2Window) Append(val float64) {
	round, nowOffset := windowNowRound(w)
	w.buckets[nowOffset].Append(round, val)
}

type po2Iter struct {
	cur uint64
	end uint64
	w   *po2Window
}

func (it *po2Iter) Next() (*matrixPo2Bucket, bool) {
	if it.cur >= it.end {
		return nil, false
	}
	b := it.w.buckets[it.cur&it.w.bucketMask]
	it.cur++
	return b, true
}
