package window

import (
	"time"
)

type track struct {
	buckets          []*bucket
	size             int64
	bucketMask       int64
	bucketMillis     int64
	bucketMillisBits int
	cycleMillis      int64
	cycleMillisBits  int
	initUnixMilli    int64
}

func newTrack(requireBuckets, requireBucketMillis int64) *track {
	size := nextGreaterPo2(requireBuckets)
	bucketMillis := lastPo2(requireBucketMillis)
	bucketMillisBits, tmp := 0, bucketMillis
	for tmp != 1 {
		tmp >>= 1
		bucketMillisBits++
	}
	cycleMillis := size << bucketMillisBits
	cycleMillisBits, tmp := 0, cycleMillis
	for tmp != 1 {
		tmp >>= 1
		cycleMillisBits++
	}
	nowMillis := NowFunc().Truncate(time.Millisecond).UnixMilli()
	initUnixMilli := nowMillis & ((1<<63 - 1) ^ (cycleMillis - 1))
	t := &track{
		buckets:          make([]*bucket, size),
		size:             size,
		bucketMask:       size - 1,
		bucketMillis:     bucketMillis,
		bucketMillisBits: bucketMillisBits,
		cycleMillis:      cycleMillis,
		cycleMillisBits:  cycleMillisBits,
		initUnixMilli:    initUnixMilli,
	}
	for i := int64(0); i < size; i++ {
		t.buckets[i] = newBucket()
	}
	return t
}
