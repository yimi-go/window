package window

import (
	"time"
)

type track struct {
	buckets            []bucket
	bucketDurationBits int
	bucketIndexMask    uint64
}

func newTrack(requireBuckets int, requireBucketDuration time.Duration) *track {
	if requireBuckets <= 0 {
		panic("window: bucket size can not be not positive")
	}
	if requireBucketDuration <= 0 {
		panic("window: bucket duration can not be not positive")
	}
	bucketSize := nextGreaterPo2(uint64(requireBuckets))
	bucketDuration := time.Duration(lastPo2(uint64(requireBucketDuration)))
	bucketDurationMask := uint64(bucketDuration) - 1

	tr := &track{
		bucketDurationBits: countContinuouslyBits(bucketDurationMask),
		bucketIndexMask:    bucketSize - 1,
		buckets:            make([]bucket, bucketSize),
	}
	for i := uint64(0); i < bucketSize; i++ {
		tr.buckets[i].next = &tr.buckets[(i+1)%bucketSize]
	}
	return tr
}
