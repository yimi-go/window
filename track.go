package window

import (
	"time"
)

type track struct {
	// 每个 bucket 的时长。要求 int64 值是二的幂。
	bucketDuration time.Duration
	// bucket 时长掩码。
	bucketDurationMask     uint64
	bucketDurationMaskBits int
	bucketNanoMask         uint64
	bucketPositionMask     int
	// 轨道上的桶。要求其长度是二的幂。
	// 通常取值为：按需要的窗口 bucket 数向上找到可以满足的最小二的幂。
	// 不能与要求的窗口 bucket 数相同。要给滑动前后留出空间。
	buckets []bucket
	// 轨道一圈耗时。要求 int64 值是二的幂。
	roundDuration time.Duration
	// 轨道周期掩码。
	roundDurationMask     uint64
	roundDurationMaskBits int
	roundNanoMask         uint64
}

func newTrack(requireBuckets int, requireBucketDuration time.Duration) *track {
	if requireBuckets <= 0 {
		panic("window: bucket size can not be not positive")
	}
	if requireBucketDuration <= 0 {
		panic("window: bucket duration can not be not positive")
	}
	bucketSize := int(nextGreaterPo2(uint64(requireBuckets)))
	bucketDuration := time.Duration(lastPo2(uint64(requireBucketDuration)))
	bucketDurationMask := uint64(bucketDuration) - 1
	roundDuration := bucketDuration * time.Duration(bucketSize)
	roundDurationMask := uint64(roundDuration) - 1

	track := &track{
		bucketDuration:         bucketDuration,
		bucketDurationMask:     bucketDurationMask,
		bucketDurationMaskBits: countContinuouslyBits(bucketDurationMask),
		bucketNanoMask:         ^bucketDurationMask,
		bucketPositionMask:     bucketSize - 1,
		buckets:                make([]bucket, bucketSize),
		roundDuration:          roundDuration,
		roundDurationMask:      roundDurationMask,
		roundDurationMaskBits:  countContinuouslyBits(roundDurationMask),
		roundNanoMask:          ^roundDurationMask,
	}
	for i := 0; i < bucketSize; i++ {
		track.buckets[i].track = track
		track.buckets[i].position = i
	}
	return track
}

func (t *track) windowRoundAndPosition(instant time.Time) (round int64, position int) {
	unixNano := instant.UnixNano()
	if unixNano < 0 {
		panic("window: we do not support instant before 1970-01-01 00:00:00")
	}
	round = int64((uint64(unixNano) & t.roundNanoMask) >> t.roundDurationMaskBits)
	nanoInRound := uint64(unixNano) & t.roundDurationMask
	position = int((nanoInRound & t.bucketNanoMask) >> t.bucketDurationMaskBits)
	return
}
