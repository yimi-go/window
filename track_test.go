package window

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_newTrack(t *testing.T) {
	bucketDuration := time.Duration(1 << 30)
	roundDuration := bucketDuration << 5
	roundMask := uint64(roundDuration) - 1
	nowNano := uint64(time.Now().UnixNano()) & ^roundMask
	nowSec := nowNano / 1000
	now := time.Unix(int64(nowSec), int64(nowNano%nowSec))
	Now = func() time.Time { return now }
	defer func() {
		Now = time.Now
	}()
	tr := newTrack(20, bucketDuration+20*time.Millisecond)
	assert.Equal(t, 32, len(tr.buckets))
	assert.Equal(t, 30, tr.bucketDurationBits)
	assert.Equal(t, uint64(31), tr.bucketIndexMask)
	for i := range tr.buckets {
		assert.Equal(t, &tr.buckets[(i+1)%len(tr.buckets)], tr.buckets[i].next)
	}
}

func Test_newTrack_panic_buckets(t *testing.T) {
	defer func() {
		if rerr := recover(); rerr == nil {
			t.Errorf("expect err, got nil")
		}
	}()
	_ = newTrack(-1, time.Second)
}

func Test_newTrack_panic_duration(t *testing.T) {
	defer func() {
		if rerr := recover(); rerr == nil {
			t.Errorf("expect err, got nil")
		}
	}()
	_ = newTrack(10, -1)
}
