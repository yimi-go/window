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
	assert.Equal(t, bucketDuration, tr.bucketDuration)
	assert.Equal(t, uint64(1<<30-1), tr.bucketDurationMask)
	assert.Equal(t, 30, tr.bucketDurationMaskBits)
	assert.Equal(t, ^uint64(1<<30-1), tr.bucketNanoMask)
	assert.Equal(t, 31, tr.bucketPositionMask)
	for i := range tr.buckets {
		assert.Same(t, tr, tr.buckets[i].track)
		assert.Equal(t, i, tr.buckets[i].position)
	}
	assert.Equal(t, roundDuration, tr.roundDuration)
	assert.Equal(t, roundMask, tr.roundDurationMask)
	assert.Equal(t, 35, tr.roundDurationMaskBits)
	assert.Equal(t, ^roundMask, tr.roundNanoMask)
	t.Logf("bucket duration: %s", tr.bucketDuration)
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

func Test_track_windowRoundAndPosition(t *testing.T) {
	bucketDuration := time.Duration(1 << 30)
	roundDuration := bucketDuration << 5
	roundMask := uint64(roundDuration) - 1
	nowNano := uint64(time.Now().UnixNano()) & ^roundMask
	nowSec := nowNano / 1000000000
	now := time.Unix(int64(nowSec), int64(nowNano%nowSec))
	Now = func() time.Time { return now }
	defer func() {
		Now = time.Now
	}()
	tr := newTrack(20, bucketDuration+20*time.Millisecond)
	tests := []struct {
		name         string
		instant      time.Time
		wantPanic    bool
		wantRound    int64
		wantPosition int
	}{
		{
			name:      "panic",
			instant:   time.UnixMilli(-1),
			wantPanic: true,
		},
		{
			name:         "ok",
			instant:      now.Add(bucketDuration * 25),
			wantPosition: 25,
			wantRound:    int64(nowNano) >> 35,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				rerr := recover()
				assert.Equal(t, tt.wantPanic, rerr != nil, "windowRoundAndPosition(%v)", tt.instant)
			}()
			gotRound, gotPosition := tr.windowRoundAndPosition(tt.instant)
			assert.Equalf(t, tt.wantRound, gotRound, "windowRoundAndPosition(%v)", tt.instant)
			assert.Equalf(t, tt.wantPosition, gotPosition, "windowRoundAndPosition(%v)", tt.instant)
		})
	}
}
