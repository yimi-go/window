package window

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_window_Position(t *testing.T) {
	bucketDuration := time.Duration(1 << 30)
	w := NewWindow(20, bucketDuration).(*window)
	now := Now()
	Now = func() time.Time {
		return now
	}
	defer func() {
		Now = time.Now
	}()
	t.Run("now_before_1970", func(t *testing.T) {
		defer func() {
			rerr := recover()
			assert.NotNil(t, rerr)
		}()
		now = time.Unix(-1, 0)
		w.Position()
	})
	t.Run("common", func(t *testing.T) {
		location, err := time.LoadLocation("Asia/Shanghai")
		assert.Nil(t, err)
		now = time.Date(2022, time.September, 26, 22, 47, 15, 0, location)
		assert.Equal(t, int64(uint64(now.UnixNano()) & ^uint64(bucketDuration-1))>>30, w.Position())
	})
}

func Test_window_Append(t *testing.T) {
	bucketDuration := time.Duration(1 << 30)
	w := NewWindow(20, bucketDuration).(*window)
	now := Now()
	Now = func() time.Time {
		return now
	}
	defer func() {
		Now = time.Now
	}()
	round, position := w.track.windowRoundAndPosition(now)
	assert.NotEqual(t, round, w.track.buckets[position].round)
	assert.Empty(t, w.track.buckets[position].data)
	w.Append(100)
	for i := 0; i < len(w.track.buckets); i++ {
		if i == position {
			assert.Equal(t, round, w.track.buckets[i].round)
			assert.Equal(t, []int64{100}, w.track.buckets[i].data)
		} else {
			assert.NotEqual(t, round, w.track.buckets[i].round)
			assert.Empty(t, w.track.buckets[i].data)
		}
	}
}

func Test_window_Aggregation(t *testing.T) {
	bucketDuration := time.Duration(1 << 30)
	w := NewWindow(20, bucketDuration).(*window)
	now := Now()
	Now = func() time.Time {
		return now
	}
	defer func() {
		Now = time.Now
	}()
	round, position := w.track.windowRoundAndPosition(now)
	tests := []struct {
		leftSkip, rightSkip uint
		want                Aggregator
	}{
		{
			leftSkip:  0,
			rightSkip: 0,
			want: &trackRangeAgg{
				state: windowState{
					size:     w.size,
					round:    round,
					position: position,
				},
				track:     w.track,
				locker:    w.rwMu.RLocker(),
				leftSkip:  0,
				rightSkip: 0,
			},
		},
		{
			leftSkip:  0,
			rightSkip: 1,
			want: &trackRangeAgg{
				state: windowState{
					size:     w.size,
					round:    round,
					position: position,
				},
				track:     w.track,
				locker:    w.rwMu.RLocker(),
				leftSkip:  0,
				rightSkip: 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d,%d", tt.leftSkip, tt.rightSkip), func(t *testing.T) {
			got := w.Aggregation(tt.leftSkip, tt.rightSkip)
			assert.Equal(t, tt.want, got)
		})
	}
}
