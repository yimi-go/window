package window

import (
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
	position := w.Position()
	offset := int(uint64(position) & w.track.bucketIndexMask)
	assert.Empty(t, w.track.buckets[offset].data)
	assert.Equal(t, int64(0), w.track.buckets[offset].count)
	w.Append(100)
	for i := 0; i < len(w.track.buckets); i++ {
		if i == offset {
			assert.Equal(t, []int64{100}, w.track.buckets[i].data)
			assert.Equal(t, int64(1), w.track.buckets[i].count)
		} else {
			assert.Empty(t, w.track.buckets[i].data)
			assert.Equal(t, int64(0), w.track.buckets[i].count)
		}
	}
	w.Append(100)
	for i := 0; i < len(w.track.buckets); i++ {
		if i == offset {
			assert.Equal(t, []int64{100, 100}, w.track.buckets[i].data)
			assert.Equal(t, int64(2), w.track.buckets[i].count)
		} else {
			assert.Empty(t, w.track.buckets[i].data)
			assert.Equal(t, int64(0), w.track.buckets[i].count)
		}
	}
}

func Test_window_Add(t *testing.T) {
	bucketDuration := time.Duration(1 << 30)
	w := NewWindow(20, bucketDuration).(*window)
	now := Now()
	Now = func() time.Time {
		return now
	}
	defer func() {
		Now = time.Now
	}()
	position := w.Position()
	offset := int(uint64(position) & w.track.bucketIndexMask)
	assert.Empty(t, w.track.buckets[offset].data)
	assert.Equal(t, int64(0), w.track.buckets[offset].count)
	w.Add(100)
	for i := 0; i < len(w.track.buckets); i++ {
		if i == offset {
			assert.Equal(t, []int64{100}, w.track.buckets[i].data)
			assert.Equal(t, int64(1), w.track.buckets[i].count)
		} else {
			assert.Empty(t, w.track.buckets[i].data)
			assert.Equal(t, int64(0), w.track.buckets[i].count)
		}
	}
	w.Add(100)
	for i := 0; i < len(w.track.buckets); i++ {
		if i == offset {
			assert.Equal(t, []int64{200}, w.track.buckets[i].data)
			assert.Equal(t, int64(2), w.track.buckets[i].count)
		} else {
			assert.Empty(t, w.track.buckets[i].data)
			assert.Equal(t, int64(0), w.track.buckets[i].count)
		}
	}
}

func Test_window_checkReset(t *testing.T) {
	tests := []struct {
		getWin    func() *window
		wantEmpty map[int]struct{}
		name      string
		position  int64
	}{
		{
			name: "just_the_last_position",
			getWin: func() *window {
				return windowAtLastPosition(32)
			},
			position:  32,
			wantEmpty: map[int]struct{}{},
		},
		{
			name: "impossible_before_last_position",
			getWin: func() *window {
				return windowAtLastPosition(33)
			},
			position: 32,
			wantEmpty: func() map[int]struct{} {
				mp := map[int]struct{}{}
				for i := 32 - 16 + 1; i < 32; i++ {
					mp[i] = struct{}{}
				}
				mp[0] = struct{}{}
				return mp
			}(),
		},
		{
			name: "window_fully_rolling_out",
			getWin: func() *window {
				return windowAtLastPosition(16)
			},
			position: 32,
			wantEmpty: func() map[int]struct{} {
				mp := map[int]struct{}{}
				for i := 32 - 16 + 1; i < 32; i++ {
					mp[i] = struct{}{}
				}
				mp[0] = struct{}{}
				return mp
			}(),
		},
		{
			name: "window_partial_rolling_out",
			getWin: func() *window {
				return windowAtLastPosition(24)
			},
			position: 32,
			wantEmpty: func() map[int]struct{} {
				mp := map[int]struct{}{}
				for i := 25; i < 32; i++ {
					mp[i] = struct{}{}
				}
				mp[0] = struct{}{}
				return mp
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := tt.getWin()
			w.checkReset(tt.position)
			for i := 0; i < 32; i++ {
				if _, ok := tt.wantEmpty[i]; ok {
					assert.Emptyf(t, w.track.buckets[i].data, "bucket[%d]", i)
				} else {
					assert.NotEmptyf(t, w.track.buckets[i].data, "bucket[%d]", i)
				}
			}
		})
	}
}

func windowAtLastPosition(lastPosition int64) *window {
	w := NewWindow(16, time.Duration(1<<30)).(*window)
	for i := range w.track.buckets {
		w.track.buckets[i].data = []int64{1, 2, 3}
	}
	w.lastPosition = lastPosition
	return w
}
