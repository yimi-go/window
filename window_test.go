package window

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/yimi-go/iter"
)

func TestPo2Window_Iterate(t *testing.T) {
	now := time.Now()
	NowFunc = func() time.Time { return now }
	defer func() {
		NowFunc = time.Now
	}()
	w := newPo2Window(8, 16, 512)
	_, offset := windowNowRound(w)
	got := w.Iterate(offset)
	want := &po2Iter{
		cur: offset + 1,
		end: offset + 8 + 1,
		w:   w,
	}
	assert.Equal(t, want, got)
	assert.Equal(t, uint64(8), iter.Count(got))
}

func TestPo2Window_Append(t *testing.T) {
	//now := time.UnixMilli(1659008022464)
	now := time.Now()
	NowFunc = func() time.Time { return now }
	defer func() {
		NowFunc = time.Now
	}()
	w := newPo2Window(8, 16, 512)
	_, offset := windowNowRound(w)
	w.Append(1.0)
	assert.Equal(t, uint64(1), iter.Count(w.buckets[offset].Points(windowNowRound(w))))
	v, ok := w.buckets[offset].Points(windowNowRound(w)).Next()
	assert.True(t, ok)
	assert.Equal(t, 1.0, v)
	now = now.Add(16 * time.Millisecond)
	w.Append(1.0)
	assert.Equal(t, uint64(1), iter.Count(w.buckets[offset].Points(windowNowRound(w))), "now: %d", now.UnixMilli())
	offsetNext := (offset + 1) & w.bucketMask
	assert.Equal(t, uint64(1), iter.Count(w.buckets[offsetNext].Points(windowNowRound(w))), "now: %d", now.UnixMilli())
	now = now.Add(time.Duration(len(w.buckets)) * 16 * time.Millisecond)
	w.Append(1.0)
	assert.Equal(t, uint64(1), iter.Count(w.buckets[offsetNext].Points(windowNowRound(w))), "now: %d", now.UnixMilli())
	v, ok = w.buckets[offsetNext].Points(windowNowRound(w)).Next()
	assert.True(t, ok)
	assert.Equal(t, 1.0, v)
}
