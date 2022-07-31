package window

import (
	"time"

	"github.com/yimi-go/iter"
)

// window is a stat window that contains multiple Buckets.
type window struct {
	track *track
	size  int64
}

func (w *window) round() (round int64, offset int64) {
	nowMilli := NowFunc().Truncate(time.Millisecond).UnixMilli()
	offset = (nowMilli - w.track.initUnixMilli) >> (w.track.bucketMillisBits)
	return nowMilli >> w.track.cycleMillisBits, offset & w.track.bucketMask
}

// Append appends point value to the head of the window.
// Usually, the right head of a window is the head of the sliding window.
func (w *window) append(point float64) {
	round, offset := w.round()
	bkt := w.track.buckets[offset&w.track.bucketMask]
	if bkt.round != round {
		bkt.reset(round)
	}
	bkt.append(point)
}

// Iterate returns an Iterator that iterates every Bucket in the window.
func (w *window) Iterate() iter.Iterator[Bucket] {
	round, offset := w.round()
	start, end := (offset+w.track.size-w.size+1)&w.track.bucketMask, offset
	if start > end {
		end += w.track.size
	}
	return &windowIter{
		buckets:    w.track.buckets,
		bucketMask: w.track.bucketMask,
		round:      round,
		offset:     offset,
		cur:        start,
		end:        end,
	}
}

type windowIter struct {
	buckets    []*bucket
	bucketMask int64
	round      int64
	offset     int64
	cur        int64
	end        int64
}

func (it *windowIter) Next() (Bucket, bool) {
	if it.cur > it.end {
		return nil, false
	}
	offset := it.cur & it.bucketMask
	bkt := it.buckets[offset]
	shouldRound := shouldRound(it.round, it.offset, offset)
	if bkt.round != shouldRound {
		bkt.reset(shouldRound)
	}
	it.cur++
	return bkt, true
}

func newWindow(size int64, bucketMillis int64) *window {
	w := &window{
		track: newTrack(size, bucketMillis),
		size:  size,
	}
	return w
}
