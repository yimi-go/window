package window

import (
	"sync"
	"time"
)

type Window interface {
	Position() int64
	Append(val int64)
	Add(val int64)
	Aggregation(skipRecent uint) Aggregator
}

type Bucket interface {
	Data() []int64
	Count() int64
}

type bucket struct {
	next  *bucket
	data  []int64
	count int64
}

func (b *bucket) append(val int64) {
	b.data = append(b.data, val)
	b.count++
}

func (b *bucket) add(val int64) {
	if b.count == 0 {
		b.append(val)
		return
	}
	b.data[0] += val
	b.count++
}

func (b *bucket) reset() {
	b.data = b.data[:0]
	b.count = 0
}

func (b *bucket) Data() []int64 {
	return b.data
}

func (b *bucket) Count() int64 {
	return b.count
}

type window struct {
	track        *track
	size         int64
	lastPosition int64
	rwMu         sync.RWMutex
}

func NewWindow(size int, requireBucketDuration time.Duration) Window {
	t := newTrack(size, requireBucketDuration)
	return &window{
		track: t,
		size:  int64(size),
	}
}

func (w *window) Position() int64 {
	unixNano := Now().UnixNano()
	if unixNano < 0 {
		panic("window: we do not support instant before 1970-01-01 00:00:00")
	}
	return unixNano >> w.track.bucketDurationBits
}

func (w *window) Append(val int64) {
	w.rwMu.Lock()
	defer w.rwMu.Unlock()
	position := w.Position()
	w.checkReset(position)
	b := &w.track.buckets[uint64(position)&w.track.bucketIndexMask]
	b.append(val)
	w.lastPosition = position
}

func (w *window) Add(val int64) {
	w.rwMu.Lock()
	defer w.rwMu.Unlock()
	position := w.Position()
	w.checkReset(position)
	b := &w.track.buckets[uint64(position)&w.track.bucketIndexMask]
	b.add(val)
	w.lastPosition = position
}

func (w *window) checkReset(position int64) {
	lastPosition := w.lastPosition
	if lastPosition == position {
		// 不用清数据
		return
	}
	if lastPosition > position {
		// 窗口后退，不可能。当作全新窗口处理，清空数据
		w.resetTo(position, w.size)
		return
	}
	// lastPosition < position
	span := position - lastPosition
	if span >= w.size {
		// 窗口内数据完全失效
		w.resetTo(position, w.size)
		return
	}
	// 清除从上次写的位置（不包括）到当前写位置（包括）
	w.resetTo(position, span)
}

func (w *window) resetTo(position int64, count int64) {
	offset := uint64(position+int64(len(w.track.buckets))-count) & w.track.bucketIndexMask
	b := &w.track.buckets[offset]
	for i := int64(0); i < count; i++ {
		b = b.next
		b.reset()
	}
}

func (w *window) Aggregation(skipRecent uint) Aggregator {
	return &windowAgg{
		skipRecent: skipRecent,
		w:          w,
	}
}
