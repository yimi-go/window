package window

import (
	"sync"
	"time"
)

type Window interface {
	Position() int64
	Append(val int64)
	Aggregation(leftSkip, rightSkip uint) Aggregator
}

type windowState struct {
	size     int
	round    int64
	position int
}

type window struct {
	rwMu  sync.RWMutex
	track *track
	size  int
}

func NewWindow(size int, requireBucketDuration time.Duration) Window {
	t := newTrack(size, requireBucketDuration)
	return &window{
		track: t,
		size:  size,
	}
}

func (w *window) Position() int64 {
	unixNano := Now().UnixNano()
	if unixNano < 0 {
		panic("window: we do not support instant before 1970-01-01 00:00:00")
	}
	return int64(uint64(unixNano)&w.track.bucketNanoMask) >> w.track.bucketDurationMaskBits
}

func (w *window) Append(val int64) {
	w.rwMu.Lock()
	defer func() {
		w.rwMu.Unlock()
	}()
	now := Now()
	round, position := w.track.windowRoundAndPosition(now)
	state := windowState{w.size, round, position}
	w.track.buckets[position].append(state, val)
}

func (w *window) Aggregation(leftSkip, rightSkip uint) Aggregator {
	now := Now()
	round, position := w.track.windowRoundAndPosition(now)
	return &trackRangeAgg{
		state: windowState{
			size:     w.size,
			round:    round,
			position: position,
		},
		track:     w.track,
		locker:    w.rwMu.RLocker(),
		leftSkip:  leftSkip,
		rightSkip: rightSkip,
	}
}
