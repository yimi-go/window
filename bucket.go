package window

import (
	"sync/atomic"
)

type bucket struct {
	track    *track
	position int

	round int64
	data  []int64
}

func (b *bucket) isDataOfWindowState(state windowState) bool {
	bucketRound := atomic.LoadInt64(&b.round)
	round, position, size := state.round, state.position, state.size
	if bucketRound == round {
		// bucket 数据在窗口 round 内
		if b.position > position {
			// 当前 bucket 在窗口右侧还往右。不在窗口之内。
			return false
		}
		if b.position < position-size+1 {
			// 当前 bucket 在窗口左侧还往左。不在窗口之内。
			return false
		}
		return true
	}
	// bucket 数据不在当前 round 内
	if position+1 >= size {
		// 窗口完全在本 round 内，而 bucket 数据不在当前 round 内，无效
		return false
	}
	//  因为窗口有部分在上一 round 尾部。想要 bucket 数据有效，bucket 必须
	// 1) bucketRound == round - 1
	if bucketRound != round-1 {
		// 不满足，无效
		return false
	}
	// 2) b.position >= leftPosition
	// 此时要其数据在效，bucket 必须在窗口左侧之右
	leftPosition := position + len(b.track.buckets) + 1 - size
	if b.position < leftPosition {
		// 当前 bucket 在窗口左侧往左，无效
		return false
	}
	return true
}

func (b *bucket) readDataOfWindowState(state windowState) []int64 {
	if b.isDataOfWindowState(state) {
		return b.data
	}
	return nil
}

func (b *bucket) append(state windowState, val int64) {
	if b.isDataOfWindowState(state) {
		b.data = append(b.data, val)
		return
	}
	b.round, b.data = state.round, b.data[:0]
	b.data = append(b.data, val)
}
