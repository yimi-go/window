package window

import (
	"fmt"
	"sync"

	"github.com/yimi-go/iter"
)

type matrixPo2Bucket struct {
	// final area
	// spanSize is the size of each span or line in the matrix.
	spanSize uint64
	// spanBitMask is the bit mask for quick getting append position in a span.
	spanBitMask uint64
	// spanBits is the bit shift size for quick getting the span index to access.
	spanBits uint64
	// offset is the offset of the bucket in the window round.
	// Note that window round is not the same as the window, they have different start position.
	offset uint64

	mu sync.RWMutex
	// state area. should be operated with protection of mutex.
	// matrix is main stat data storage.
	// Every span or line of the matrix is of size power-of-two.
	matrix [][]float64
	// cap is the capacity of the matrix.
	// This is to avoid multiplication costs when calculating the matrix capacity.
	cap uint64
	// round is the window round this bucket belongs to.
	// This field should only be operated in atomic.
	round int64
	// len is the value count in the matrix.
	// It is 0 at initial and after reset.
	len uint64
}

func (b *matrixPo2Bucket) shouldRound(round int64, nowOffset uint64) int64 {
	// for example, current offset is 0, now offset is 1 and round is 3,
	// then bucket 0 and 1 should round 3, and later bucket 2, 3, 4, ..., should round 2.
	if b.offset <= nowOffset {
		return round
	}
	return round - 1
}

func newMatrixPo2Bucket(spanSize uint64, offset uint64) *matrixPo2Bucket {
	if spanSize&(spanSize-1) != 0 {
		panic(fmt.Errorf("window: bucket span size must be power-of-two: 0b%064b", spanSize))
	}
	bits, bitsCounter := uint64(0), spanSize
	for bitsCounter != 1 {
		bitsCounter >>= 1
		bits++
	}
	b := &matrixPo2Bucket{
		spanSize:    spanSize,
		spanBitMask: spanSize - 1,
		spanBits:    bits,
		offset:      offset,
		matrix: [][]float64{
			make([]float64, spanSize),
		},
		cap: spanSize,
	}
	return b
}

// After calling this method, the round must be matched:
// Case 1): round unchanged and now offset unchanged.
//   The buckets round must be matched after resetIfRoundExpired.
// Case 2): round unchanged and now offset changed to the next.
//   Now offset is greater than bucket's offset but round unchanged, match.
// Case 3): round changed and now offset changed to round starting.
//   Now round is 1 greater than bucket's round but now offset is less than bucket's offset, match.
//The operations after calling this method must be able to be finished in a round period.
func (b *matrixPo2Bucket) resetIfRoundExpired(round int64, nowOffset uint64) {
	shouldRound := b.shouldRound(round, nowOffset)
	b.mu.RLock()
	if b.round == shouldRound {
		b.mu.RUnlock()
		return
	}
	b.mu.RUnlock()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.len = 0
	b.matrix = [][]float64{
		make([]float64, b.spanSize),
	}
	b.cap = b.spanSize
	b.round = shouldRound
}

// Points returns bucket stat points iterator.
func (b *matrixPo2Bucket) Points(round int64, nowOffset uint64) iter.Iterator[float64] {
	b.resetIfRoundExpired(round, nowOffset)
	b.mu.RLock()
	defer b.mu.RUnlock()
	it := &matrixIter{
		position:    0,
		spanBits:    b.spanBits,
		spanBitMask: b.spanBitMask,
		matrix:      b.matrix,
		len:         b.len,
	}
	return it
}

// Append appends the point value to the bucket.
func (b *matrixPo2Bucket) Append(round int64, point float64) {
	b.resetIfRoundExpired(round, b.offset)
	b.mu.Lock()
	defer b.mu.Unlock()
	for b.len >= b.cap {
		b.matrix = append(b.matrix, make([]float64, b.spanSize))
		b.cap += b.spanSize
	}
	span, pos := b.len>>b.spanBits, b.len&b.spanBitMask
	b.matrix[span][pos] = point
	b.len++
}

type matrixIter struct {
	spanBits    uint64
	spanBitMask uint64
	matrix      [][]float64
	len         uint64
	position    uint64
}

func (it *matrixIter) Next() (float64, bool) {
	if it.position >= it.len {
		return 0, false
	}
	span, spanPos := it.position>>it.spanBits, it.position&it.spanBitMask
	it.position++
	float64s := it.matrix[span]
	return float64s[spanPos], true
}
