package window

import (
	"sync"

	"github.com/yimi-go/iter"
)

// RollingCounter represents a ring window based on time duration.
type RollingCounter interface {
	// Add adds the given value to the window.
	Add(float64)
	// Min finds the min value within the window.
	Min() float64
	// Max finds the max value within the window.
	Max() float64
	// Avg computes average value within the window.
	Avg() float64
	// Sum computes sum value within the window.
	Sum() float64
	// Count counts values within the window.
	Count() int64
	// Reduce applies the reduction function to all buckets within the window.
	Reduce(func(iter.Iterator[Bucket]) float64) float64
}

// RollingCounterOpts contains the arguments for creating RollingCounter.
type RollingCounterOpts struct {
	WindowSize          int
	RequireBucketMillis int64
}

type rollingCounter struct {
	mu sync.RWMutex
	w  *window
}

// NewRollingCounter creates a new RollingCounter bases on RollingCounterOpts.
func NewRollingCounter(opts RollingCounterOpts) RollingCounter {
	return &rollingCounter{
		w: newWindow(int64(opts.WindowSize), opts.RequireBucketMillis),
	}
}

func (r *rollingCounter) Add(val float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.w.append(val)
}

var bucketPointsMapFn = func(b Bucket) iter.Iterator[float64] {
	return iter.Slice(b.Points())
}

func (r *rollingCounter) Avg() float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	avg, _ := iter.Mean(iter.FlatMap(r.w.Iterate(), bucketPointsMapFn))
	return avg
}

func (r *rollingCounter) Min() float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	min, _ := iter.Min(iter.FlatMap(r.w.Iterate(), bucketPointsMapFn))
	return min
}

func (r *rollingCounter) Max() float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	max, _ := iter.Max(iter.FlatMap(r.w.Iterate(), bucketPointsMapFn))
	return max
}

func (r *rollingCounter) Sum() float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	sum, _ := iter.Sum(iter.FlatMap(r.w.Iterate(), bucketPointsMapFn))
	return sum
}

func (r *rollingCounter) Count() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	count := iter.Count(iter.FlatMap(r.w.Iterate(), bucketPointsMapFn))
	return int64(count)
}

func (r *rollingCounter) Reduce(f func(iter.Iterator[Bucket]) float64) float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return f(r.w.Iterate())
}
