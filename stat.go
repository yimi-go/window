package window

import "github.com/yimi-go/iter"

// StatWindow is a sliding time statistics window.
type StatWindow interface {
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
	Count() uint64
	// Reduce applies the reduction function to all values within the window.
	Reduce(func(float64) float64) float64
}

type po2stat struct {
	w *po2Window
}

func (p *po2stat) Add(v float64) {
	p.w.Append(v)
}

func (p *po2stat) Min() float64 {
	round, offset := windowNowRound(p.w)
	points := iter.FlatMap(p.w.Iterate(offset), func(t *matrixPo2Bucket) iter.Iterator[float64] {
		return t.Points(round, offset)
	})
	min, _ := iter.Min(points)
	return min
}

func (p *po2stat) Max() float64 {
	round, offset := windowNowRound(p.w)
	points := iter.FlatMap(p.w.Iterate(offset), func(t *matrixPo2Bucket) iter.Iterator[float64] {
		return t.Points(round, offset)
	})
	max, _ := iter.Max(points)
	return max
}

func (p *po2stat) Avg() float64 {
	round, offset := windowNowRound(p.w)
	points := iter.FlatMap(p.w.Iterate(offset), func(t *matrixPo2Bucket) iter.Iterator[float64] {
		return t.Points(round, offset)
	})
	mean, _ := iter.Mean(points)
	return mean
}

func (p *po2stat) Sum() float64 {
	round, offset := windowNowRound(p.w)
	points := iter.FlatMap(p.w.Iterate(offset), func(t *matrixPo2Bucket) iter.Iterator[float64] {
		return t.Points(round, offset)
	})
	sum, _ := iter.Sum(points)
	return sum
}

func (p *po2stat) Count() uint64 {
	round, offset := windowNowRound(p.w)
	points := iter.FlatMap(p.w.Iterate(offset), func(t *matrixPo2Bucket) iter.Iterator[float64] {
		return t.Points(round, offset)
	})
	return iter.Count(points)
}

func (p *po2stat) Reduce(fn func(float64) float64) float64 {
	round, offset := windowNowRound(p.w)
	points := iter.FlatMap(p.w.Iterate(offset), func(t *matrixPo2Bucket) iter.Iterator[float64] {
		return t.Points(round, offset)
	})
	reduce, _ := iter.Reduce(points, 0, func(_ float64, e float64) float64 {
		return fn(e)
	})
	return reduce
}

type options struct {
	requireBuckets      uint64
	requireBucketMillis uint64
	bucketSpanSize      uint64
}

// Option is option function for creating a power-of-two stat window.
type Option func(o *options)

// RequireBuckets creates an Option that set buckets num that must have in window.
func RequireBuckets(buckets uint64) Option {
	return func(o *options) {
		o.requireBuckets = buckets
	}
}

// RequireBucketsMillis creates an Option that set maximum bucket duration
// length in milliseconds for a window.
func RequireBucketsMillis(bucketMillis uint64) Option {
	return func(o *options) {
		o.requireBucketMillis = bucketMillis
	}
}

// BucketSpanSize creates an Option that set span size for the buckets in the window.
// This size must be power-of-two.
func BucketSpanSize(spanSize uint64) Option {
	return func(o *options) {
		o.bucketSpanSize = spanSize
	}
}

// NewPo2StatWindow creates a new power-of-two stat window by options.
// By default, the created stat window ensures 16 buckets and
// provides bucket precision at 512 milliseconds.
func NewPo2StatWindow(opts ...Option) StatWindow {
	o := &options{
		requireBuckets:      16,
		requireBucketMillis: 512,
		bucketSpanSize:      512,
	}
	for _, opt := range opts {
		opt(o)
	}
	return &po2stat{
		w: newPo2Window(o.requireBuckets, o.requireBucketMillis, o.bucketSpanSize),
	}
}
