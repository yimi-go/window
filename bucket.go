package window

// Bucket is stat bucket of Window.
type Bucket interface {
	// Points returns bucket stat points.
	Points() []float64
}

type bucket struct {
	// round is the window round number this bucket belongs to.
	round int64
	// points stores buckets data.
	points []float64
}

func newBucket() *bucket {
	return &bucket{
		points: make([]float64, 0, 512),
	}
}

func (b *bucket) Points() []float64 {
	return b.points
}

func (b *bucket) append(point float64) {
	b.points = append(b.points, point)
}

func (b *bucket) reset(round int64) {
	b.points = b.points[:0]
	b.round = round
}
