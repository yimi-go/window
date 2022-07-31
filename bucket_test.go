package window

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBucket_Points(t *testing.T) {
	bkt := newBucket()
	points := bkt.Points()
	assert.Empty(t, points)
}

func TestBucket_append(t *testing.T) {
	bkt := newBucket()
	bkt.append(1.2)
	assert.Equal(t, []float64{1.2}, bkt.points)
}

func TestBucket_reset(t *testing.T) {
	bkt := newBucket()
	bkt.points = []float64{1.2, 3.4}
	bkt.reset(999)
	assert.Empty(t, bkt.points)
	assert.Equal(t, int64(999), bkt.round)
}
