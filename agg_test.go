package window

import (
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func buildAgg(leftSkip, rightSkip uint) *trackRangeAgg {
	agg := &trackRangeAgg{
		state: windowState{
			size:     20,
			position: 19,
		},
		track: &track{
			buckets:            make([]bucket, 32),
			bucketPositionMask: 31,
		},
		locker:    &sync.Mutex{},
		leftSkip:  leftSkip,
		rightSkip: rightSkip,
	}
	for i := range agg.track.buckets {
		for j := 0; j < i; j++ {
			agg.track.buckets[i].data = append(agg.track.buckets[i].data, int64(j)+1)
		}
	}
	return agg
}

func Test_trackRangeAgg_Reduce(t *testing.T) {
	tests := []struct {
		leftSkip, rightSkip uint
		want                int64
	}{
		{
			0, 0, 190,
		},
		{
			2, 0, 189,
		},
		{
			0, 2, 153,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d,%d", tt.leftSkip, tt.rightSkip), func(t *testing.T) {
			agg := buildAgg(tt.leftSkip, tt.rightSkip)
			var sum int64
			agg.Reduce(func(data []int64) {
				max, hasValue := int64(math.MinInt64), false
				for _, v := range data {
					hasValue = true
					if max < v {
						max = v
					}
				}
				if hasValue {
					sum += max
				}
			})
			assert.Equal(t, tt.want, sum)
		})
	}
}

func Test_trackRangeAgg_Min(t *testing.T) {
	agg := buildAgg(0, 0)
	assert.Equal(t, int64(1), agg.Min())
	agg = buildAgg(0, 19)
	assert.Equal(t, int64(0), agg.Min())
}

func Test_trackRangeAgg_Max(t *testing.T) {
	agg := buildAgg(0, 0)
	assert.Equal(t, int64(19), agg.Max())
	agg = buildAgg(0, 19)
	assert.Equal(t, int64(0), agg.Max())
}

func Test_trackRangeAgg_Avg(t *testing.T) {
	agg := buildAgg(0, 0)
	assert.Equal(t, 7.0, agg.Avg())
	agg = buildAgg(0, 19)
	assert.Equal(t, 0.0, agg.Avg())
}

func Test_trackRangeAgg_Sum(t *testing.T) {
	agg := buildAgg(0, 0)
	assert.Equal(t, int64(1330), agg.Sum())
}

func Test_trackRangeAgg_Count(t *testing.T) {
	agg := buildAgg(0, 0)
	assert.Equal(t, int64(190), agg.Count())
}
