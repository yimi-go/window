package window

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func buildAgg(skipRecent uint) (agg *windowAgg, fbt time.Time, bd time.Duration) {
	bd = time.Duration(1 << 30)
	w := NewWindow(16, bd).(*window)
	agg = w.Aggregation(skipRecent).(*windowAgg)
	bits := countContinuouslyBits(1<<30-1) + countContinuouslyBits(nextGreaterPo2(20)-1)
	nsec := time.Now().UnixNano()
	nsec >>= bits
	nsec <<= bits
	sec := nsec / 1000_000_000
	nsec %= sec
	fbt = time.Unix(sec, nsec)
	now := fbt.Add(bd * 15)
	Now = func() time.Time { return now }
	defer func() { Now = time.Now }()
	position := w.Position()
	w.lastPosition = position
	for i := range w.track.buckets {
		b := &w.track.buckets[i]
		for j := 0; j < i; j++ {
			b.data = append(b.data, int64(j+1))
		}
		b.count = int64(i)
	}
	return
}

func Test_windowAgg_Reduce(t *testing.T) {
	tests := []struct {
		skipRecent uint
		want       int64
	}{
		{
			0, 120,
		},
		{
			2, 91,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.skipRecent), func(t *testing.T) {
			agg, fbt, bd := buildAgg(tt.skipRecent)
			now := fbt.Add(bd * 15)
			Now = func() time.Time { return now }
			defer func() { Now = time.Now }()
			var sum int64
			agg.Reduce(func(bucket Bucket) (done bool) {
				max, hasValue := int64(math.MinInt64), false
				for _, v := range bucket.Data() {
					hasValue = true
					if max < v {
						max = v
					}
				}
				if hasValue {
					sum += max
				}
				return false
			})
			assert.Equal(t, tt.want, sum)
		})
	}
}

func Test_windowAgg_Reduce2(t *testing.T) {
	tests := []struct {
		addPosition int
		want        int64
	}{
		{
			0, 120,
		},
		{
			-1, 0,
		},
		{
			16, 0,
		},
		{
			8, 92,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.addPosition), func(t *testing.T) {
			agg, fbt, bd := buildAgg(0)
			now := fbt.Add(bd * time.Duration(15+tt.addPosition))
			Now = func() time.Time { return now }
			defer func() { Now = time.Now }()
			var sum int64
			agg.Reduce(func(bucket Bucket) (done bool) {
				max, hasValue := int64(math.MinInt64), false
				for _, v := range bucket.Data() {
					hasValue = true
					if max < v {
						max = v
					}
				}
				if hasValue {
					sum += max
				}
				return false
			})
			assert.Equal(t, tt.want, sum)
		})
	}
}

func Test_windowAgg_Reduce3(t *testing.T) {
	tests := []struct {
		addPosition int
		want        int64
	}{
		{
			0, 0,
		},
		{
			-1, 0,
		},
		{
			16, 0,
		},
		{
			8, 8,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.addPosition), func(t *testing.T) {
			agg, fbt, bd := buildAgg(0)
			now := fbt.Add(bd * time.Duration(15+tt.addPosition))
			Now = func() time.Time { return now }
			defer func() { Now = time.Now }()
			var sum int64
			agg.Reduce(func(bucket Bucket) (done bool) {
				max, hasValue := int64(math.MinInt64), false
				for _, v := range bucket.Data() {
					hasValue = true
					if max < v {
						max = v
					}
				}
				if hasValue {
					sum += max
				}
				return true
			})
			assert.Equal(t, tt.want, sum)
		})
	}
}

func Test_windowAgg_Min(t *testing.T) {
	tests := []struct {
		skipRecent uint
		want       int64
	}{
		{
			skipRecent: 0,
			want:       1,
		},
		{
			skipRecent: 19,
			want:       0,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.skipRecent), func(t *testing.T) {
			agg, fbt, bd := buildAgg(tt.skipRecent)
			now := fbt.Add(bd * 15)
			Now = func() time.Time { return now }
			defer func() { Now = time.Now }()
			assert.Equal(t, tt.want, agg.Min())
		})
	}
}

func Test_windowAgg_Max(t *testing.T) {
	tests := []struct {
		skipRecent uint
		want       int64
	}{
		{
			skipRecent: 0,
			want:       15,
		},
		{
			skipRecent: 15,
			want:       0,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.skipRecent), func(t *testing.T) {
			agg, fbt, bd := buildAgg(tt.skipRecent)
			now := fbt.Add(bd * 15)
			Now = func() time.Time { return now }
			defer func() { Now = time.Now }()
			assert.Equal(t, tt.want, agg.Max())
		})
	}
}

func Test_windowAgg_Avg(t *testing.T) {
	tests := []struct {
		skipRecent uint
		want       float64
	}{
		{
			skipRecent: 0,
			want:       680.0 / 120.0,
		},
		{
			skipRecent: 15,
			want:       0.0,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.skipRecent), func(t *testing.T) {
			agg, fbt, bd := buildAgg(tt.skipRecent)
			now := fbt.Add(bd * 15)
			Now = func() time.Time { return now }
			defer func() { Now = time.Now }()
			assert.Equal(t, tt.want, agg.Avg())
		})
	}
}

func Test_windowAgg_Sum(t *testing.T) {
	tests := []struct {
		skipRecent uint
		want       int64
	}{
		{
			skipRecent: 0,
			want:       680,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.skipRecent), func(t *testing.T) {
			agg, fbt, bd := buildAgg(tt.skipRecent)
			now := fbt.Add(bd * 15)
			Now = func() time.Time { return now }
			defer func() { Now = time.Now }()
			assert.Equal(t, tt.want, agg.Sum())
		})
	}
}

func Test_windowAgg_Count(t *testing.T) {
	tests := []struct {
		skipRecent uint
		want       int64
	}{
		{
			skipRecent: 0,
			want:       120,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.skipRecent), func(t *testing.T) {
			agg, fbt, bd := buildAgg(tt.skipRecent)
			now := fbt.Add(bd * 15)
			Now = func() time.Time { return now }
			defer func() { Now = time.Now }()
			assert.Equal(t, tt.want, agg.Count())
		})
	}
}
