package window

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEnsureBuckets(t *testing.T) {
	o := &options{}
	RequireBuckets(999)(o)
	assert.Equal(t, uint64(999), o.requireBuckets)
}

func TestRequirePrecisionMillis(t *testing.T) {
	o := &options{}
	RequireBucketsMillis(999)(o)
	assert.Equal(t, uint64(999), o.requireBucketMillis)
}

func TestBucketSpanSize(t *testing.T) {
	o := &options{}
	BucketSpanSize(999)(o)
	assert.Equal(t, uint64(999), o.bucketSpanSize)
}

func TestNewPo2StatWindow(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		s := NewPo2StatWindow().(*po2stat)
		assert.Equal(t, 16, len(s.w.buckets))
		assert.Equal(t, uint64(512), s.w.bucketMillis)
	})
	t.Run("opts", func(t *testing.T) {
		count := 0
		_ = NewPo2StatWindow(func(o *options) {
			count++
		})
		assert.Equal(t, 1, count)
	})
}

func TestPo2stat_Add(t *testing.T) {
	now := time.Now()
	NowFunc = func() time.Time { return now }
	defer func() {
		NowFunc = time.Now
	}()
	s := NewPo2StatWindow(RequireBuckets(3), RequireBucketsMillis(1000))
	now = time.UnixMilli(s.(*po2stat).w.initUnixMilli)
	listPoints := func() []float64 {
		points := make([]float64, 0)
		s.Reduce(func(p float64) float64 {
			points = append(points, p)
			return 0
		})
		return points
	}
	assert.Equal(t, []float64{}, listPoints())
	s.Add(1)
	assert.Equal(t, []float64{1.0}, listPoints())
	bucketDuration := time.Millisecond * time.Duration(s.(*po2stat).w.bucketMillis)
	now = now.Add(bucketDuration)
	s.Add(2)
	s.Add(3)
	assert.Equal(t, []float64{1, 2, 3}, listPoints())
	now = now.Add(bucketDuration)
	now = now.Add(bucketDuration)
	now = now.Add(bucketDuration)
	s.Add(4)
	assert.Equal(t, []float64{2, 3, 4}, listPoints())
	now = now.Add(bucketDuration)
	s.Add(5)
	assert.Equal(t, []float64{4, 5}, listPoints())
}

func TestPo2stat_Reduces(t *testing.T) {
	now := time.Now()
	NowFunc = func() time.Time { return now }
	defer func() {
		NowFunc = time.Now
	}()
	s := NewPo2StatWindow(RequireBuckets(3), RequireBucketsMillis(1000))
	now = time.UnixMilli(s.(*po2stat).w.initUnixMilli)
	bucketCount := len(s.(*po2stat).w.buckets)
	bucketDuration := time.Duration(s.(*po2stat).w.bucketMillis) * time.Millisecond
	for i := 0; i < bucketCount; i++ {
		for j := 0; j <= i; j++ {
			s.Add(float64(i + 1))
		}
		if i < bucketCount-1 {
			now = now.Add(bucketDuration)
		}
	}
	var result float64
	_ = s.Reduce(func(p float64) float64 {
		result += p
		return 0
	})
	assert.Equal(t, 30.0, result)
	assert.Equal(t, 3.0, s.Avg())
	assert.Equal(t, 1.0, s.Min())
	assert.Equal(t, 4.0, s.Max())
	assert.Equal(t, 30.0, s.Sum())
	assert.Equal(t, uint64(10), s.Count())
}

func TestPo2Window_DataRace(t *testing.T) {
	t.Skip("skip data race test")
	s := NewPo2StatWindow(RequireBuckets(3), RequireBucketsMillis(2))
	stop := make(chan bool)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				s.Add(1)
				time.Sleep(time.Microsecond)
			}
		}
	}()
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = s.Reduce(func(p float64) float64 {
					return 0
				})
			}
		}
	}()
	time.Sleep(time.Second * 3)
	close(stop)
	w := s.(*po2stat).w
	//atLeast := uint64(len(w.buckets)-1) * w.bucketMillis
	//assert.GreaterOrEqual(t, s.Count(), atLeast)
	atMost := uint64(len(w.buckets)) * w.bucketMillis
	assert.LessOrEqual(t, s.Count(), atMost)
}

func BenchmarkPo2stat_Add(b *testing.B) {
	s := NewPo2StatWindow(RequireBuckets(4), RequireBucketsMillis(128))
	b.SetParallelism(runtime.NumCPU())
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Add(1)
		}
	})
}

func BenchmarkPo2stat_Reduce(b *testing.B) {
	s := NewPo2StatWindow(RequireBucketsMillis(2))
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				s.Add(1)
				time.Sleep(time.Millisecond)
			}
		}
	}()
	b.SetParallelism(runtime.NumCPU())
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = s.Reduce(func(f float64) float64 {
				return 0
			})
		}
	})
	close(stop)
}
