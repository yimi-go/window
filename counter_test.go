package window

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/yimi-go/iter"
)

func TestRollingCounter_Add(t *testing.T) {
	defer func() {
		NowFunc = time.Now
	}()
	now := time.UnixMilli(1659282941614)
	NowFunc = func() time.Time { return now }
	size := 3
	opts := RollingCounterOpts{
		WindowSize:          size,
		RequireBucketMillis: 1024,
	}
	r := NewRollingCounter(opts)
	listBuckets := func() [][]float64 {
		buckets := make([][]float64, 0)
		r.Reduce(func(i iter.Iterator[Bucket]) float64 {
			for {
				bucket, ok := i.Next()
				if !ok {
					break
				}
				buckets = append(buckets, bucket.Points())
			}
			return 0.0
		})
		return buckets
	}
	assert.Equal(t, [][]float64{{}, {}, {}}, listBuckets())
	r.Add(1)
	assert.Equal(t, [][]float64{{}, {}, {1}}, listBuckets())
	now = now.Add(1024 * time.Millisecond)
	r.Add(2)
	r.Add(3)
	assert.Equal(t, [][]float64{{}, {1}, {2, 3}}, listBuckets())
	now = now.Add(1024 * time.Millisecond)
	r.Add(4)
	r.Add(5)
	r.Add(6)
	assert.Equal(t, [][]float64{{1}, {2, 3}, {4, 5, 6}}, listBuckets())
	now = now.Add(1024 * time.Millisecond)
	r.Add(7)
	assert.Equal(t, [][]float64{{2, 3}, {4, 5, 6}, {7}}, listBuckets())
}

func TestRollingCounter_Reduce(t *testing.T) {
	defer func() {
		NowFunc = time.Now
	}()
	now := time.Now()
	NowFunc = func() time.Time { return now }
	size := 3
	bucketDuration := 1024 * time.Millisecond
	opts := RollingCounterOpts{
		WindowSize:          size,
		RequireBucketMillis: 1024,
	}
	r := NewRollingCounter(opts)
	for x := 0; x < size; x = x + 1 {
		for i := 0; i <= x; i++ {
			r.Add(float64(i + 1))
		}
		if x < size-1 {
			now = now.Add(bucketDuration)
		}
	}
	var result = r.Reduce(func(iterator iter.Iterator[Bucket]) float64 {
		var result float64
		for {
			bucket, ok := iterator.Next()
			if !ok {
				break
			}
			result += bucket.Points()[0]
		}
		return result
	})
	assert.Equal(t, 3.0, result)
	assert.Equal(t, int64(6), r.Count())
	assert.Equal(t, 10.0/6, r.Avg())
	assert.Equal(t, 1.0, r.Min())
	assert.Equal(t, 3.0, r.Max())
	assert.Equal(t, 10.0, r.Sum())
}

func TestRollingCounterDataRace(t *testing.T) {
	t.Skip("skip data race test")
	size := 3
	opts := RollingCounterOpts{
		WindowSize:          size,
		RequireBucketMillis: 10,
	}
	r := NewRollingCounter(opts)
	var stop = make(chan bool)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				r.Add(1)
				time.Sleep(time.Millisecond * 5)
			}
		}
	}()
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = r.Reduce(func(i iter.Iterator[Bucket]) float64 {
					for {
						bucket, ok := i.Next()
						if !ok {
							break
						}
						for range bucket.Points() {
							continue
						}
					}
					return 0
				})
			}
		}
	}()
	time.Sleep(time.Second * 3)
	close(stop)
}

func BenchmarkRollingCounterIncr(b *testing.B) {
	size := 3
	opts := RollingCounterOpts{
		WindowSize:          size,
		RequireBucketMillis: 100,
	}
	r := NewRollingCounter(opts)
	b.SetParallelism(runtime.NumCPU() * 100)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.Add(1)
		}
	})
}

func BenchmarkRollingCounterReduce(b *testing.B) {
	size := 3
	opts := RollingCounterOpts{
		WindowSize:          size,
		RequireBucketMillis: 1000,
	}
	r := NewRollingCounter(opts)
	stop := make(chan struct{})
	go func() {
		select {
		case <-stop:
			return
		default:
			r.Add(1)
			time.Sleep(time.Microsecond)
		}
	}()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = r.Reduce(func(i iter.Iterator[Bucket]) float64 {
				var result float64
				for {
					bucket, ok := i.Next()
					if !ok {
						break
					}
					if len(bucket.Points()) != 0 {
						result += bucket.Points()[0]
					}
				}
				return result
			})
		}
	})
}
