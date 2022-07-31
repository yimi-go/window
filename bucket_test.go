package window

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/yimi-go/iter"
)

func TestNewMatrixPo2Bucket(t *testing.T) {
	t.Run("not_po2", func(t *testing.T) {
		defer func() {
			assert.NotNil(t, recover(), "should panic when length is not power-of-two")
		}()
		newMatrixPo2Bucket(333, 0)
	})
}

func TestMatrixIter_shouldRound(t *testing.T) {
	b := newMatrixPo2Bucket(2, 1)
	assert.Equal(t, int64(0), b.shouldRound(1, 0))
	assert.Equal(t, int64(1), b.shouldRound(1, 1))
	assert.Equal(t, int64(1), b.shouldRound(1, 2))
}

func TestMatrixIter_resetIfRoundExpired(t *testing.T) {
	tests := []struct {
		round     int64
		offset    uint64
		nowRound  int64
		nowOffset uint64
		wantBnp   uint64
		wantRound int64
	}{
		{
			offset:    1,
			nowOffset: 1,
			round:     0,
			nowRound:  100,
			wantBnp:   0,
			wantRound: 100,
		},
		{
			offset:    1,
			nowOffset: 1,
			round:     99,
			nowRound:  100,
			wantBnp:   0,
			wantRound: 100,
		},
		{
			offset:    1,
			nowOffset: 1,
			round:     100,
			nowRound:  100,
			wantBnp:   1,
			wantRound: 100,
		},
		{
			offset:    1,
			nowOffset: 1,
			round:     101, // imposable
			nowRound:  100,
			wantBnp:   0,
			wantRound: 100,
		},
		{
			offset:    1,
			nowOffset: 0,
			round:     0,
			nowRound:  100,
			wantRound: 99,
			wantBnp:   0,
		},
		{
			offset:    1,
			nowOffset: 0,
			round:     99,
			nowRound:  100,
			wantRound: 99,
			wantBnp:   1,
		},
		{
			offset:    1,
			nowOffset: 0,
			round:     100, // imposable
			nowRound:  100,
			wantRound: 99,
			wantBnp:   0,
		},
		{
			offset:    1,
			nowOffset: 2,
			round:     0,
			nowRound:  100,
			wantRound: 100,
			wantBnp:   0,
		},
		{
			offset:    1,
			nowOffset: 2,
			round:     99,
			nowRound:  100,
			wantRound: 100,
			wantBnp:   0,
		},
		{
			offset:    1,
			nowOffset: 2,
			round:     100,
			nowRound:  100,
			wantRound: 100,
			wantBnp:   1,
		},
		{
			offset:    1,
			nowOffset: 2,
			round:     101, // imposable
			nowRound:  100,
			wantRound: 100,
			wantBnp:   0,
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			b := newMatrixPo2Bucket(2, tt.offset)
			b.round = tt.round
			b.len = 1
			b.resetIfRoundExpired(tt.nowRound, tt.nowOffset)
			assert.Equal(t, tt.wantBnp, b.len)
			assert.Equal(t, tt.wantRound, b.round)
		})
	}
	t.Run("dataRace", func(t *testing.T) {
		b := newMatrixPo2Bucket(2, 1)
		wg := &sync.WaitGroup{}
		for i := 0; i < runtime.NumCPU(); i++ {
			wg.Add(1)
			go func() {
				for i := uint64(0); i < 10000; i++ {
					atomic.StoreUint64(&b.len, 1)
					b.resetIfRoundExpired(int64(i+1), i%3)
				}
				wg.Done()
			}()
		}
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(time.Second * 20):
			t.Errorf("timeout")
		}
		assert.Equal(t, int64(9999), b.round)
		assert.Equal(t, uint64(1), b.len)
	})
}

func BenchmarkMatrixIter_resetIfRoundExpired(b *testing.B) {
	bu := newMatrixPo2Bucket(2, 1)
	i := uint64(1)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bu.len = 1
			bu.resetIfRoundExpired(int64(i), i%3)
		}
	})
}

func TestMatrixPo2Bucket_Points(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		b := newMatrixPo2Bucket(512, 1)
		b.round = 1
		assert.Equal(t, uint64(0), iter.Count(b.Points(1, 1)))
	})
	t.Run("points", func(t *testing.T) {
		b := newMatrixPo2Bucket(512, 1)
		b.round = 1
		b.matrix[0][0] = 1.0
		b.len = 1
		assert.Equal(t, uint64(1), iter.Count(b.Points(1, 1)))
	})
}

func TestMatrixPo2Bucket_Append(t *testing.T) {
	t.Run("base", func(t *testing.T) {
		b := newMatrixPo2Bucket(512, 1)
		b.round = 1
		b.Append(1, 1.2)
		assert.Equal(t, uint64(1), iter.Count(b.Points(1, 1)))
		v, ok := b.Points(1, 1).Next()
		assert.True(t, ok)
		assert.Equal(t, 1.2, v)
	})
	t.Run("dataRace", func(t *testing.T) {
		b := newMatrixPo2Bucket(512, 1)
		b.round = 1
		wg := &sync.WaitGroup{}
		for i := 0; i < runtime.NumCPU(); i++ {
			wg.Add(1)
			go func() {
				for i := 0; i < 10000; i++ {
					b.Append(1, 1.0)
				}
				wg.Done()
			}()
		}
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(time.Second * 10):
			t.Fatal("timeout")
		}
		assert.Equal(t, uint64(runtime.NumCPU())*10000, iter.Count(b.Points(1, 1)))
	})
}
