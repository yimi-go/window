package window

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewTrack(t *testing.T) {
	now := time.UnixMilli(int64(999)<<11 + 111)
	NowFunc = func() time.Time { return now }
	defer func() {
		NowFunc = time.Now
	}()
	tk := newTrack(10, 200)
	assert.Equal(t, int64(16), tk.size)
	assert.Equal(t, 16, len(tk.buckets))
	assert.Equal(t, int64(15), tk.bucketMask)
	assert.Equal(t, int64(128), tk.bucketMillis)
	assert.Equal(t, 7, tk.bucketMillisBits)
	assert.Equal(t, int64(2048), tk.cycleMillis)
	assert.Equal(t, 11, tk.cycleMillisBits)
	assert.Equal(t, int64(999)<<11, tk.initUnixMilli)
	for i := range tk.buckets {
		assert.NotNil(t, tk.buckets[i])
	}
}
