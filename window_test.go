package window

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/yimi-go/iter"
)

func TestNewWindow(t *testing.T) {
	w := newWindow(10, 200)
	tk := newTrack(10, 200)
	want := &window{
		track: tk,
		size:  10,
	}
	assert.Equal(t, want, w)
}

func TestWindow_append(t *testing.T) {
	now := time.UnixMilli(int64(999)<<11 + 111)
	NowFunc = func() time.Time { return now }
	defer func() {
		NowFunc = time.Now
	}()
	w := newWindow(10, 200)
	w.append(1.2)
	bkt := w.track.buckets[0]
	assert.Equal(t, int64(999), bkt.round)
	assert.Equal(t, 1, len(bkt.points))
	assert.Equal(t, 1.2, bkt.points[0])
}

func TestWindow_Iterate(t *testing.T) {
	now := time.UnixMilli(int64(999)<<11 + 111)
	NowFunc = func() time.Time { return now }
	defer func() {
		NowFunc = time.Now
	}()
	w := newWindow(10, 200)
	w.append(1.2)
	it := w.Iterate().(*windowIter)
	assert.Equal(t, 16, len(it.buckets))
	assert.Equal(t, int64(15), it.bucketMask)
	assert.Equal(t, int64(999), it.round)
	assert.Equal(t, int64(0), it.offset)
	assert.Equal(t, int64(7), it.cur)
	assert.Equal(t, uint64(10), iter.Count[Bucket](it))
}

//func Test_impl_Iterator(t *testing.T) {
//	w := NewWindow(Options{Size: 3}).(*impl)
//	for offset := 0; offset < 6; offset++ {
//		for count := 0; count < 6; count++ {
//			got := w.Iterator(offset, count)
//			want := &iterator{
//				count:         count,
//				iteratedCount: 0,
//				cur:           w.buckets[offset%w.size],
//			}
//			if !reflect.DeepEqual(want, got) {
//				t.Errorf("want %v, got %v", want, got)
//			}
//		}
//	}
//}
