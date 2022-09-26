package window

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_bucket_isDataOfWindowState(t *testing.T) {
	bucketDuration := time.Duration(1 << 30)
	tr := newTrack(20, bucketDuration+20*time.Millisecond)
	round, _ := tr.windowRoundAndPosition(time.Now())
	for i := 0; i < len(tr.buckets); i++ {
		b := &tr.buckets[i]
		b.data = append(b.data, int64(i))
		// 0-23 round
		// 24-28 round 0
		// 29-31 round - 1
		if i < 24 {
			b.round = round
		} else if i >= 29 {
			b.round = round - 1
		}
	}
	tests := []struct {
		name   string
		state  windowState
		bucket *bucket
		want   bool
	}{
		{
			name:   "round_match_at_win_right",
			state:  windowState{size: 20, round: round, position: 21},
			bucket: &tr.buckets[22],
			want:   false,
		},
		{
			name:   "round_match_at_win_left",
			state:  windowState{size: 20, round: round, position: 21},
			bucket: &tr.buckets[1],
			want:   false,
		},
		{
			name:   "round_match_left_window_edge",
			state:  windowState{size: 20, round: round, position: 21},
			bucket: &tr.buckets[2],
			want:   true,
		},
		{
			name:   "round_match_right_window_edge",
			state:  windowState{size: 20, round: round, position: 21},
			bucket: &tr.buckets[21],
			want:   true,
		},
		{
			name:   "round_match_in_window",
			state:  windowState{size: 20, round: round, position: 21},
			bucket: &tr.buckets[10],
			want:   true,
		},
		{
			name:   "window_in_round_bucket_not_in",
			state:  windowState{size: 20, round: round, position: 19},
			bucket: &tr.buckets[31],
			want:   false,
		},
		{
			name:   "too_old_round",
			state:  windowState{size: 20, round: round, position: 16},
			bucket: &tr.buckets[28],
			want:   false,
		},
		{
			name:   "last_round_at_window_left",
			state:  windowState{size: 20, round: round, position: 18},
			bucket: &tr.buckets[30],
			want:   false,
		},
		{
			name:   "last_round_in_window",
			state:  windowState{size: 20, round: round, position: 18},
			bucket: &tr.buckets[31],
			want:   true,
		},
		{
			name:   "data_override",
			state:  windowState{size: 20, round: round, position: 2},
			bucket: &tr.buckets[23],
			want:   false,
		},
		{
			name:   "data_too_old",
			state:  windowState{size: 20, round: round, position: 2},
			bucket: &tr.buckets[24],
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.bucket.isDataOfWindowState(tt.state),
				"isDataOfWindowState(%v), bucket %v", tt.state, tt.bucket)
		})
	}
}

func Test_bucket_append_and_readDataOfWindowState(t *testing.T) {
	bucketDuration := time.Duration(1 << 30)
	tr := newTrack(20, bucketDuration+20*time.Millisecond)
	round, position := tr.windowRoundAndPosition(time.Now())
	b := &tr.buckets[position]
	state := windowState{size: 20, round: round, position: position}
	assert.Empty(t, b.readDataOfWindowState(state))
	b.append(state, 100)
	assert.Equal(t, []int64{100}, b.readDataOfWindowState(state))
	b.append(state, 100)
	assert.Equal(t, []int64{100, 100}, b.readDataOfWindowState(state))
}
