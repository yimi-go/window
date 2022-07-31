package window

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLastPo2(t *testing.T) {
	tests := []struct {
		ensure    uint64
		want      uint64
		wantPanic bool
	}{
		{
			ensure:    0,
			wantPanic: true,
		},
		{
			ensure: 1,
			want:   1,
		},
		{
			ensure: 2,
			want:   2,
		},
		{
			ensure: 3,
			want:   2,
		},
		{
			ensure: 9,
			want:   8,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.ensure), func(t *testing.T) {
			defer func() {
				assert.Equal(t, tt.wantPanic, recover() != nil)
			}()
			assert.Equal(t, tt.want, lastPo2(tt.ensure))
		})
	}
}

func TestNextPo2(t *testing.T) {
	tests := []struct {
		ensure uint64
		want   uint64
	}{
		{
			ensure: 0,
			want:   1,
		},
		{
			ensure: 1,
			want:   1,
		},
		{
			ensure: 2,
			want:   2,
		},
		{
			ensure: 3,
			want:   4,
		},
		{
			ensure: 9,
			want:   16,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.ensure), func(t *testing.T) {
			assert.Equal(t, tt.want, nextPo2(tt.ensure))
		})
	}
}
