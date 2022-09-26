package window

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_countContinuouslyBits(t *testing.T) {
	type args struct {
		i uint64
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "1023",
			args: args{
				i: 1023,
			},
			want: 10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, countContinuouslyBits(tt.args.i), "countContinuouslyBits(%v)", tt.args.i)
		})
	}
}
