package window

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShouldRound(t *testing.T) {
	assert.Equal(t, int64(0), shouldRound(1, 0, 1))
	assert.Equal(t, int64(1), shouldRound(1, 1, 1))
	assert.Equal(t, int64(1), shouldRound(1, 2, 1))
}
