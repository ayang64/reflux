package metrics

import (
	"testing"

	"github.com/ayang64/reflux/pkg/testing/assert"
)

func TestID_newID(t *testing.T) {
	id := newID(0xff, 0xff0f0fff)
	assert.Equal(t, id, ID(0xff0f0fff000000ff))
	assert.Equal(t, id.id(), uint32(0xff))
	assert.Equal(t, id.gid(), uint32(0xff0f0fff))
}

func TestID_setGID(t *testing.T) {
	id := ID(1)
	assert.Equal(t, id.gid(), uint32(0))
	id.setGID(1)
	assert.Equal(t, id.gid(), uint32(1))
}
