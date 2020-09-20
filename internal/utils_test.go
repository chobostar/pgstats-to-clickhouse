package internal

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetHash(t *testing.T) {
	var (
		s1, s2, s3 = "a", "b", "c"
		s4, s5, s6 = "a", "b", "c"
		s7, s8, s9 = "ab", "", "f"
	)
	assert.Equal(t, getHash(s1, s2, s3), getHash(s4, s5, s6), "Hash of the same strings should be equal")
	assert.NotEqual(t, getHash(s1, s2, s3), getHash(s7, s8, s9), "Hash of diff strings should not be equal")
}
