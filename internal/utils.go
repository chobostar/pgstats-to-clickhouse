package internal

import (
	"fmt"
	"hash/crc32"
	"strings"
)

func getHash(in ...string) uint32 {
	var str strings.Builder
	h := crc32.NewIEEE()

	for _, s := range in {
		str.WriteString(s)
		str.WriteString(";")
	}

	if _, err := h.Write([]byte(str.String())); err != nil {
		panic(fmt.Errorf("unexpected error in hash gen: %w", err))
	}
	return h.Sum32()
}
