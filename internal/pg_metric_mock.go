package internal

import "testing"

type SomePgMetric struct{}

func (s *SomePgMetric) isSkippable(old PgMetric) bool {
	return true
}

func (s *SomePgMetric) delta(old PgMetric) PgMetric {
	return &SomePgMetric{}
}

func (s *SomePgMetric) getHash() uint32 {
	return 0
}

func (s *SomePgMetric) getValue(hostname string) []interface{} {
	return []interface{}{1, 2}
}

func assertPanic(t *testing.T, f func(), errorText string) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf(errorText)
		}
	}()
	f()
}
