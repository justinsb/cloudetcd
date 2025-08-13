package memorylog

import (
	"testing"

	"justinsb.com/cloudetcd/pkg/persistence"
	"justinsb.com/cloudetcd/pkg/persistence/logtests"
)

func TestMemoryLog_All(t *testing.T) {
	logtests.RunAll(t, func(t *testing.T) persistence.Log {
		return New()
	})
}
