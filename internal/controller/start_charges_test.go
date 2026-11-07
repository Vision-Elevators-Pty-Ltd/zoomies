package controller

import (
	"testing"
	"time"

	"github.com/eyupio/zoomies/internal/scheduler"
)

// A pass's start charges are what the passes since recorded, never nil, and a
// record is dropped once it is older than startChargeTTL: a runner still
// pending then is charged its whole size by the scheduler.
func TestStartChargesAreKeptUntilTheyExpire(t *testing.T) {
	c := &Controller{}
	at := time.Date(2026, 11, 7, 1, 0, 0, 0, time.UTC)
	if got := c.startChargesAt(at); got == nil || len(got) != 0 {
		t.Fatalf("with nothing recorded the charges are %v, want an empty, non-nil map", got)
	}
	c.recordStartCharge("run_a", scheduler.Reservation{MemoryMB: 16384, CPUs: 2.4}, at)
	c.recordStartCharge("", scheduler.Reservation{MemoryMB: 1}, at)
	got := c.startChargesAt(at.Add(time.Minute))
	if len(got) != 1 || got["run_a"].MemoryMB != 16384 || got["run_a"].CPUs != 2.4 {
		t.Fatalf("a minute later the charges are %v", got)
	}
	if got := c.startChargesAt(at.Add(startChargeTTL + time.Second)); len(got) != 0 {
		t.Fatalf("past the TTL the charges are %v, want none", got)
	}
	if len(c.startCharges) != 0 {
		t.Fatal("an expired record was kept in memory")
	}
}
