package scheduler

import (
	"testing"

	"github.com/eyupio/zoomies/internal/store"
)

// A 60 GB host with 40 GB measured free and memory booked far past the
// machine, so the measured check is the only thing that binds.
func startHost(freeMB int64) *store.Host {
	return withUsage(sized("a", 64, 64, 60*1024, 500*1024), 10, freeMB)
}

func startsAt(h *store.Host, percent int, need *Requirement) int {
	p := limited("trusted", 1, 24*1024)
	hs := newHostSetCharged([]*store.Host{h}, []*store.Pool{p}, nil, now, 1000, 100, percent)
	hs.need = need
	return len(hs.place(p, 20))
}

// At 100% a start is charged its whole size, as it always was: 40 GB free
// takes one 24 GB runner.
func TestAStartChargeOfOneHundredPercentChangesNothing(t *testing.T) {
	p := limited("trusted", 1, 24*1024)
	before := len(newHostSetOvercommitted([]*store.Host{startHost(40 * 1024)}, []*store.Pool{p}, nil, now, 1000, 100).place(p, 20))
	if got := startsAt(startHost(40*1024), 100, nil); got != before || got != 1 {
		t.Fatalf("placed %d at 100%% and %d before, want 1 each", got, before)
	}
}

// At 30% the same host takes more starts of the same runner, because each is
// held against the measurement at 30% of its size.
func TestAThirtyPercentStartChargeAdmitsMoreStarts(t *testing.T) {
	h := startHost(40 * 1024)
	charge := startCharge(24*1024, 0, 30)
	want := int((40*1024 - h.MemoryReserve()) / charge)
	if got := startsAt(h, 30, nil); got != want || got <= 1 {
		t.Fatalf("placed %d at 30%%, want %d", got, want)
	}
}

// What a job is known to need is the floor under the charge.
func TestAStartIsChargedAtLeastWhatItsJobIsKnownToNeed(t *testing.T) {
	h := startHost(40 * 1024)
	need := &Requirement{MemoryMB: 12 * 1024, Samples: 5}
	want := int((40*1024 - h.MemoryReserve()) / (12 * 1024))
	if got := startsAt(h, 30, need); got != want {
		t.Fatalf("placed %d with a 12 GB requirement, want %d", got, want)
	}
	if c := startCharge(24*1024, 30*1024, 30); c != 24*1024 {
		t.Fatalf("charge = %d, want it capped at the size", c)
	}
}

// The reserve still stands: nothing starts where measured free is less than
// the reserve and the charge.
func TestNoStartWhereMeasuredFreeIsBelowTheReserveAndTheCharge(t *testing.T) {
	h := startHost(0)
	short := h.MemoryReserve() + startCharge(24*1024, 0, 30) - 1
	h.Usage.MemoryAvailableMB = &short
	if got := startsAt(h, 30, nil); got != 0 {
		t.Fatalf("placed %d with less than the reserve and one charge free", got)
	}
	enough := short + 1
	h.Usage.MemoryAvailableMB = &enough
	if got := startsAt(h, 30, nil); got != 1 {
		t.Fatalf("placed %d with exactly the reserve and one charge free, want 1", got)
	}
}
