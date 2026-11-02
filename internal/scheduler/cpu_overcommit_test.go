package scheduler

import (
	"math"
	"testing"
	"time"

	"github.com/eyupio/zoomies/internal/store"
)

// cpuHost is a measured 24-CPU host with far more memory than its CPUs can
// book, at cpu percent busy, so CPU is the only thing that runs out.
func cpuHost(id string, cpu float64) *store.Host {
	return withUsage(sized(id, 64, 24, 512*1024, 500*1024), cpu, 400*1024)
}

func cpuFits(h *store.Host, percent int, size float64) int {
	return int(math.Floor(h.Allocatable().CPUs*float64(percent)/100/size + 1e-9))
}

// At 100% the CPU overcommit changes nothing.
func TestCPUOvercommitAtOneHundredPercentChangesNothing(t *testing.T) {
	p := limited("light", 2, 1024)
	plain := newHostSet([]*store.Host{cpuHost("a", 20)}, []*store.Pool{p}, nil, now).place(p, 64)
	at100 := newHostSetOvercommitted([]*store.Host{cpuHost("a", 20)}, []*store.Pool{p}, nil, now, 100, 100).place(p, 64)
	if want := cpuFits(cpuHost("a", 20), 100, 2); len(plain) != want || len(at100) != want {
		t.Fatalf("placed %d plain and %d at 100%%, want %d each", len(plain), len(at100), want)
	}
}

// At 200% a host takes twice the CPU bookings, and the host view's figure says so.
func TestCPUOvercommitAtTwoHundredPercentBooksTwice(t *testing.T) {
	p := limited("light", 2, 1024)
	h := cpuHost("a", 20)
	got := newHostSetOvercommitted([]*store.Host{h}, []*store.Pool{p}, nil, now, 100, 200).place(p, 64)
	if want := cpuFits(h, 200, 2); len(got) != want || want != 2*cpuFits(h, 100, 2) {
		t.Fatalf("placed %d at 200%%, want %d, twice the %d booked once", len(got), want, cpuFits(h, 100, 2))
	}
	if b := BookingCPUs(h, 200); math.Abs(b-2*h.Allocatable().CPUs) > 1e-9 {
		t.Fatalf("booking CPUs = %v, want twice %v", b, h.Allocatable().CPUs)
	}
}

// The booking is overcommitted; the machine is not. A host whose real CPU is
// at 85% takes no new start, however much booking it has left.
func TestCPUOvercommitStillHoldsAStartAtEightyFivePercentRealCPU(t *testing.T) {
	p := limited("light", 2, 1024)
	p.Labels = store.NormalizeLabels([]string{"linux"})
	jobs := []*store.Job{queued("j1", time.Minute, "linux"), queued("j2", time.Minute, "linux")}

	busy := cpuHost("host_a", 85)
	s := snap([]*store.Pool{p}, nil, jobs, []*store.Host{busy})
	s.Policy.CPUOvercommitPercent = 400
	if n := countOf(only(t, Decide(s)).Actions, ActionCreate); n != 0 {
		t.Fatalf("created %d on a host at 85%% real CPU, want 0", n)
	}
	calm := cpuHost("host_a", 30)
	s = snap([]*store.Pool{p}, nil, jobs, []*store.Host{calm})
	s.Policy.CPUOvercommitPercent = 400
	if n := countOf(only(t, Decide(s)).Actions, ActionCreate); n != 2 {
		t.Fatalf("created %d on a calm host, want 2", n)
	}
}

// Without a fresh CPU sample nothing would notice a host was full, so it is
// booked once.
func TestAHostWithNoFreshCPUSampleIsNotCPUOvercommitted(t *testing.T) {
	p := limited("light", 2, 1024)
	h := sized("a", 64, 24, 512*1024, 500*1024)
	if got := newHostSetOvercommitted([]*store.Host{h}, []*store.Pool{p}, nil, now, 100, 400).place(p, 64); len(got) != cpuFits(h, 100, 2) {
		t.Fatalf("placed %d on an unmeasured host at 400%%, want the %d it books once", len(got), cpuFits(h, 100, 2))
	}
}
