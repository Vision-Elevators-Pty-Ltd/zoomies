package scheduler

import (
	"testing"
	"time"

	"github.com/eyupio/zoomies/internal/store"
)

// GitHub's clock has been seen most of an hour ahead of ours, which writes a
// queued_at in the future. Such a job has waited for a time nobody can measure,
// not for none: held to the scale-up delay it would wait until the two clocks
// agreed, which is how a pool sat idle under a queue of twenty jobs.
func TestAJobQueuedInTheFutureIsNotHeldByTheScaleUpDelay(t *testing.T) {
	p := testPool("linux-x64", "linux")
	s := snap([]*store.Pool{p}, nil,
		[]*store.Job{queued("j1", -48*time.Minute, "linux")},
		[]*store.Host{testHost("host_a", 8, 0)})
	s.Policy.ScaleUpDelay = 30 * time.Second

	if n := countOf(only(t, Decide(s)).Actions, ActionCreate); n != 1 {
		t.Fatalf("a job queued 48 minutes in the future produced %d creates, want 1", n)
	}
}

func TestQueuedAgeReadsAFutureQueueTimeAsNow(t *testing.T) {
	future := &store.Job{QueuedAt: now.Add(48 * time.Minute)}
	if got := future.QueuedAge(now); got != 0 {
		t.Fatalf("age of a job queued in the future = %v, want 0", got)
	}
	if !future.QueuedInFuture(now) {
		t.Fatal("a queue time 48 minutes ahead was not read as in the future")
	}
	past := &store.Job{QueuedAt: now.Add(-time.Minute)}
	if got := past.QueuedAge(now); got != time.Minute || past.QueuedInFuture(now) {
		t.Fatalf("age of a job queued a minute ago = %v", got)
	}
}

// measuredHost is a 32 GB-allocatable host, measured, with free memory free.
func measuredHost(id string, freeMB int64) *store.Host {
	h := sized(id, 64, 64, hostFor(32*1024), 500*1024)
	return withUsage(h, 10, freeMB)
}

// At 100% the overcommit changes nothing: the same host takes the same runners
// with and without the setting.
func TestOvercommitAtOneHundredPercentChangesNothing(t *testing.T) {
	p := limited("big", 1, 8*1024)
	plain := newHostSet([]*store.Host{measuredHost("a", 64*1024)}, []*store.Pool{p}, nil, now).place(p, 12)
	at100 := newHostSetWith([]*store.Host{measuredHost("a", 64*1024)}, []*store.Pool{p}, nil, now, 100).place(p, 12)
	if len(plain) != 4 || len(at100) != 4 {
		t.Fatalf("placed %d plain and %d at 100%%, want 4 and 4", len(plain), len(at100))
	}
}

// At 200% a host takes twice the bookings, where the memory is measured free.
func TestOvercommitAtTwoHundredPercentBooksTwice(t *testing.T) {
	p := limited("big", 1, 8*1024)
	h := measuredHost("a", 200*1024)
	if got := newHostSetWith([]*store.Host{h}, []*store.Pool{p}, nil, now, 200).place(p, 12); len(got) != 8 {
		t.Fatalf("placed %d at 200%%, want 8: twice the 4 the host books once", len(got))
	}
	if got := BookingMemoryMB(h, 200); got != 2*h.Allocatable().MemoryMB {
		t.Fatalf("booking memory = %d, want twice %d", got, h.Allocatable().MemoryMB)
	}
}

// The booking is overcommitted; the machine is not. A start is still refused
// where the host's measured free memory, less its reserve, is short of the
// runner's size.
func TestOvercommitStillRefusesAStartWhenMeasuredFreeIsShort(t *testing.T) {
	p := limited("big", 1, 8*1024)
	h := measuredHost("a", 0)
	free := 8*1024 + h.MemoryReserve() - 1
	h.Usage.MemoryAvailableMB = &free
	if got := newHostSetWith([]*store.Host{h}, []*store.Pool{p}, nil, now, 200).place(p, 1); len(got) != 0 {
		t.Fatalf("placed %v on a host with less than 8 GB and its reserve measured free", got)
	}
	enough := 8*1024 + h.MemoryReserve()
	h.Usage.MemoryAvailableMB = &enough
	if got := newHostSetWith([]*store.Host{h}, []*store.Pool{p}, nil, now, 200).place(p, 2); len(got) != 1 {
		t.Fatalf("placed %d with one runner's size and the reserve free, want exactly 1", len(got))
	}
}

// Without a fresh measurement nothing bounds an overcommitted booking, so the
// host is booked once.
func TestAnUnmeasuredHostIsNotOvercommitted(t *testing.T) {
	p := limited("big", 1, 8*1024)
	h := sized("a", 64, 64, hostFor(32*1024), 500*1024)
	if got := newHostSetWith([]*store.Host{h}, []*store.Pool{p}, nil, now, 300).place(p, 12); len(got) != 4 {
		t.Fatalf("placed %d on an unmeasured host at 300%%, want the 4 it books once", len(got))
	}
}

// The policy reaches the pass.
func TestDecideBooksAtThePolicysOvercommit(t *testing.T) {
	p := limited("big", 1, 8*1024)
	p.Labels = store.NormalizeLabels([]string{"linux"})
	jobs := []*store.Job{}
	for _, id := range []string{"j1", "j2", "j3", "j4", "j5", "j6", "j7", "j8"} {
		jobs = append(jobs, queued(id, time.Minute, "linux"))
	}
	s := snap([]*store.Pool{p}, nil, jobs, []*store.Host{measuredHost("host_a", 200*1024)})
	if n := countOf(only(t, Decide(s)).Actions, ActionCreate); n != 4 {
		t.Fatalf("created %d at the default, want 4", n)
	}
	s = snap([]*store.Pool{p}, nil, jobs, []*store.Host{measuredHost("host_a", 200*1024)})
	s.Policy.MemoryOvercommitPercent = 200
	if n := countOf(only(t, Decide(s)).Actions, ActionCreate); n != 8 {
		t.Fatalf("created %d at 200%%, want 8", n)
	}
}
