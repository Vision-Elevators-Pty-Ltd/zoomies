package scheduler

import (
	"strings"
	"testing"
	"time"

	"github.com/eyupio/zoomies/internal/store"
)

// The fleet finding 55 was measured on (4 Nov): ci-nl-01 is one 62 GB machine
// with two agents, ci-nl-01-pr and ci-nl-01-trusted, booked at 300%, and
// zoomies-pr-browser types every runner at 32 GB while its jobs peak at 5-27 GB.

const gb = int64(1024)

// nlAgent is one agent on a 62 GB machine with 49 GB measured free, labelled as
// being on machine.
func nlAgent(id, machine string, freeMB int64) *store.Host {
	h := withUsage(sized(id, 14, 24, 61888, 500*1024), 12, freeMB)
	h.ReserveMemoryMB = 6144
	if machine != "" {
		h.Labels = store.StringMap{"host": machine}
	}
	return h
}

func browserPool() *store.Pool {
	p := limited("zoomies-pr-browser", 8, 32*gb)
	p.MaxRunners = 32
	return p
}

// peaksOf is n measured runs of a job, each peaking at one of mbs in turn.
func peaksOf(n int, mbs ...int64) []store.JobPeak {
	out := make([]store.JobPeak, 0, n)
	for i := range n {
		out = append(out, store.JobPeak{MemoryMB: mbs[i%len(mbs)]})
	}
	return out
}

func key(job string, p *store.Pool) store.JobUsageKey {
	return store.JobUsageKey{Repo: "Vision-Elevators-Pty-Ltd/ops_app", Workflow: "PR", JobName: job, PoolID: p.ID}
}

// busyOn is a runner of p on h that has been running for a while, whose
// sample the host's last measurement already contains.
func busyOn(id string, p *store.Pool, h *store.Host) *store.Runner {
	r := testRunner(id, p, store.RunnerBusy, time.Hour)
	r.HostID = h.ID
	started := now.Add(-time.Hour)
	r.ContainerStartedAt = &started
	return r
}

// The figure a job's history books: the ninetieth percentile of its peaks and
// a fourth more, in 64 MB steps; nothing below five runs.
func TestHistoryBookingIsTheNinetiethPercentileAndTheHeadroom(t *testing.T) {
	if got, ok := historyBooking(peaksOf(20, 8*gb), 25); !ok || got != 10*gb {
		t.Fatalf("visual-shard at 8 GB books %d MB (known %v), want 10 GB", got, ok)
	}
	if _, ok := historyBooking(peaksOf(4, 8*gb), 25); ok {
		t.Fatal("four runs gave a booking; fewer than five must book the runner whole")
	}
	// A run the kernel killed counts at half as much again as the limit it
	// hit, whatever the percentile says.
	killed := append(peaksOf(19, 8*gb), store.JobPeak{MemoryMB: 9 * gb, OOMKilled: true, GrantedMemoryMB: 16 * gb})
	if got, _ := historyBooking(killed, 25); got != 30*gb {
		t.Fatalf("a job killed at 16 GB books %d MB, want 16 GB x 1.5 x 1.25 = 30 GB", got)
	}
}

// (a) A busy runner whose job is a visual shard (8 GB peaks) books ~10 GB of its
// 32 GB, and at the finding's figures the agent takes runners it refused before.
func TestARunnerIsBookedAtItsJobsHistoryNotItsSize(t *testing.T) {
	p := browserPool()
	h := nlAgent("ci-nl-01-pr", "", 49*gb)
	var runners []*store.Runner
	jobs := map[string]store.JobUsageKey{}
	// Five runners, 160 GB typed: what the agent had booked when 30 jobs waited.
	for i, job := range []string{"visual-shard (1/3)", "visual-shard (2/3)", "story-shots", "session-security", "draft-affected"} {
		r := busyOn("run_"+string(rune('a'+i)), p, h)
		runners = append(runners, r)
		jobs[r.ID] = key(job, p)
	}
	history := map[store.JobUsageKey][]store.JobPeak{
		key("visual-shard (1/3)", p): peaksOf(20, 7*gb, 8*gb),
		key("visual-shard (2/3)", p): peaksOf(20, 7*gb, 8*gb),
		key("story-shots", p):        peaksOf(20, 5*gb),
		key("session-security", p):   peaksOf(20, 13*gb),
		key("draft-affected", p):     peaksOf(20, 10*gb, 14*gb, 18*gb, 22*gb),
	}
	in := &BookingInputs{PoolRuns: map[string][]store.JobPeak{p.ID: peaksOf(100, 3*gb, 5*gb, 8*gb, 14*gb)}, RunnerJobs: jobs}
	byPool := map[string][]*store.Runner{p.ID: runners}

	b := newBooker(BookingHistory, 25, in, history, []*store.Pool{p}, nil)
	if got, _, basis := b.runner(p, h, runners[0]); got != 10*gb || basis != basisJob {
		t.Fatalf("a visual-shard runner books %d MB from %s, want 10 GB from its job", got, basis)
	}

	typed := newHostSetFrom([]*store.Host{h}, []*store.Pool{p}, byPool, now, ledger{overcommitPercent: 300, cpuOvercommitPercent: 400, startChargePercent: 30})
	hist := newHostSetFrom([]*store.Host{h}, []*store.Pool{p}, byPool, now, ledger{overcommitPercent: 300, cpuOvercommitPercent: 400, startChargePercent: 30, book: b})
	before, after := len(typed.place(p, 20)), len(hist.place(p, 20))
	if before != 0 {
		t.Fatalf("booked by size, the agent took %d more 32 GB runners; the finding had it refusing every one", before)
	}
	if after <= before {
		t.Fatalf("booked by history the agent took %d runners, no more than the %d it took booked by size", after, before)
	}
	// The measured check still bounds it: 49 GB free less the 6 GB reserve, at
	// a 30% start charge of 9.6 GB a start.
	if want := int((49*gb - h.MemoryReserve()) / startCharge(32*gb, 0, 30)); after != want {
		t.Fatalf("booked by history the agent took %d runners, want the %d its measured free memory admits", after, want)
	}
}

// (b) A runner booked at its pool's figure before GitHub hands it a job is
// re-booked at that job's own history once it is known: a draft-affected run
// books near 28 GB, not the 10 GB its pool's light jobs would say.
func TestARunnerIsRebookedAtItsJobOnceTheJobIsKnown(t *testing.T) {
	p := browserPool()
	h := nlAgent("ci-nl-01-pr", "", 49*gb)
	r := busyOn("run_draft", p, h)
	history := map[store.JobUsageKey][]store.JobPeak{key("draft-affected", p): peaksOf(20, 14*gb, 18*gb, 20*gb, 23*gb)}
	in := &BookingInputs{PoolRuns: map[string][]store.JobPeak{p.ID: peaksOf(100, 3*gb, 5*gb, 6*gb, 7*gb)}}

	b := newBooker(BookingHistory, 25, in, history, []*store.Pool{p}, nil)
	unknown, _, basis := b.runner(p, h, r)
	if basis != basisPool || unknown != 8960 {
		t.Fatalf("before its job is known the runner books %d MB from %s, want its pool's 7 GB x 1.25 = 8.75 GB", unknown, basis)
	}
	in.RunnerJobs = map[string]store.JobUsageKey{r.ID: key("draft-affected", p)}
	b = newBooker(BookingHistory, 25, in, history, []*store.Pool{p}, nil)
	known, _, basis := b.runner(p, h, r)
	if basis != basisJob || known < 27*gb || known > 30*gb {
		t.Fatalf("running draft-affected the runner books %d MB from %s, want 27-30 GB from its job", known, basis)
	}
}

// A runner whose job is not yet known may take the heaviest job waiting, so it
// is booked for that, not for its pool's lighter average.
func TestARunnerWithoutAJobIsBookedForTheHeaviestJobWaiting(t *testing.T) {
	p := browserPool()
	history := map[store.JobUsageKey][]store.JobPeak{key("draft-affected", p): peaksOf(20, 22*gb)}
	in := &BookingInputs{PoolRuns: map[string][]store.JobPeak{p.ID: peaksOf(100, 4*gb)}}
	waiting := &store.Job{Repo: "Vision-Elevators-Pty-Ltd/ops_app", Workflow: "PR", JobName: "draft-affected", State: store.JobQueued}
	quiet := newBooker(BookingHistory, 25, in, history, []*store.Pool{p}, nil)
	busy := newBooker(BookingHistory, 25, in, history, []*store.Pool{p}, map[string][]*store.Job{p.ID: {waiting}})
	if got := quiet.start(p, 32*gb, nil); got != 5*gb {
		t.Fatalf("with nothing heavy waiting a start books %d MB, want the pool's 4 GB x 1.25 = 5 GB", got)
	}
	if got := busy.start(p, 32*gb, nil); got != 28160 {
		t.Fatalf("with draft-affected waiting a start books %d MB, want its 22 GB x 1.25 = 27.5 GB", got)
	}
}

// A runner measured using more than its history says is booked at least that,
// and one the valve has lent memory to is booked whole.
func TestARunnerIsNeverBookedBelowWhatItUsesOrWhenItWasLentMemory(t *testing.T) {
	p := browserPool()
	h := nlAgent("ci-nl-01-pr", "", 49*gb)
	r := busyOn("run_1", p, h)
	in := &BookingInputs{PoolRuns: map[string][]store.JobPeak{p.ID: peaksOf(100, 4*gb)}}
	b := newBooker(BookingHistory, 25, in, nil, []*store.Pool{p}, nil)
	r.MemoryBytes = 12 * gb << 20
	if got, _, _ := b.runner(p, h, r); got != 12*gb {
		t.Fatalf("a runner using 12 GB books %d MB", got)
	}
	r.MemoryBytes, r.LentMemoryMB = 0, 2*gb
	if got, charge, basis := b.runner(p, h, r); got != charge || basis != basisTyped {
		t.Fatalf("a runner lent memory books %d MB of %d (%s), want all of it", got, charge, basis)
	}
}

// (c) Two agents on one machine share one ceiling: the machine's booking
// memory once, against what both have booked.
func TestTwoAgentsOnOneMachineShareOneCeiling(t *testing.T) {
	p := browserPool()
	trusted := limited("zoomies-trusted", 8, 24*gb)
	fleet := func(machine string) (*hostSet, *store.Host, *store.Host) {
		pr := nlAgent("ci-nl-01-pr", machine, 49*gb)
		tr := nlAgent("ci-nl-01-trusted", machine, 49*gb)
		runners := map[string][]*store.Runner{}
		for i := range 4 { // 128 GB on the pull-request agent
			runners[p.ID] = append(runners[p.ID], busyOn("pr_"+string(rune('a'+i)), p, pr))
		}
		runners[trusted.ID] = []*store.Runner{busyOn("tr_a", trusted, tr)} // 24 GB on the trusted one
		hs := newHostSetFrom([]*store.Host{pr, tr}, []*store.Pool{p, trusted}, runners, now,
			ledger{overcommitPercent: 300, cpuOvercommitPercent: 400, startChargePercent: 30, machineLabel: "host"})
		return hs, pr, tr
	}
	var shared *hostSet
	alone, pr, tr := fleet("")
	ceiling := BookingMemoryMB(pr, 300)
	if alone.bookedMemory[pr.ID] != ceiling-128*gb || alone.bookedMemory[tr.ID] != ceiling-24*gb {
		t.Fatalf("unlabelled, each agent should have its own ceiling: %d and %d left", alone.bookedMemory[pr.ID], alone.bookedMemory[tr.ID])
	}
	shared, pr, tr = fleet("ci-nl-01")
	if want := ceiling - 152*gb; shared.bookedMemory[pr.ID] != want || shared.bookedMemory[tr.ID] != want {
		t.Fatalf("on one machine both agents should have %d MB left, the machine's; got %d and %d", want, shared.bookedMemory[pr.ID], shared.bookedMemory[tr.ID])
	}
	// The machine may be booked for 3 x 54.4 GB = 163.3 GB and has 152 GB
	// booked: neither agent has room for a 32 GB or a 24 GB runner, though
	// each, booked alone, had room for both.
	for _, pool := range []*store.Pool{p, trusted} {
		shared, _, _ = fleet("ci-nl-01")
		if got := shared.place(pool, 1); len(got) != 0 {
			t.Fatalf("the shared machine took a %s runner on %v", pool.Name, got)
		}
		alone, _, _ = fleet("")
		if got := alone.place(pool, 1); len(got) != 1 {
			t.Fatalf("booked alone, an agent should have had room for a %s runner; placed %v", pool.Name, got)
		}
	}
}

// The measurement is the machine's too: what starts on one agent is taken from
// what the other may start against.
func TestAgentsOnOneMachineShareTheMeasuredFreeMemory(t *testing.T) {
	p := browserPool()
	pr := nlAgent("ci-nl-01-pr", "ci-nl-01", 49*gb)
	tr := nlAgent("ci-nl-01-trusted", "ci-nl-01", 49*gb)
	tr.Labels["pool"] = "trusted"
	hs := newHostSetFrom([]*store.Host{pr, tr}, []*store.Pool{p}, nil, now,
		ledger{overcommitPercent: 1000, startChargePercent: 30, machineLabel: "host"})
	want := int((49*gb - pr.MemoryReserve()) / startCharge(32*gb, 0, 30))
	if got := len(hs.place(p, 20)); got != want {
		t.Fatalf("two agents on one machine with 49 GB free took %d starts, want the %d the machine's measurement admits", got, want)
	}
	if hs.measuredMemory[pr.ID] != hs.measuredMemory[tr.ID] {
		t.Fatalf("the agents disagree about the machine's free memory: %d and %d", hs.measuredMemory[pr.ID], hs.measuredMemory[tr.ID])
	}
}

// (d) With no history, history booking books every runner whole, and the pass
// places exactly as typed booking does.
func TestWithoutHistoryEveryRunnerIsBookedAtItsSize(t *testing.T) {
	p := browserPool()
	h := nlAgent("ci-nl-01-pr", "", 49*gb)
	runners := map[string][]*store.Runner{p.ID: {busyOn("run_a", p, h), busyOn("run_b", p, h)}}
	b := newBooker(BookingHistory, 25, &BookingInputs{}, nil, []*store.Pool{p}, nil)
	for _, r := range runners[p.ID] {
		if got, charge, basis := b.runner(p, h, r); got != charge || basis != basisTyped {
			t.Fatalf("with no history a runner books %d of %d MB (%s)", got, charge, basis)
		}
	}
	typed := newHostSetFrom([]*store.Host{h}, []*store.Pool{p}, runners, now, ledger{overcommitPercent: 300, startChargePercent: 30})
	hist := newHostSetFrom([]*store.Host{h}, []*store.Pool{p}, runners, now, ledger{overcommitPercent: 300, startChargePercent: 30, book: b})
	if typed.bookedMemory[h.ID] != hist.bookedMemory[h.ID] {
		t.Fatalf("booked left %d MB by size and %d by an empty history", typed.bookedMemory[h.ID], hist.bookedMemory[h.ID])
	}
	if a, b := len(typed.place(p, 20)), len(hist.place(p, 20)); a != b {
		t.Fatalf("placed %d by size and %d by an empty history", a, b)
	}
	// And history booking is off below 100%: the ledger is the machine once.
	once := newHostSetFrom([]*store.Host{h}, []*store.Pool{p}, runners, now, ledger{overcommitPercent: 100, startChargePercent: 30, book: b})
	plain := newHostSetCharged([]*store.Host{h}, []*store.Pool{p}, runners, now, 100, 100, 30)
	if once.bookedMemory[h.ID] != plain.bookedMemory[h.ID] || once.byHistory != nil || once.bookings != nil {
		t.Fatal("at 100% memory overcommit history booking changed the ledger")
	}
}

// (e) However little the history books, nothing starts where the measured free
// memory is less than the reserve and one start charge.
func TestHistoryBookingKeepsTheMeasuredCheck(t *testing.T) {
	p := browserPool()
	h := nlAgent("ci-nl-01-pr", "", 0)
	short := h.MemoryReserve() + startCharge(32*gb, 0, 30) - 1
	h.Usage.MemoryAvailableMB = &short
	in := &BookingInputs{PoolRuns: map[string][]store.JobPeak{p.ID: peaksOf(100, 1*gb)}}
	b := newBooker(BookingHistory, 25, in, nil, []*store.Pool{p}, nil)
	hs := newHostSetFrom([]*store.Host{h}, []*store.Pool{p}, nil, now, ledger{overcommitPercent: 300, startChargePercent: 30, book: b})
	if got := hs.place(p, 20); len(got) != 0 {
		t.Fatalf("placed %d with less than the reserve and one start charge measured free", len(got))
	}
	if d := hs.shortOfMemory(h, p); !strings.Contains(d, "measured free") {
		t.Fatalf("short of memory says %q, want the measurement named", d)
	}
	enough := short + 1
	h.Usage.MemoryAvailableMB = &enough
	hs = newHostSetFrom([]*store.Host{h}, []*store.Pool{p}, nil, now, ledger{overcommitPercent: 300, startChargePercent: 30, book: b})
	if got := hs.place(p, 20); len(got) != 1 {
		t.Fatalf("placed %d with exactly the reserve and one charge free, want 1", len(got))
	}
}

// The reason for a create says how the runner was booked, and a pool held back
// by the booking says how much of the machine is booked and how.
func TestTheReasonSaysHowTheRunnerWasBooked(t *testing.T) {
	p := browserPool()
	p.Labels = store.StringSlice{"self-hosted", "zoomies-pr-browser"}
	pr := nlAgent("ci-nl-01-pr", "ci-nl-01", 49*gb)
	tr := nlAgent("ci-nl-01-trusted", "ci-nl-01", 49*gb)
	tr.Labels["pool"] = "trusted"
	p.HostSelector = store.StringMap{}
	j := queued("visual-shard (1/3)", time.Minute, "self-hosted", "zoomies-pr-browser")
	j.Repo, j.Workflow = "Vision-Elevators-Pty-Ltd/ops_app", "PR"
	s := snap([]*store.Pool{p}, nil, []*store.Job{j}, []*store.Host{pr, tr})
	s.Policy.MemoryOvercommitPercent, s.Policy.StartChargePercent = 300, 30
	s.Policy.MemoryBooking, s.Policy.MemoryBookingHeadroomPercent, s.Policy.MachineLabel = BookingHistory, 25, "host"
	s.JobHistory = map[store.JobUsageKey][]store.JobPeak{key("visual-shard (1/3)", p): peaksOf(20, 8*gb)}
	s.Booking = &BookingInputs{PoolRuns: map[string][]store.JobPeak{p.ID: peaksOf(100, 6*gb)}}
	plan := Decide(s)
	if len(plan.Actions) == 0 {
		t.Fatalf("nothing was created: %+v", plan.Pools)
	}
	if r := plan.Actions[0].Reason; !strings.Contains(r, "booked 10 GB of its 32 GB by job history") || !strings.Contains(r, "on machine ci-nl-01") {
		t.Fatalf("the reason does not say how the runner was booked: %q", r)
	}
	hb := plan.Bookings[pr.ID]
	if hb == nil || hb.Mode != BookingHistory || hb.Machine != "ci-nl-01" || len(hb.MachineHosts) != 2 || !hb.Overcommitted {
		t.Fatalf("the plan's booking for the agent is %+v", hb)
	}
}

// A history the store switches to working-set peaks has enough runs for a
// booking to be taken from.
func TestTheStoreSwitchesHistoryAtTheBookingsMinimum(t *testing.T) {
	if store.WorkingSetMinRuns != BookingMinSamples {
		t.Fatalf("store.WorkingSetMinRuns = %d, BookingMinSamples = %d; they must agree", store.WorkingSetMinRuns, BookingMinSamples)
	}
}
