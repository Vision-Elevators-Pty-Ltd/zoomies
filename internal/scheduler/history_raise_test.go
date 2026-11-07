package scheduler

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/eyupio/zoomies/internal/store"
)

// The fleet finding 70 was measured on (5 Nov): ops_app's typecheck runs on
// zoomies-trusted, a docker-in-docker pool typed at 8 CPU / 24 GB and held to
// that between the pair, and a full run now needs 20-24 GB; two runs were
// killed at exactly 24 GB on the day.

// trustedPool is zoomies-trusted: a pair held to its typed 24 GB between them.
func trustedPool() *store.Pool {
	p := limited("zoomies-trusted", 8, 24*gb)
	p.DockerMode = store.DockerDinD
	p.Resources.DinDSharedLimit = true
	p.MaxRunners = 16
	return p
}

// nodockerPool is a pool with no daemon beside the runner, typed at 24 GB.
func nodockerPool() *store.Pool {
	p := limited("zoomies-trusted-nodocker", 8, 24*gb)
	p.MaxRunners = 16
	return p
}

// pairPool is a docker-in-docker pool that gives each container its typed 24 GB.
func pairPool() *store.Pool {
	p := limited("zoomies-ops-sa", 8, 24*gb)
	p.DockerMode = store.DockerDinD
	return p
}

// trustedAgent is a 62 GB agent that holds pairs to one limit, with freeMB
// measured free.
func trustedAgent(id string, freeMB int64) *store.Host {
	h := nlAgent(id, "", freeMB)
	h.Features = store.StringSlice{store.FeatureDinDSharedLimit, ElasticMemoryFeature}
	return h
}

// typecheckOn is a live runner of p on h running typecheck, created at the
// pool's typed size.
func typecheckOn(id string, p *store.Pool, h *store.Host) *store.Runner {
	r := busyOn(id, p, h)
	r.AllocatedMemoryMB, r.AllocationSource = p.Resources.MemoryMB, store.AllocationFromPool
	return r
}

// typecheckRuns is the day's history: twenty runs, the ninetieth percentile at
// 23.5 GB, which books 23.5 x 1.25 = 29.4 GB.
func typecheckRuns() []store.JobPeak { return peaksOf(20, 9*gb, 16*gb, 23*gb+512) }

const typecheckNeed = int64(30080) // 23.5 GB x 1.25 in 64 MB steps

func TestTheTypecheckHistoryNeedsMoreThanItsTypedSize(t *testing.T) {
	if got, ok := historyBooking(typecheckRuns(), 25); !ok || got != typecheckNeed {
		t.Fatalf("typecheck books %d MB (known %v), want %d", got, ok, typecheckNeed)
	}
}

// A runner whose job needs more than it was created with may be lent up to that
// need. A pair that gives each container the typed size is lent the difference
// on top of both; a pair held to one limit is not lent anything, because its
// slice caps the pair however far the valve raises a container.
func TestAHistoryGrantIsTheJobsNeedAboveWhatTheRunnerWasGiven(t *testing.T) {
	h := trustedAgent("ci-nl-01-trusted", 40*gb)
	for _, tc := range []struct {
		name        string
		pool        *store.Pool
		need        int64
		wantOK      bool
		wantCeiling int64
	}{
		{"a runner with no daemon grows to the need", nodockerPool(), typecheckNeed, true, typecheckNeed},
		// 48 GB and the 5.4 GB difference is past the 62 GB machine above its
		// floor, so the pair is held to that.
		{"a pair of whole limits is lent the difference over both, to the machine above its floor", pairPool(), typecheckNeed, true, min(48*gb+typecheckNeed-24*gb, h.MemoryMB-MemoryPoolFloor(h))},
		{"a pair held to one limit is not lent what its slice would refuse", trustedPool(), typecheckNeed, false, 0},
		{"a job that fits is given nothing", nodockerPool(), 20 * gb, false, 0},
		{"no need is no grant", nodockerPool(), 0, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := typecheckOn("run_tc", tc.pool, h)
			g, ok := HistoryGrantFor(tc.pool, h, r, tc.need)
			if ok != tc.wantOK || g.CeilingMB != tc.wantCeiling {
				t.Fatalf("grant = %+v (%v), want ceiling %d (%v)", g, ok, tc.wantCeiling, tc.wantOK)
			}
			if ok && !strings.Contains(g.Reason(), "raised for job history (need 30080 MB)") {
				t.Fatalf("the grant says %q, which does not say why", g.Reason())
			}
		})
	}
}

// A history that says more than twice the typed size is more likely an outlier
// than a size: the raise stops at twice.
func TestAHistoryRaiseNeverPassesTwiceTheTypedSize(t *testing.T) {
	p := nodockerPool()
	h := sized("big", 16, 64, 512*gb, 500*gb)
	g, ok := HistoryGrantFor(p, h, typecheckOn("run_tc", p, h), 80*gb)
	if !ok || g.TargetMB != 48*gb || g.CeilingMB != 48*gb {
		t.Fatalf("a job said to need 80 GB is granted %+v (%v), want twice the typed 24 GB", g, ok)
	}
}

// Nor past what the host can hold: its memory less the floor the valve keeps
// free, and the ceiling the host's owner put on one runner.
func TestAHistoryRaiseNeverPassesWhatTheHostCanHold(t *testing.T) {
	p := nodockerPool()
	small := sized("small", 2, 8, 32*gb, 500*gb)
	if got, want := HistoryRaiseCap(p, small), small.MemoryMB-MemoryPoolFloor(small); got != want || got >= 32*gb {
		t.Fatalf("a 32 GB host caps a raise at %d MB, want its memory less the floor, %d", got, want)
	}
	g, ok := HistoryGrantFor(p, small, typecheckOn("run_tc", p, small), 40*gb)
	if !ok || g.TargetMB != HistoryRaiseCap(p, small) {
		t.Fatalf("on a 32 GB host the grant is %+v (%v), want its cap", g, ok)
	}
	owned := sized("owned", 2, 8, 128*gb, 500*gb)
	owned.RunnerProfile.Standard.BurstMaxMemoryMB = 28 * gb
	if got := HistoryRaiseCap(p, owned); got != 28*gb {
		t.Fatalf("a host whose owner caps a runner at 28 GB caps a raise at %d MB", got)
	}
	tiny := sized("tiny", 2, 8, 24*gb, 500*gb)
	if _, ok := HistoryGrantFor(p, tiny, typecheckOn("run_tc", p, tiny), 40*gb); ok {
		t.Fatal("a host that cannot hold more than the typed size gave a grant")
	}
}

// historyRaiseFleet is one trusted agent with room, a pool and the typecheck
// job waiting on it with its history.
func historyRaiseFleet(p *store.Pool, h *store.Host, raise string) Snapshot {
	job := queued("typecheck", 0, p.Labels...)
	job.Repo, job.Workflow, job.JobName = "Vision-Elevators-Pty-Ltd/ops_app", "PR", "typecheck"
	s := snap([]*store.Pool{p}, nil, []*store.Job{job}, []*store.Host{h})
	s.Policy.HistoryRaise = raise
	s.Policy.MemoryBookingHeadroomPercent = 25
	s.JobHistory = map[store.JobUsageKey][]store.JobPeak{key("typecheck", p): typecheckRuns()}
	return s
}

// The finding itself: with typecheck waiting, the next runner of the shared
// pair pool is created at the job's need on a host with room, which is the one
// way such a pair is given more, because its slice is set when it is created.
func TestARunnerIsCreatedAtTheWaitingJobsNeedAboveItsTypedSize(t *testing.T) {
	for _, p := range []*store.Pool{trustedPool(), nodockerPool()} {
		t.Run(p.Name, func(t *testing.T) {
			creates := actionsOf(only(t, Decide(historyRaiseFleet(p, trustedAgent("ci-nl-01-trusted", 49*gb), HistoryRaiseOn))).Actions, ActionCreate)
			if len(creates) != 1 {
				t.Fatalf("creates = %d, want 1", len(creates))
			}
			a := creates[0]
			if a.Size == nil || a.Size.MemoryMB != typecheckNeed || a.Size.CPUs != 8 || !a.SizedFromHistory {
				t.Fatalf("the runner was created at %+v (from history %v), want 8 CPU and %d MB", a.Size, a.SizedFromHistory, typecheckNeed)
			}
			if !strings.Contains(a.Reason, "raised to 8 CPU and 29.4 GB for job history") {
				t.Fatalf("the reason %q does not say the runner was raised for its job", a.Reason)
			}
		})
	}
}

// A pair that gives each container the typed size whole is not created larger:
// both containers would be given the raise and the host charged twice it. It is
// raised by the valve once its job is known instead.
func TestAPairOfWholeLimitsIsNotCreatedLarger(t *testing.T) {
	p := pairPool()
	a := actionsOf(only(t, Decide(historyRaiseFleet(p, trustedAgent("ci-nl-01-trusted", 49*gb), HistoryRaiseOn))).Actions, ActionCreate)
	if len(a) != 1 || a[0].Size != nil {
		t.Fatalf("creates = %+v, want one at the pool's own size", a)
	}
}

// Off is the plan from before the setting existed, action for action.
func TestHistoryRaiseOffChangesNoAction(t *testing.T) {
	p := trustedPool()
	on := Decide(historyRaiseFleet(p, trustedAgent("ci-nl-01-trusted", 49*gb), HistoryRaiseOn))
	off := Decide(historyRaiseFleet(p, trustedAgent("ci-nl-01-trusted", 49*gb), HistoryRaiseOff))
	unset := Decide(historyRaiseFleet(p, trustedAgent("ci-nl-01-trusted", 49*gb), ""))
	if !reflect.DeepEqual(off.Actions, unset.Actions) {
		t.Fatalf("off and unset differ:\noff   %+v\nunset %+v", off.Actions, unset.Actions)
	}
	if reflect.DeepEqual(on.Actions, off.Actions) {
		t.Fatal("on and off produced the same plan, so the fleet is not what this test thinks")
	}
	if a := actionsOf(off.Actions, ActionCreate); len(a) != 1 || a[0].Size != nil || a[0].SizedFromHistory {
		t.Fatalf("with history_raise off the runner was created at %+v, want its typed size", a)
	}
	if off.RunnerNeeds != nil {
		t.Fatalf("with history_raise off the plan carries runner needs %v", off.RunnerNeeds)
	}
}

// Where no host has room for the raised size the runner is placed at its typed
// size as before, and the reason says it was not raised. Holding every job on
// the pool for one heavy one is the deadlock of 1 Nov.
func TestARaiseNoHostHasRoomForFallsBackToTheTypedSize(t *testing.T) {
	p := nodockerPool()
	// 28 GB measured free has room for a 24 GB start charged whole, and none
	// for 29.4 GB.
	s := historyRaiseFleet(p, trustedAgent("ci-nl-01-trusted", 28*gb), HistoryRaiseOn)
	a := actionsOf(only(t, Decide(s)).Actions, ActionCreate)
	if len(a) != 1 {
		t.Fatalf("creates = %d, want 1: a raise that does not fit must not hold the pool", len(a))
	}
	if a[0].Size != nil || a[0].SizedFromHistory {
		t.Fatalf("the runner was created at %+v, want the typed size", a[0].Size)
	}
	if !strings.Contains(a[0].Reason, "not raised for job history") {
		t.Fatalf("the reason %q does not say the raise did not fit", a[0].Reason)
	}
}

// Fewer than five runs say nothing, and nothing is raised.
func TestAJobWithAShortHistoryIsNotRaised(t *testing.T) {
	p := nodockerPool()
	s := historyRaiseFleet(p, trustedAgent("ci-nl-01-trusted", 49*gb), HistoryRaiseOn)
	s.JobHistory[key("typecheck", p)] = peaksOf(4, 30*gb)
	if a := actionsOf(only(t, Decide(s)).Actions, ActionCreate); len(a) != 1 || a[0].Size != nil {
		t.Fatalf("a job with four runs raised its runner: %+v", a)
	}
}

// With scheduler.history_sizing=on a typed pool's runner was already placed
// only on a host with room for the need; history_raise now also creates it at
// the need, where before it was created at the typed size it was known to
// outgrow.
func TestHistorySizingOnRaisesATypedRunnerOnAHostWithRoomForIt(t *testing.T) {
	p := nodockerPool()
	small := trustedAgent("a-small", 26*gb)
	big := trustedAgent("b-big", 49*gb)
	for _, raise := range []string{HistoryRaiseOff, HistoryRaiseOn} {
		s := historyRaiseFleet(p, small, raise)
		s.Hosts = []*store.Host{small, big}
		s.HistorySizing = HistoryOn
		a := actionsOf(only(t, Decide(s)).Actions, ActionCreate)
		if len(a) != 1 || a[0].HostID != "b-big" {
			t.Fatalf("raise %s: creates = %+v, want one on the host with room for the need", raise, a)
		}
		raised := a[0].Size != nil && a[0].Size.MemoryMB == typecheckNeed
		if raised != (raise == HistoryRaiseOn) {
			t.Fatalf("raise %s: created at %+v", raise, a[0].Size)
		}
	}
}

// Once GitHub has said a runner took typecheck, the plan names its need so the
// controller can give the valve a rule for it; only with the setting on, only
// for a job with five runs, and only above what the runner was given.
func TestThePlanNamesTheNeedOfARunnerWhoseJobOutgrowsIt(t *testing.T) {
	p := nodockerPool()
	h := trustedAgent("ci-nl-01-trusted", 40*gb)
	heavy, light := typecheckOn("run_heavy", p, h), typecheckOn("run_light", p, h)
	s := snap([]*store.Pool{p}, []*store.Runner{heavy, light}, nil, []*store.Host{h})
	s.Policy.MemoryBookingHeadroomPercent = 25
	s.JobHistory = map[store.JobUsageKey][]store.JobPeak{key("typecheck", p): typecheckRuns(), key("lint", p): peaksOf(20, 4*gb)}
	s.Booking = &BookingInputs{RunnerJobs: map[string]store.JobUsageKey{heavy.ID: key("typecheck", p), light.ID: key("lint", p)}}
	if got := Decide(s).RunnerNeeds; got != nil {
		t.Fatalf("with history_raise off the plan names %v", got)
	}
	s.Policy.HistoryRaise = HistoryRaiseOn
	got := Decide(s).RunnerNeeds
	if !reflect.DeepEqual(got, map[string]int64{heavy.ID: typecheckNeed}) {
		t.Fatalf("needs = %v, want only the typecheck runner at %d", got, typecheckNeed)
	}
	s.JobHistory[key("typecheck", p)] = peaksOf(4, 30*gb)
	if got := Decide(s).RunnerNeeds; got == nil || len(got) != 0 {
		t.Fatalf("a job with four runs named needs %v, want none (and not unknown)", got)
	}
	// A pass that could not say which job each runner runs names no needs at
	// all, so the controller keeps the ones it had.
	s.Booking = nil
	if got := Decide(s).RunnerNeeds; got != nil {
		t.Fatalf("without the running jobs the plan named %v, want nil", got)
	}
}

// In the overcommitted ledger a runner the valve will let grow to its job's
// need is booked at that ceiling, above its size, so the machine's one
// ceiling counts what it is about to take. Off, it is booked as before.
func TestARaisedRunnerIsBookedAtWhatItMayGrowTo(t *testing.T) {
	p := nodockerPool()
	h := trustedAgent("ci-nl-01-trusted", 40*gb)
	r := typecheckOn("run_tc", p, h)
	history := map[store.JobUsageKey][]store.JobPeak{key("typecheck", p): typecheckRuns()}
	in := &BookingInputs{RunnerJobs: map[string]store.JobUsageKey{r.ID: key("typecheck", p)}}

	b := newBooker(BookingHistory, 25, in, history, []*store.Pool{p}, nil)
	if got, charge, basis := b.runner(p, h, r); got != charge || basis != basisTyped {
		t.Fatalf("with history_raise off the runner books %d of %d (%s), want its size", got, charge, basis)
	}
	b = newBooker(BookingHistory, 25, in, history, []*store.Pool{p}, nil)
	b.raise = true
	got, charge, basis := b.runner(p, h, r)
	if got != typecheckNeed || charge != 24*gb || basis != basisRaised {
		t.Fatalf("raised, the runner books %d of %d (%s), want %d above its size", got, charge, basis, typecheckNeed)
	}
	// Lent memory changes nothing: the grant is what it may grow to.
	r.LentMemoryMB = 2 * gb
	if got, _, basis := b.runner(p, h, r); got != typecheckNeed || basis != basisRaised {
		t.Fatalf("lent 2 GB, the raised runner books %d (%s)", got, basis)
	}
	totals := bookingTotals([]*store.Host{h}, []*store.Pool{p}, map[string][]*store.Runner{p.ID: {r}}, b, nil, 100)
	if hb := totals[h.ID]; hb.RaisedByHistory != 1 || hb.BookedMemoryMB != typecheckNeed {
		t.Fatalf("the host books %+v, want one runner raised by history at %d", hb, typecheckNeed)
	}
}

// The valve's pool of memory includes what the grants may lend, which is why an
// agent whose host's guarantees fill the machine still carries a grant out, and
// what the host measured as free still bounds the whole.
func TestTheValvesCapacityIncludesTheHistoryGrantsAndStaysMeasured(t *testing.T) {
	free := int64(20 * gb)
	in := MemoryPoolInput{
		TotalMB: 62 * gb, FloorMB: 9 * gb,
		Workloads: []MemoryWorkload{{ID: "a", GuaranteeMB: 24 * gb}, {ID: "b", GuaranteeMB: 24 * gb}, {ID: "c", GuaranteeMB: 24 * gb}},
	}
	if got := PlanMemoryPool(in).CapacityMB; got != 0 {
		t.Fatalf("a host whose guarantees pass the machine lends %d MB without a grant, want none", got)
	}
	in.HistoryGrantMB = 6 * gb
	if got := PlanMemoryPool(in); got.CapacityMB != 6*gb || got.HistoryGrantMB != 6*gb {
		t.Fatalf("with a 6 GB grant the host may lend %+v, want 6 GB", got)
	}
	in.AvailableMB = &free
	in.HistoryGrantMB = 40 * gb
	if got := PlanMemoryPool(in); got.CapacityMB != free-9*gb || got.Binding != MemoryBoundMeasured {
		t.Fatalf("with 20 GB free and a 9 GB floor the host may lend %+v, want 11 GB measured", got)
	}
}

// The same under the policy the fleet runs: memory booked at 300% by history,
// starts charged 30% of their size. The raised runner books its raised size in
// the overcommitted ledger, and the reason says so.
func TestARaiseUnderTheLivePolicyIsBookedAtItsRaisedSize(t *testing.T) {
	p := trustedPool()
	h := trustedAgent("ci-nl-01-trusted", 49*gb)
	s := historyRaiseFleet(p, h, HistoryRaiseOn)
	s.Policy.MemoryOvercommitPercent, s.Policy.StartChargePercent, s.Policy.CPUOvercommitPercent = 300, 30, 400
	s.Policy.MemoryBooking = BookingHistory
	s.Booking = &BookingInputs{PoolRuns: map[string][]store.JobPeak{p.ID: peaksOf(100, 4*gb, 9*gb)}}
	a := actionsOf(only(t, Decide(s)).Actions, ActionCreate)
	if len(a) != 1 || a[0].Size == nil || a[0].Size.MemoryMB != typecheckNeed {
		t.Fatalf("creates = %+v, want one raised to %d MB", a, typecheckNeed)
	}
	if !strings.Contains(a[0].Reason, "booked its 29.4 GB; 29.4 GB of 163.3 GB booked on ci-nl-01-trusted") {
		t.Fatalf("the reason %q does not say the raised runner was booked at its raised size", a[0].Reason)
	}
}

// killedRuns is typecheck's day with kills in it: eighteen runs that finished
// at the same p90 of 23.5 GB, and two killed at the 24 GB limit.
func killedRuns() []store.JobPeak {
	out := typecheckRuns()[:18]
	for range 2 {
		out = append(out, store.JobPeak{MemoryMB: 24575, OOMKilled: true, GrantedMemoryMB: 24 * gb})
	}
	return out
}

// A kill says the limit was too little, and the headroom is how much more a
// raise gives for it: 24 GB killed is raised to 30 GB, not to the 45 GB the
// ledger's figure (half as much again, then the headroom) would make it. The
// ledger's own figure is unchanged.
func TestARaiseAfterAKillIsTheKilledLimitAndTheHeadroom(t *testing.T) {
	if got, ok := raiseNeedMB(killedRuns(), 25); !ok || got != 30*gb {
		t.Fatalf("after two kills at 24 GB the raise is %d MB (known %v), want 24 GB x 1.25 = 30 GB", got, ok)
	}
	if got, _ := historyBooking(killedRuns(), 25); got != 46080 {
		t.Fatalf("the ledger's figure moved to %d MB; it is 24 GB x 1.5 x 1.25 = 46080 and this change leaves it alone", got)
	}
	// Without kills the raise is the p90 and the headroom, as the booking is.
	if got, _ := raiseNeedMB(typecheckRuns(), 25); got != typecheckNeed {
		t.Fatalf("without kills the raise is %d MB, want %d", got, typecheckNeed)
	}
	// A p90 above the killed limit wins, and a killed run is one of the five.
	heavier := append(peaksOf(18, 28*gb), store.JobPeak{MemoryMB: 24575, OOMKilled: true, GrantedMemoryMB: 24 * gb})
	if got, _ := raiseNeedMB(heavier, 25); got != 35*gb {
		t.Fatalf("with a p90 of 28 GB the raise is %d MB, want 28 GB x 1.25 = 35 GB", got)
	}
	short := append(peaksOf(3, 20*gb), store.JobPeak{OOMKilled: true, GrantedMemoryMB: 24 * gb})
	if _, ok := raiseNeedMB(short, 25); ok {
		t.Fatal("four runs, one of them killed, gave a raise; fewer than five say nothing")
	}
	// The create, the grant and the raised booking all use it.
	p := nodockerPool()
	h := trustedAgent("ci-nl-01-trusted", 49*gb)
	s := historyRaiseFleet(p, h, HistoryRaiseOn)
	s.JobHistory[key("typecheck", p)] = killedRuns()
	if a := actionsOf(only(t, Decide(s)).Actions, ActionCreate); len(a) != 1 || a[0].Size == nil || a[0].Size.MemoryMB != 30*gb {
		t.Fatalf("after two kills the runner was created at %+v, want 30 GB", a)
	}
	r := typecheckOn("run_tc", p, h)
	history := map[store.JobUsageKey][]store.JobPeak{key("typecheck", p): killedRuns()}
	b := newBooker(BookingHistory, 25, &BookingInputs{RunnerJobs: map[string]store.JobUsageKey{r.ID: key("typecheck", p)}}, history, []*store.Pool{p}, nil)
	b.raise = true
	if got, _, basis := b.runner(p, h, r); got != 30*gb || basis != basisRaised {
		t.Fatalf("after two kills the raised runner books %d MB (%s), want 30 GB", got, basis)
	}
}

// heavyAndLight is the merge group: typecheck and five light jobs waiting on
// the pool, on two agents with room for every one of them raised.
func heavyAndLight(p *store.Pool) Snapshot {
	a, b := trustedAgent("a1", 55*gb), trustedAgent("a2", 55*gb)
	s := historyRaiseFleet(p, a, HistoryRaiseOn)
	s.Hosts = []*store.Host{a, b}
	for _, name := range []string{"lint", "unit (1/2)", "unit (2/2)", "build", "docs"} {
		j := queued(name, 0, p.Labels...)
		j.Repo, j.Workflow = "Vision-Elevators-Pty-Ltd/ops_app", "PR"
		s.Jobs = append(s.Jobs, j)
		s.JobHistory[key(name, p)] = peaksOf(20, 3*gb, 5*gb)
	}
	s.Policy.MemoryOvercommitPercent, s.Policy.StartChargePercent, s.Policy.CPUOvercommitPercent = 300, 30, 400
	s.Policy.MemoryBooking = BookingHistory
	s.Booking = &BookingInputs{PoolRuns: map[string][]store.JobPeak{p.ID: peaksOf(100, 4*gb, 9*gb)}}
	return s
}

func raisedCreates(as []Action) (raised, typed int) {
	for _, a := range actionsOf(as, ActionCreate) {
		if a.SizedFromHistory {
			raised++
		} else {
			typed++
		}
	}
	return raised, typed
}

// On a pool the valve can raise later, one heavy job among five light ones
// raises one runner, not six: whichever runner GitHub hands the heavy job, the
// valve lends it the rest once the job is known.
func TestOneHeavyJobAmongLightOnesRaisesOneRunnerWhereTheValveCanRaiseLater(t *testing.T) {
	p := nodockerPool()
	raised, typed := raisedCreates(only(t, Decide(heavyAndLight(p))).Actions)
	if raised != 1 || typed != 5 {
		t.Fatalf("1 heavy and 5 light jobs created %d raised and %d typed runners, want 1 and 5", raised, typed)
	}
	// Two heavy jobs, two raised runners.
	s := heavyAndLight(p)
	j := queued("typecheck-2", 0, p.Labels...)
	j.Repo, j.Workflow, j.JobName = "Vision-Elevators-Pty-Ltd/ops_app", "PR", "typecheck"
	s.Jobs = append(s.Jobs, j)
	if raised, _ := raisedCreates(only(t, Decide(s)).Actions); raised != 2 {
		t.Fatalf("2 heavy jobs created %d raised runners, want 2", raised)
	}
}

// A pair held to one shared limit has no second chance: the valve cannot raise
// its slice, and a heavy job GitHub hands a typed runner is killed at the
// typed size. So every runner created while the heavy job waits is raised
// where a host has room, and created at the typed size where none has, so the
// light jobs never wait for it.
func TestEveryRunnerOfASharedPairPoolIsRaisedWhileAHeavyJobWaits(t *testing.T) {
	p := trustedPool()
	raised, typed := raisedCreates(only(t, Decide(heavyAndLight(p))).Actions)
	if raised != 6 || typed != 0 {
		t.Fatalf("with room, 1 heavy and 5 light jobs created %d raised and %d typed runners, want 6 and 0", raised, typed)
	}
	// 17 GB free past each agent's 6 GB reserve holds one raised start (30% of
	// 29.4 GB, 8.8 GB) and then room for a typed start (7.2 GB) but not another
	// raised one: each agent takes one of each.
	s := heavyAndLight(p)
	for _, h := range s.Hosts {
		withUsage(h, 12, 6*gb+17*gb)
	}
	raised, typed = raisedCreates(only(t, Decide(s)).Actions)
	if raised != 2 || typed != 2 {
		t.Fatalf("short of room, the pass created %d raised and %d typed runners, want 2 and 2", raised, typed)
	}
	// No room for any raised start: every runner typed, none held back.
	s = heavyAndLight(p)
	for _, h := range s.Hosts {
		withUsage(h, 12, 6*gb+8*gb)
	}
	raised, typed = raisedCreates(only(t, Decide(s)).Actions)
	if raised != 0 || typed == 0 {
		t.Fatalf("with no room for a raised start the pass created %d raised and %d typed runners, want only typed", raised, typed)
	}
}

// A raised runner still waiting for its job counts against the heavy jobs, so
// the next pass does not raise another for the same one. Once it is running a
// job it no longer does.
func TestARaisedRunnerWithoutAJobIsNotRaisedAgainOnTheNextPass(t *testing.T) {
	p := nodockerPool()
	s := heavyAndLight(p)
	waiting := testRunner("run_raised", p, store.RunnerRegistering, 0)
	waiting.HostID, waiting.AllocatedMemoryMB, waiting.AllocationSource = "a1", typecheckNeed, store.AllocationHistory
	s.Runners = map[string][]*store.Runner{p.ID: {waiting}}
	if raised, _ := raisedCreates(only(t, Decide(s)).Actions); raised != 0 {
		t.Fatalf("with a raised runner registering the pass raised %d more", raised)
	}
	waiting.State = store.RunnerBusy
	s.RunnersWithAJob = map[string]bool{waiting.ID: true}
	if raised, _ := raisedCreates(only(t, Decide(s)).Actions); raised != 1 {
		t.Fatalf("with the raised runner busy on another job the pass raised %d, want 1", raised)
	}
}

// A waiting raised runner smaller than the need it would serve does not count:
// one raised to 29.4 GB before a kill moved the need to 30 GB and more is not
// one the heavy job can use.
func TestARaisedRunnerBelowTheNeedDoesNotCountAsWaiting(t *testing.T) {
	p := nodockerPool()
	s := heavyAndLight(p)
	s.JobHistory[key("typecheck", p)] = append(typecheckRuns()[:18],
		store.JobPeak{MemoryMB: 30079, OOMKilled: true, GrantedMemoryMB: 30080},
		store.JobPeak{MemoryMB: 30079, OOMKilled: true, GrantedMemoryMB: 30080})
	small := testRunner("run_small", p, store.RunnerIdle, 0)
	small.HostID, small.AllocatedMemoryMB, small.AllocationSource = "a1", typecheckNeed, store.AllocationHistory
	s.Runners = map[string][]*store.Runner{p.ID: {small}}
	pp := only(t, Decide(s))
	raised, _ := raisedCreates(pp.Actions)
	if raised != 1 {
		t.Fatalf("with an idle runner raised to %d MB for a need of 37.6 GB the pass raised %d, want 1", typecheckNeed, raised)
	}
	for _, a := range actionsOf(pp.Actions, ActionCreate) {
		if a.SizedFromHistory && a.Size.MemoryMB != 37632 {
			t.Fatalf("raised to %d MB, want 30080 x 1.25 = 37632", a.Size.MemoryMB)
		}
	}
}

// A raised runner this pass reaps is not waiting for anything: counting it
// would leave the heavy job with no raised runner at all, because the creates
// that replace it fill the pool.
func TestARaisedRunnerReapedThisPassDoesNotCountAsWaiting(t *testing.T) {
	p := nodockerPool()
	s := heavyAndLight(p)
	s.Policy.ProvisionTimeout = 5 * time.Minute
	stuck := testRunner("run_stuck", p, store.RunnerProvisioning, time.Hour)
	stuck.HostID, stuck.AllocatedMemoryMB, stuck.AllocationSource = "a1", typecheckNeed, store.AllocationHistory
	s.Runners = map[string][]*store.Runner{p.ID: {stuck}}
	pp := only(t, Decide(s))
	failed := false
	for _, a := range pp.Actions {
		if a.RunnerID == stuck.ID && a.Kind != ActionCreate {
			failed = true
		}
	}
	if !failed {
		t.Fatalf("the stuck runner was not reaped, so this is not the case it means to be: %+v", pp.Actions)
	}
	if raised, _ := raisedCreates(pp.Actions); raised != 1 {
		t.Fatalf("with the raised runner reaped the pass raised %d, want 1", raised)
	}
}

// A runner the valve will let grow is booked at its grant only on a host whose
// agent can lend; elsewhere the grant can never be carried out.
func TestARaisedRunnerIsBookedAtItsGrantOnlyWhereTheAgentCanLend(t *testing.T) {
	p := nodockerPool()
	h := trustedAgent("ci-nl-01-trusted", 40*gb)
	r := typecheckOn("run_tc", p, h)
	history := map[store.JobUsageKey][]store.JobPeak{key("typecheck", p): typecheckRuns()}
	b := newBooker(BookingHistory, 25, &BookingInputs{RunnerJobs: map[string]store.JobUsageKey{r.ID: key("typecheck", p)}}, history, []*store.Pool{p}, nil)
	b.raise = true
	if got, _, basis := b.runner(p, h, r); got != typecheckNeed || basis != basisRaised {
		t.Fatalf("on a host that can lend the runner books %d MB (%s), want %d", got, basis, typecheckNeed)
	}
	h.Features = store.StringSlice{store.FeatureDinDSharedLimit}
	if got, charge, basis := b.runner(p, h, r); got != charge || got != 24*gb || basis != basisTyped {
		t.Fatalf("on a host that cannot lend the runner books %d of %d MB (%s), want its typed 24 GB", got, charge, basis)
	}
}

// Each kill at the raised size moves the next need up by the headroom, to twice
// the typed size: 24 -> 30 -> 37.5 -> 46.9 -> 48 GB. Adaptive and bounded.
func TestEachKillAtTheRaisedSizeMovesTheNeedUpToTheCap(t *testing.T) {
	p := nodockerPool()
	h := sized("big", 16, 64, 512*gb, 500*gb)
	limit := 24 * gb
	var got []int64
	for range 5 {
		runs := append(peaksOf(18, 9*gb), store.JobPeak{MemoryMB: limit - 1, OOMKilled: true, GrantedMemoryMB: limit})
		need, _ := raiseNeedMB(runs, 25)
		limit = min(need, HistoryRaiseCap(p, h))
		got = append(got, limit)
	}
	want := []int64{30 * gb, 38400, 48000, 48 * gb, 48 * gb}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("successive kills raise to %v, want %v", got, want)
	}
}

// A pool no host can create raised (a pair of whole limits) is not tried, and
// its reason does not claim a host was short of room.
func TestAPoolThatIsNeverRaisedAtCreationSaysNothingAboutRoom(t *testing.T) {
	p := pairPool()
	big := trustedAgent("big", 180*gb)
	big.MemoryMB = 200 * gb
	a := actionsOf(only(t, Decide(historyRaiseFleet(p, big, HistoryRaiseOn))).Actions, ActionCreate)
	if len(a) != 1 || a[0].Size != nil || strings.Contains(a[0].Reason, "no host has room") {
		t.Fatalf("creates = %+v, want one at the typed size with no claim about room", a)
	}
}

// A shared-limit pool on a host whose agent cannot hold a pair to one limit is
// a pair of whole limits there: it is not created raised, and its runner is
// lent for its job by the valve instead.
func TestASharedPoolOnAHostWithoutSharedLimitsIsRaisedByTheValve(t *testing.T) {
	p := trustedPool()
	h := nlAgent("old", "", 49*gb)
	h.Features = store.StringSlice{ElasticMemoryFeature}
	if a := actionsOf(only(t, Decide(historyRaiseFleet(p, h, HistoryRaiseOn))).Actions, ActionCreate); len(a) != 1 || a[0].Size != nil {
		t.Fatalf("creates = %+v, want the typed size", a)
	}
	if _, ok := HistoryGrantFor(p, h, typecheckOn("run_tc", p, h), typecheckNeed); !ok {
		t.Fatal("no valve grant for a pair the host does not hold to one limit")
	}
}
