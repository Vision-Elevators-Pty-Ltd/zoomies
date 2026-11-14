package scheduler

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/eyupio/zoomies/internal/store"
)

// The shape of 13 Nov 2026: a fleet full on memory, one old job whose runner
// is large (visual-build, 24 GB), and a stream of younger, smaller requests
// from other pools that compete for the same hosts (image builds, light jobs).

// fullHost is a 32 GB host (after its reserve) with plenty of slots, carrying
// the given busy runners.
func fullHost(id string, busy int) *store.Host {
	h := sized(id, 32, 64, hostFor(32*gb), 500*1024)
	h.ActiveRunners = busy
	return h
}

func busyAt(id string, p *store.Pool, hostID string) *store.Runner {
	return onHost(testRunner(id, p, store.RunnerBusy, time.Minute), hostID)
}

func oldestFirstSnap(pools []*store.Pool, runners []*store.Runner, jobs []*store.Job, hosts []*store.Host) Snapshot {
	s := snap(pools, runners, jobs, hosts)
	s.Policy.OldestFirst = OldestFirstOn
	return s
}

func createsFor(p Plan, poolID string) []Action {
	var out []Action
	for _, a := range p.Actions {
		if a.Kind == ActionCreate && a.PoolID == poolID {
			out = append(out, a)
		}
	}
	return out
}

func poolPlan(t *testing.T, p Plan, poolID string) PoolPlan {
	t.Helper()
	for _, pp := range p.Pools {
		if pp.PoolID == poolID {
			return pp
		}
	}
	t.Fatalf("no plan for %s", poolID)
	return PoolPlan{}
}

// The starvation itself, pass by pass. A host is full of 4 GB work; every pass
// one of those jobs finishes and a younger 4 GB job arrives. A 24 GB job queued
// before all of them is never started under the old order, because each pass
// the 4 GB of room that freed goes to the younger job that fits in it. Under
// oldest-first the room is held until it is whole and the old job starts.
func TestOldestFirstEndsTheStarvationOfALargeOldJob(t *testing.T) {
	for _, tc := range []struct {
		mode      string
		wantStart bool
	}{{OldestFirstOff, false}, {OldestFirstOn, true}} {
		t.Run(tc.mode, func(t *testing.T) {
			big := limited("visual", 8, 24*gb)
			small := limited("image", 2, 4*gb)
			var runners []*store.Runner
			for i := range 8 {
				runners = append(runners, busyAt(fmt.Sprintf("r_small_%d", i), small, "host_a"))
			}
			old := queued("job_old", 10*time.Minute, "visual")
			started, startedAt := false, -1
			for pass := range 8 {
				// One small job finished since the last pass, and a younger one
				// is queued.
				runners = runners[1:]
				jobs := []*store.Job{old, queued(fmt.Sprintf("job_young_%d", pass), time.Duration(8-pass)*time.Second, "image")}
				if started {
					jobs = jobs[1:]
				}
				h := fullHost("host_a", len(runners))
				s := snap([]*store.Pool{big, small}, runners, jobs, []*store.Host{h})
				s.Policy.OldestFirst = tc.mode
				plan := Decide(s)
				for _, a := range plan.Actions {
					if a.Kind != ActionCreate {
						continue
					}
					pool := small
					if a.PoolID == big.ID {
						pool = big
						if !started {
							started, startedAt = true, pass
						}
					}
					runners = append(runners, onHost(testRunner(fmt.Sprintf("r_new_%d_%s", pass, pool.Name), pool, store.RunnerBusy, 0), "host_a"))
				}
			}
			if started != tc.wantStart {
				t.Fatalf("oldest_first=%s: the old 24 GB job started=%v (pass %d), want %v", tc.mode, started, startedAt, tc.wantStart)
			}
			if tc.wantStart && startedAt != 5 {
				// 32 GB full of 4 GB work: six have to finish before 24 GB is whole.
				t.Errorf("the old job started on pass %d, want 5, the first pass with 24 GB free", startedAt)
			}
		})
	}
}

// While it waits, the old job's host is reserved for it, and the younger
// request that would have taken the room says why it is waiting, naming the
// job it is waiting for.
func TestAYoungerRequestWaitsForTheRoomReservedForAnOlderJob(t *testing.T) {
	big := limited("visual", 8, 24*gb)
	small := limited("image", 2, 4*gb)
	runners := []*store.Runner{
		busyAt("r1", small, "host_a"), busyAt("r2", small, "host_a"), busyAt("r3", small, "host_a"),
		busyAt("r4", small, "host_a"), busyAt("r5", small, "host_a"),
	}
	jobs := []*store.Job{queued("job_old", 10*time.Minute, "visual"), queued("job_young", time.Minute, "image")}
	plan := Decide(oldestFirstSnap([]*store.Pool{big, small}, runners, jobs, []*store.Host{fullHost("host_a", 5)}))

	if c := createsFor(plan, small.ID); len(c) != 0 {
		t.Fatalf("the younger request took the room the older job is waiting for: %+v", c)
	}
	pp := poolPlan(t, plan, small.ID)
	if pp.WaitingFor == nil || pp.WaitingFor.JobID != "job_old" || pp.WaitingFor.HostID != "host_a" {
		t.Fatalf("WaitingFor = %+v, want the old job's hold on host_a", pp.WaitingFor)
	}
	for _, want := range []string{"waiting: capacity on host_a reserved for older job job_old", "pool visual"} {
		if !strings.Contains(pp.Blocked, want) {
			t.Errorf("Blocked = %q, want it to contain %q", pp.Blocked, want)
		}
	}
	if !pp.BlockedAtCapacity || pp.BlockedNoEligibleHost {
		t.Errorf("a reservation is a full fleet working, not a fault: %+v", pp)
	}
	bp := poolPlan(t, plan, big.ID)
	if bp.Reserved == nil || bp.Reserved.HostID != "host_a" || bp.Reserved.Fits {
		t.Fatalf("Reserved = %+v, want a reservation on host_a for a job that does not fit yet", bp.Reserved)
	}
	if !strings.Contains(bp.Blocked, "reserved for its oldest job job_old") {
		t.Errorf("the old pool's own reason does not say its room is kept: %q", bp.Blocked)
	}
	if len(plan.Holds) != 1 || plan.Holds[0].JobID != "job_old" || plan.Holds[0].MemoryMB != 24*gb {
		t.Errorf("Holds = %+v, want the one 24 GB reservation", plan.Holds)
	}
}

// Backfill: the reservation is on the host where the old job will fit
// soonest, and younger work still starts everywhere else, and beside it where
// what is left covers it. Utilisation does not drop for the reservation.
func TestYoungerWorkStillBackfillsWhereItCannotDelayTheReservation(t *testing.T) {
	big := limited("visual", 8, 24*gb)
	small := limited("image", 2, 4*gb)
	small.MaxRunners = 20
	var runners []*store.Runner
	// host_a: 20 GB used, 12 GB free. host_b: 12 GB used, 20 GB free: the old
	// job is 4 GB short there and 12 GB short on host_a, so host_b is held.
	for i := range 5 {
		runners = append(runners, busyAt(fmt.Sprintf("ra%d", i), small, "host_a"))
	}
	for i := range 3 {
		runners = append(runners, busyAt(fmt.Sprintf("rb%d", i), small, "host_b"))
	}
	jobs := []*store.Job{queued("job_old", 10*time.Minute, "visual")}
	for i := range 6 {
		jobs = append(jobs, queued(fmt.Sprintf("job_young_%d", i), time.Minute, "image"))
	}
	hosts := []*store.Host{fullHost("host_a", 5), fullHost("host_b", 3)}

	plan := Decide(oldestFirstSnap([]*store.Pool{big, small}, runners, jobs, hosts))
	bp := poolPlan(t, plan, big.ID)
	if bp.Reserved == nil || bp.Reserved.HostID != "host_b" {
		t.Fatalf("Reserved = %+v, want host_b, the host 4 GB short rather than 12", bp.Reserved)
	}
	got := createsFor(plan, small.ID)
	if len(got) != 3 {
		t.Fatalf("younger creates = %d, want the 3 that fit in host_a's 12 GB", len(got))
	}
	for _, a := range got {
		if a.HostID != "host_a" {
			t.Errorf("a younger runner went on the reserved host: %+v", a)
		}
	}

	// The same fleet with the old order starts all six, three of them in the
	// 20 GB the old job is waiting for on host_b, ...
	off := snap([]*store.Pool{big, small}, runners, jobs, hosts)
	if n := len(createsFor(Decide(off), small.ID)); n != 6 {
		t.Fatalf("old order: younger creates = %d, want 6", n)
	}

	// ... and with 28 GB free on host_b a younger 4 GB runner still starts
	// beside the reservation: 28 - 24 leaves room for one.
	runners = runners[:6]
	hosts = []*store.Host{fullHost("host_a", 5), fullHost("host_b", 1)}
	plan = Decide(oldestFirstSnap([]*store.Pool{big, small}, runners, jobs, hosts))
	if c := createsFor(plan, big.ID); len(c) != 1 || c[0].HostID != "host_b" {
		t.Fatalf("the old job did not start on host_b once 24 GB was free there: %+v", c)
	}
	onB := 0
	for _, a := range createsFor(plan, small.ID) {
		if a.HostID == "host_b" {
			onB++
		}
	}
	if onB != 1 {
		t.Errorf("younger runners on host_b = %d, want 1 beside the old job's 24 GB", onB)
	}
}

// When the old job fits now, the room it fits in is kept for it within the
// pass, so a younger pool served earlier in the pass cannot take it first --
// which is what the old order did whenever the younger pool had waited longer
// since its last create.
func TestRoomThatFitsTheOldJobIsNotTakenEarlierInThePass(t *testing.T) {
	big := limited("visual", 8, 24*gb)
	small := limited("image", 2, 4*gb)
	// 24 GB free: the old job's runner exactly, or six younger ones.
	runners := []*store.Runner{busyAt("r1", small, "host_a"), busyAt("r2", small, "host_a")}
	jobs := []*store.Job{queued("job_old", 10*time.Minute, "visual")}
	for i := range 6 {
		jobs = append(jobs, queued(fmt.Sprintf("job_young_%d", i), time.Minute, "image"))
	}
	hosts := []*store.Host{fullHost("host_a", 2)}
	// The big pool provisioned more recently, so the old order serves the
	// small pool first in the pass.
	last := map[string]time.Time{big.ID: ago(time.Second), small.ID: ago(time.Hour)}

	off := snap([]*store.Pool{big, small}, runners, jobs, hosts)
	off.LastProvisioned = last
	if c := createsFor(Decide(off), big.ID); len(c) != 0 {
		t.Fatalf("old order: the old job started (%+v); the test no longer shows the overtaking", c)
	}
	on := oldestFirstSnap([]*store.Pool{big, small}, runners, jobs, hosts)
	on.LastProvisioned = last
	plan := Decide(on)
	if c := createsFor(plan, big.ID); len(c) != 1 {
		t.Fatalf("oldest-first: the old job did not get the room that fits it: %+v", plan.Actions)
	}
	if c := createsFor(plan, small.ID); len(c) != 0 {
		t.Errorf("younger runners placed in the old job's room: %+v", c)
	}
	for _, h := range plan.Holds {
		if h.JobID == "job_old" {
			t.Errorf("a hold outlived the runner it was kept for: %+v", h)
		}
	}
}

// Priority still comes first: a hold for a lower-priority pool's job does not
// bind a higher-priority pool, however much younger its job is.
func TestAHoldDoesNotBindAHigherPriorityPool(t *testing.T) {
	big := limited("visual", 8, 24*gb)
	small := limited("image", 2, 4*gb)
	small.Priority = 10
	runners := []*store.Runner{busyAt("r1", small, "host_a"), busyAt("r2", small, "host_a"), busyAt("r3", small, "host_a")}
	jobs := []*store.Job{queued("job_old", 10*time.Minute, "visual"), queued("job_young", time.Minute, "image")}
	plan := Decide(oldestFirstSnap([]*store.Pool{big, small}, runners, jobs, []*store.Host{fullHost("host_a", 3)}))
	if c := createsFor(plan, small.ID); len(c) != 1 {
		t.Fatalf("the higher-priority pool was held for a lower one's job: %+v", poolPlan(t, plan, small.ID))
	}
}

// A pool whose own oldest job is older than the hold is not bound by it: the
// line is by job age, not by which pool holds what.
func TestAnOlderJobIsNotHeldForAYoungerOnesReservation(t *testing.T) {
	big := limited("visual", 8, 24*gb)
	small := limited("image", 2, 4*gb)
	other := limited("lint", 2, 4*gb)
	var runners []*store.Runner
	for i := range 6 {
		runners = append(runners, busyAt(fmt.Sprintf("r%d", i), small, "host_a"))
	}
	jobs := []*store.Job{
		queued("job_oldest", 20*time.Minute, "lint"),
		queued("job_old", 10*time.Minute, "visual"),
		queued("job_young", time.Minute, "image"),
	}
	plan := Decide(oldestFirstSnap([]*store.Pool{big, small, other}, runners, jobs, []*store.Host{fullHost("host_a", 6)}))
	if c := createsFor(plan, other.ID); len(c) != 1 {
		t.Fatalf("the oldest job was held behind a younger job's reservation: %+v", poolPlan(t, plan, other.ID))
	}
	if c := createsFor(plan, small.ID); len(c) != 0 {
		t.Errorf("the youngest job took room reserved for an older one: %+v", c)
	}
}

// Off, the zero value included, nothing is held: the plan is the one the
// scheduler made before the setting existed.
func TestOldestFirstOffHoldsNothing(t *testing.T) {
	big := limited("visual", 8, 24*gb)
	small := limited("image", 2, 4*gb)
	runners := []*store.Runner{busyAt("r1", small, "host_a"), busyAt("r2", small, "host_a"), busyAt("r3", small, "host_a"),
		busyAt("r4", small, "host_a"), busyAt("r5", small, "host_a")}
	jobs := []*store.Job{queued("job_old", 10*time.Minute, "visual"), queued("job_young", time.Minute, "image")}
	plan := Decide(snap([]*store.Pool{big, small}, runners, jobs, []*store.Host{fullHost("host_a", 5)}))
	if len(plan.Holds) != 0 || len(createsFor(plan, small.ID)) != 1 {
		t.Fatalf("off: holds %+v, younger creates %d; want none and 1", plan.Holds, len(createsFor(plan, small.ID)))
	}
}
