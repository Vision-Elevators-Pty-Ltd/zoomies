package scheduler

import (
	"strings"
	"testing"
	"time"

	"github.com/eyupio/zoomies/internal/store"
)

// The fleet the change was measured on (6 Nov 2026): ci-nl-02 is one 62 GB
// machine with two agents booked at 300%, a ceiling of 3 x 54.4 = 163.3 GB.
// It refused 138 starts that day with 140-163 GB of the ceiling booked while
// 9-25 GB of its memory was in use, about 45 GB measured free.

func imageBuildPool() *store.Pool {
	p := limited("zoomies-image-build", 8, 16*gb)
	p.MaxRunners = 40
	return p
}

// usingMB is busyOn with the runner last measured using mb.
func usingMB(id string, p *store.Pool, h *store.Host, mb int64) *store.Runner {
	r := busyOn(id, p, h)
	r.MemoryBytes = mb << 20
	return r
}

// idleOn is a runner of p on h with no job yet, using mb, whose sample the
// host's measurement contains.
func idleOn(id string, p *store.Pool, h *store.Host, mb int64) *store.Runner {
	r := usingMB(id, p, h, mb)
	r.State = store.RunnerIdle
	return r
}

// nl02 is ci-nl-02 as it stood at a refusal: two agents, ten image-build
// runners between them booked at their whole 16 GB (160 GB of the 163.3 GB
// ceiling) and each using 1.2 GB, with 45 GB measured free and no history.
func nl02(admitByMeasure bool) (*hostSet, *store.Pool, *store.Host, *store.Host) {
	p := imageBuildPool()
	pilot := nlAgent("ci-nl-02-pilot", "ci-nl-02", 45*gb)
	trusted := nlAgent("ci-nl-02-trusted", "ci-nl-02", 45*gb)
	var runners []*store.Runner
	for i := range 10 {
		h := pilot
		if i%2 == 1 {
			h = trusted
		}
		runners = append(runners, usingMB("run_"+string(rune('a'+i)), p, h, 1229))
	}
	hs := newHostSetFrom([]*store.Host{pilot, trusted}, []*store.Pool{p}, map[string][]*store.Runner{p.ID: runners}, now,
		ledger{overcommitPercent: 300, cpuOvercommitPercent: 400, startChargePercent: 30, machineLabel: "host", admitByMeasure: admitByMeasure})
	return hs, p, pilot, trusted
}

// At the 6 Nov refusal, the growth guard holds the start the booking refused:
// ten runners that may each still grow to 16 GB are 148 GB of growth, against
// 39 GB measured free. The guard is every runner's growth, not the largest.
func TestTheGrowthGuardSumsEveryRunnersGrowth(t *testing.T) {
	booked, p, _, _ := nl02(false)
	if got := booked.place(p, 20); len(got) != 0 {
		t.Fatalf("booked admission placed %d image builds with 160 of 163.3 GB booked", len(got))
	}
	measured, p, pilot, trusted := nl02(true)
	// No history: every runner is expected to reach its 16 GB limit.
	want := 10 * (16*gb - 1229)
	if g := measured.guard(pilot.ID); g != want || measured.guard(trusted.ID) != want {
		t.Fatalf("the growth guard is %d and %d MB, want the machine's %d on both agents", g, measured.guard(trusted.ID), want)
	}
	if got := measured.place(p, 20); len(got) != 0 {
		t.Fatalf("measured admission placed %d image builds; the guard is %d MB past 39 GB free", len(got), want)
	}
	if hb := measured.bookings[pilot.ID]; hb == nil || hb.Admission != AdmissionMeasured || hb.GrowthReserveMB != want ||
		hb.ExpectedGrowthMB != want || hb.LimitGrowthMB != 0 {
		t.Fatalf("the host's booking view is %+v", hb)
	}
}

// The guard's two parts, from history: a runner running a known job may grow
// to its job's ninetieth percentile (no headroom), one with no job yet to its
// pool's median; and the most any one of them may go on past that to its limit.
func TestTheGrowthGuardIsEveryJobsUsualPeakAndOneLimit(t *testing.T) {
	p := browserPool() // 32 GB
	h := withUsage(sized("ci-big", 14, 24, 256*gb, 500*1024), 12, 120*gb)
	h.ReserveMemoryMB = 6144
	visual, draft := usingMB("run_v", p, h, 3*gb), usingMB("run_d", p, h, 4*gb)
	idle := idleOn("run_i", p, h, 100)
	history := map[store.JobUsageKey][]store.JobPeak{
		key("visual-shard (1/3)", p): peaksOf(20, 8*gb),
		key("draft-affected", p):     peaksOf(20, 22*gb),
	}
	in := &BookingInputs{
		PoolRuns:   map[string][]store.JobPeak{p.ID: peaksOf(100, 2*gb, 4*gb, 6*gb)},
		RunnerJobs: map[string]store.JobUsageKey{visual.ID: key("visual-shard (1/3)", p), draft.ID: key("draft-affected", p)},
	}
	b := newBooker(BookingHistory, 25, in, history, []*store.Pool{p}, nil)
	hs := newHostSetFrom([]*store.Host{h}, []*store.Pool{p}, map[string][]*store.Runner{p.ID: {visual, draft, idle}}, now,
		ledger{overcommitPercent: 300, cpuOvercommitPercent: 800, startChargePercent: 30, book: b, admitByMeasure: true})
	// visual 8 - 3 = 5 GB; draft 22 - 4 = 18 GB; the idle runner, its pool's
	// median of 4 GB less its 100 MB.
	expected := (8*gb - 3*gb) + (22*gb - 4*gb) + (4*gb - 100)
	// Past that to the 32 GB limit: the idle runner has 28 GB to go.
	limit := 32*gb - 4*gb
	if hs.growthSum[h.ID] != expected || hs.growthMax[h.ID] != limit {
		t.Fatalf("growth guard %d + %d MB, want %d + %d", hs.growthSum[h.ID], hs.growthMax[h.ID], expected, limit)
	}
	// A start adds its own: from its 9.6 GB charge to its pool's 4 GB median
	// is nothing, and past the charge to 32 GB is 22.4 GB, below the largest.
	before := hs.measuredMemory[h.ID]
	if got := hs.place(p, 1); len(got) != 1 {
		t.Fatalf("no start with %d MB past the guard", before)
	}
	if hs.growthMax[h.ID] != limit || hs.growthSum[h.ID] != expected {
		t.Fatalf("after a start the guard is %d + %d MB", hs.growthSum[h.ID], hs.growthMax[h.ID])
	}
	if spent := before - hs.measuredMemory[h.ID]; spent != startCharge(32*gb, 0, 30) {
		t.Fatalf("a start took %d MB from the measurement, want only its charge", spent)
	}
}

// A large machine whose idle runners are booked for the heaviest job waiting:
// the booking is full, but what they typically use is small, so measured
// admission starts what the measurement less the guard allows.
func TestMeasuredAdmissionStartsWhereTheGuardFits(t *testing.T) {
	p := browserPool()
	h := withUsage(sized("p01-ci-01-pr", 40, 32, 225478, 500*1024), 20, 200*gb)
	h.ReserveMemoryMB = 8192
	var runners []*store.Runner
	for i := range 20 {
		runners = append(runners, idleOn("idle_"+string(rune('a'+i)), p, h, 100))
	}
	history := map[store.JobUsageKey][]store.JobPeak{key("draft-affected", p): peaksOf(20, 26*gb)}
	in := &BookingInputs{PoolRuns: map[string][]store.JobPeak{p.ID: peaksOf(100, 2*gb)}}
	waiting := &store.Job{Repo: "Vision-Elevators-Pty-Ltd/ops_app", Workflow: "PR", JobName: "draft-affected", State: store.JobQueued}
	b := newBooker(BookingHistory, 25, in, history, []*store.Pool{p}, map[string][]*store.Job{p.ID: {waiting}})
	at := func(measure bool) *hostSet {
		return newHostSetFrom([]*store.Host{h}, []*store.Pool{p}, map[string][]*store.Runner{p.ID: runners}, now,
			ledger{overcommitPercent: 300, cpuOvercommitPercent: 800, startChargePercent: 30, book: b, admitByMeasure: measure})
	}
	booked := at(false)
	if left := booked.bookedMemory[h.ID]; left >= 32*gb {
		t.Fatalf("20 idle runners booked for draft-affected should fill the ceiling; %d MB left", left)
	}
	if got := booked.place(p, 5); len(got) != 0 {
		t.Fatalf("booked admission placed %d", len(got))
	}
	measured := at(true)
	got := measured.placeAvoiding(p, 5, nil)
	if len(got) == 0 {
		t.Fatal("measured admission placed nothing on 192 GB free with a 68 GB guard")
	}
	for _, pl := range got {
		if pl.size != nil {
			t.Fatalf("a runner was created at %+v; its limit must stay 32 GB", *pl.size)
		}
	}
	if m := measured.measuredMemory[h.ID]; m < 0 {
		t.Fatalf("placing left the guard unfunded by %d MB", -m)
	}
}

// The measured check is unchanged under measured admission: nothing starts
// where the measurement is short of the reserve and one start charge, however
// much room the booking has, and at or below the reserve the pressure hold
// refuses the host outright.
func TestMeasuredAdmissionKeepsTheMeasuredCheckAndTheReserve(t *testing.T) {
	p := imageBuildPool()
	h := nlAgent("ci-nl-02-pilot", "", 0)
	runners := map[string][]*store.Runner{p.ID: {usingMB("run_a", p, h, 2*gb)}}
	at := func(freeMB int64) *hostSet {
		h.Usage.MemoryAvailableMB = &freeMB
		return newHostSetFrom([]*store.Host{h}, []*store.Pool{p}, runners, now,
			ledger{overcommitPercent: 300, startChargePercent: 30, admitByMeasure: true})
	}
	short := h.MemoryReserve() + startCharge(16*gb, 0, 30) - 1
	hs := at(short)
	if got := hs.place(p, 20); len(got) != 0 {
		t.Fatalf("placed %d one MB short of the reserve and a start charge, with 147 GB of booking left", len(got))
	}
	if d := hs.shortOfMemory(h, p); !strings.Contains(d, "measured free") || !strings.Contains(d, "is charged") {
		t.Fatalf("short of memory says %q, want the measurement named", d)
	}
	if got := at(short+1).place(p, 20); len(got) != 1 {
		t.Fatalf("placed %d with exactly the reserve and one charge free, want 1", len(got))
	}
	if got := at(h.MemoryReserve()).place(p, 20); len(got) != 0 {
		t.Fatalf("placed %d on a host whose free memory is at its reserve", len(got))
	}
}

// Where the booking is full, a start needs the reserve, its charge and the
// whole guard, its own growth included, measured free: one MB short refuses
// it, exactly enough takes one, and the reason names the guard.
func TestMeasuredAdmissionPastAFullBookingHoldsTheGrowthGuard(t *testing.T) {
	p := imageBuildPool()
	h := withUsage(sized("ci-big", 20, 64, 256*gb, 500*1024), 10, 0)
	h.ReserveMemoryMB = 4 * gb
	var rs []*store.Runner
	jobs := map[string]store.JobUsageKey{}
	for i := range 10 { // each runs a job whose p90 is 15 GB, and uses 2 GB
		r := usingMB("run_"+string(rune('a'+i)), p, h, 2*gb)
		rs = append(rs, r)
		jobs[r.ID] = key("deployability-compose (web)", p)
	}
	history := map[store.JobUsageKey][]store.JobPeak{key("deployability-compose (web)", p): peaksOf(20, 15*gb)}
	in := &BookingInputs{PoolRuns: map[string][]store.JobPeak{p.ID: peaksOf(100, 4*gb)}, RunnerJobs: jobs}
	b := newBooker(BookingHistory, 25, in, history, []*store.Pool{p}, nil)
	at := func(freeMB int64) *hostSet {
		h.Usage.MemoryAvailableMB = &freeMB
		hs := newHostSetFrom([]*store.Host{h}, []*store.Pool{p}, map[string][]*store.Runner{p.ID: rs}, now,
			ledger{overcommitPercent: 300, cpuOvercommitPercent: 800, startChargePercent: 30, book: b, admitByMeasure: true})
		hs.bookedMemory[h.ID] = 0 // the booking full, as if the rest were booked: only the measured rule can admit
		hs.syncMemory(h.ID)
		return hs
	}
	// The runners may still grow from 2 GB to 15 GB each: 130 GB; one of them
	// 1 GB more to its limit. A start is charged 4.8 GB and typically uses its
	// pool's 4 GB median, below its charge; past its charge to 16 GB is 11.2 GB,
	// the largest growth to a limit now.
	charge := startCharge(16*gb, 0, 30)
	guard := 10*(15*gb-2*gb) + (16*gb - charge)
	short := h.MemoryReserve() + charge + guard - 1
	hs := at(short)
	if got := hs.place(p, 20); len(got) != 0 {
		t.Fatalf("placed %d one MB short of the reserve, the charge and the guard", len(got))
	}
	if d := hs.shortOfMemory(h, p); !strings.Contains(d, "growth guard") || !strings.Contains(d, "nor has its booking room") {
		t.Fatalf("short of memory says %q, want the growth guard and the booking named", d)
	}
	if got := at(short+1).place(p, 20); len(got) != 1 {
		t.Fatalf("placed %d with exactly the reserve, the charge and the guard free, want 1", len(got))
	}
}

// Measured admission never refuses what booked admission takes: on a quiet
// machine whose booking has room, a guard the measurement cannot cover does not
// cost a start the booking and the measurement allowed before.
func TestMeasuredAdmissionNeverAdmitsLessThanBooked(t *testing.T) {
	p := browserPool()
	h := nlAgent("ci-nl-01-pr", "", 47*gb)
	idle := idleOn("run_idle", p, h, 200) // no history: a 32 GB guard
	runners := map[string][]*store.Runner{p.ID: {idle}}
	count := func(measure bool) int {
		hs := newHostSetFrom([]*store.Host{h}, []*store.Pool{p}, runners, now,
			ledger{overcommitPercent: 300, cpuOvercommitPercent: 400, startChargePercent: 30, admitByMeasure: measure})
		return len(hs.place(p, 20))
	}
	booked, measured := count(false), count(true)
	if booked == 0 || measured != booked {
		t.Fatalf("booked admission placed %d and measured %d; measured must take at least what booked does", booked, measured)
	}
}

// The booking is a soft hint: between two machines with the same free memory,
// the one booked less takes the start, though either could.
func TestTheBookingOrdersHostsUnderMeasuredAdmission(t *testing.T) {
	p := imageBuildPool()
	light := nlAgent("ci-nl-01-trusted", "", 45*gb)
	heavy := nlAgent("ci-nl-02-trusted", "", 45*gb)
	var runners []*store.Runner
	for i := range 6 {
		runners = append(runners, usingMB("heavy_"+string(rune('a'+i)), p, heavy, 1229))
	}
	runners = append(runners, usingMB("light_a", p, light, 1229))
	hs := newHostSetFrom([]*store.Host{heavy, light}, []*store.Pool{p}, map[string][]*store.Runner{p.ID: runners}, now,
		ledger{overcommitPercent: 300, cpuOvercommitPercent: 400, startChargePercent: 30, admitByMeasure: true})
	if got := hs.place(p, 1); len(got) != 1 || got[0] != light.ID {
		t.Fatalf("the start went to %v, want the host booked less (%s)", got, light.ID)
	}
}

// Nothing changes at 100% overcommit: there is no booking ceiling to lift.
func TestMeasuredAdmissionChangesNothingWithoutOvercommit(t *testing.T) {
	p := imageBuildPool()
	h := nlAgent("ci-nl-02-pilot", "", 45*gb)
	runners := map[string][]*store.Runner{p.ID: {usingMB("run_a", p, h, 1229), usingMB("run_b", p, h, 1229)}}
	off := newHostSetFrom([]*store.Host{h}, []*store.Pool{p}, runners, now, ledger{overcommitPercent: 100, startChargePercent: 30})
	on := newHostSetFrom([]*store.Host{h}, []*store.Pool{p}, runners, now, ledger{overcommitPercent: 100, startChargePercent: 30, admitByMeasure: true})
	if on.byMeasure || on.growthSum != nil {
		t.Fatal("measured admission is on at 100% overcommit")
	}
	if a, b := len(off.place(p, 20)), len(on.place(p, 20)); a != b {
		t.Fatalf("placed %d booked and %d measured at 100%%", a, b)
	}
}

// Finding 1 of the vision.9 review: a start charged a job's whole need when it
// was placed is charged that again by the passes after, until a host sample
// contains it. Charged only the percentage, the next pass on the same sample
// admitted a second 16 GB start into room for one.
func TestAPendingStartKeepsItsChargeAcrossPasses(t *testing.T) {
	p := imageBuildPool()
	h := nlAgent("ci-nl-02-pilot", "", 0)
	free := h.MemoryReserve() + 20*gb
	h.Usage.MemoryAvailableMB = &free
	need := &Requirement{MemoryMB: 16 * gb}
	pass := func(runners []*store.Runner, charges map[string]Reservation) (*hostSet, []placement) {
		hs := newHostSetFrom([]*store.Host{h}, []*store.Pool{p}, map[string][]*store.Runner{p.ID: runners}, now,
			ledger{overcommitPercent: 300, cpuOvercommitPercent: 400, startChargePercent: 30, startCharges: charges})
		hs.need = need
		return hs, hs.placeAvoiding(p, 1, nil)
	}
	_, first := pass(nil, map[string]Reservation{})
	if len(first) != 1 || first[0].charge.MemoryMB != 16*gb {
		t.Fatalf("the first pass placed %+v; want one start charged the job's 16 GB", first)
	}
	// The runner it made: provisioning, created after the host's last sample.
	r := testRunner("run_new", p, store.RunnerProvisioning, 0)
	r.HostID = h.ID
	r.CreatedAt = now.Add(time.Second)
	runners := []*store.Runner{r}
	charges := map[string]Reservation{r.ID: first[0].charge}
	for i := range 3 { // three passes, no new heartbeat
		hs, again := pass(runners, charges)
		if len(again) != 0 {
			t.Fatalf("pass %d placed a second 16 GB start into 20 GB free with one 16 GB start pending", i+2)
		}
		if want := 20*gb - 16*gb; hs.measuredMemory[h.ID] != want {
			t.Fatalf("pass %d measured %d MB free, want %d (the pending start's 16 GB charge held)", i+2, hs.measuredMemory[h.ID], want)
		}
	}
	// With records kept, a pending runner with none is charged its whole size;
	// with none kept (nil), the percentage, as before.
	if hs, _ := pass(runners, map[string]Reservation{}); hs.measuredMemory[h.ID] != 20*gb-16*gb {
		t.Fatalf("an unrecorded pending runner was charged %d MB, want its whole 16 GB", 20*gb-hs.measuredMemory[h.ID])
	}
	if hs, _ := pass(runners, nil); hs.measuredMemory[h.ID] != 20*gb-startCharge(16*gb, 0, 30) {
		t.Fatalf("without records a pending runner was charged %d MB, want the 30%%", 20*gb-hs.measuredMemory[h.ID])
	}
}

// The same on a machine of two agents: the charge of a start pending on one
// is held against the machine's one measurement on both.
func TestAPendingStartsChargeIsTheMachines(t *testing.T) {
	p := imageBuildPool()
	pilot := nlAgent("ci-nl-02-pilot", "ci-nl-02", 0)
	trusted := nlAgent("ci-nl-02-trusted", "ci-nl-02", 0)
	free := pilot.MemoryReserve() + 20*gb
	pilot.Usage.MemoryAvailableMB, trusted.Usage.MemoryAvailableMB = &free, &free
	r := testRunner("run_new", p, store.RunnerProvisioning, 0)
	r.HostID, r.CreatedAt = pilot.ID, now.Add(time.Second)
	hs := newHostSetFrom([]*store.Host{pilot, trusted}, []*store.Pool{p}, map[string][]*store.Runner{p.ID: {r}}, now,
		ledger{overcommitPercent: 300, startChargePercent: 30, machineLabel: "host", startCharges: map[string]Reservation{r.ID: {MemoryMB: 16 * gb}}})
	if hs.measuredMemory[pilot.ID] != 4*gb || hs.measuredMemory[trusted.ID] != 4*gb {
		t.Fatalf("the machine measured %d / %d MB free, want 4 GB on both agents", hs.measuredMemory[pilot.ID], hs.measuredMemory[trusted.ID])
	}
}

// Finding 2 of the vision.9 review: a reduced runner is admitted at its
// reduced size, not on room worked out for the standard one. The reviewer's
// case: ten 16 GB runners using 2 GB each, 16 GB free past the reserve, a 30%
// charge and an 8 GB minimum. Booked admission refuses; measured created an
// 11.2 GB runner and left the guard 1.4 GB unfunded.
func TestAReducedStartIsAdmittedAtItsOwnSize(t *testing.T) {
	p := imageBuildPool()
	p.Resources.MinMemoryMB = 8 * gb
	h := nlAgent("ci-nl-02-pilot", "", 0)
	free := h.MemoryReserve() + 16*gb
	h.Usage.MemoryAvailableMB = &free
	var rs []*store.Runner
	for i := range 10 {
		rs = append(rs, usingMB("run_"+string(rune('a'+i)), p, h, 2*gb))
	}
	for _, measure := range []bool{false, true} {
		hs := newHostSetFrom([]*store.Host{h}, []*store.Pool{p}, map[string][]*store.Runner{p.ID: rs}, now,
			ledger{overcommitPercent: 300, cpuOvercommitPercent: 400, startChargePercent: 30, admitByMeasure: measure})
		if got := hs.placeAvoiding(p, 5, nil); len(got) != 0 {
			t.Fatalf("measure=%v placed %d (first size %+v); the guard and the booking both refuse", measure, len(got), got[0].size)
		}
	}
	// Where a reduced runner is admissible, it is the largest admissible size,
	// and placing it leaves the measurement covering its charge. On an empty
	// machine with 3 GB measured free, a 32 GB pool with an 8 GB minimum fits
	// 10 GB (30% of 10 GB is 3 GB), not the 25 GB that the room worked out for
	// a 32 GB start (3 GB + 70% of 32 GB) suggested.
	big := browserPool()
	big.Resources.MinMemoryMB = 8 * gb
	free = h.MemoryReserve() + 3*gb
	hs := newHostSetFrom([]*store.Host{h}, []*store.Pool{big}, nil, now,
		ledger{overcommitPercent: 300, cpuOvercommitPercent: 400, startChargePercent: 30})
	got := hs.placeAvoiding(big, 1, nil)
	if len(got) != 1 || got[0].size == nil {
		t.Fatalf("placed %+v; want one reduced runner", got)
	}
	// The largest size whose charge, 30% rounded down, is within 3,072 MB.
	if mb := got[0].size.MemoryMB; mb != 10243 {
		t.Fatalf("a reduced runner of %d MB, want 10,243 MB, the largest whose 30%% charge fits 3 GB", mb)
	}
	if hs.measuredMemory[h.ID] < 0 {
		t.Fatalf("placing the reduced runner left the measurement %d MB short", -hs.measuredMemory[h.ID])
	}
}

// The kill switch: scheduler.memory_admission=booked, through Decide, is the
// vision.8 plan; measured creates where the measurement less the guard allows.
func TestMemoryAdmissionIsAKillSwitchThroughThePolicy(t *testing.T) {
	plan := func(mode string) Plan {
		p := browserPool()
		p.Labels = store.StringSlice{"self-hosted", "zoomies-pr-browser"}
		p.HostSelector = store.StringMap{}
		p.MaxRunners = 40
		h := withUsage(sized("p01-ci-01-pr", 40, 32, 225478, 500*1024), 20, 200*gb)
		h.ReserveMemoryMB = 8192
		var runners []*store.Runner
		for i := range 20 {
			r := idleOn("idle_"+string(rune('a'+i)), p, h, 100)
			r.PoolID = p.ID
			runners = append(runners, r)
		}
		var jobs []*store.Job
		for i := range 25 {
			j := queued("job_"+string(rune('a'+i)), time.Minute, "self-hosted", "zoomies-pr-browser")
			j.Repo, j.Workflow, j.JobName = "Vision-Elevators-Pty-Ltd/ops_app", "PR", "draft-affected"
			jobs = append(jobs, j)
		}
		s := snap([]*store.Pool{p}, runners, jobs, []*store.Host{h})
		s.Policy.MemoryOvercommitPercent, s.Policy.CPUOvercommitPercent, s.Policy.StartChargePercent = 300, 800, 30
		s.Policy.MemoryBooking, s.Policy.MemoryBookingHeadroomPercent = BookingHistory, 25
		s.Policy.MemoryAdmission = mode
		s.JobHistory = map[store.JobUsageKey][]store.JobPeak{key("draft-affected", p): peaksOf(20, 26*gb)}
		s.Booking = &BookingInputs{PoolRuns: map[string][]store.JobPeak{p.ID: peaksOf(100, 2*gb)}}
		s.StartCharges = map[string]Reservation{}
		return Decide(s)
	}
	if got := actionsOf(plan(AdmissionBooked).Actions, ActionCreate); len(got) != 0 {
		t.Fatalf("booked admission created %d runners with 20 idle runners booked for draft-affected", len(got))
	}
	if got := actionsOf(plan("").Actions, ActionCreate); len(got) != 0 {
		t.Fatalf("an unset admission created %d runners; the zero value must be booked", len(got))
	}
	got := actionsOf(plan(AdmissionMeasured).Actions, ActionCreate)
	if len(got) == 0 {
		t.Fatal("measured admission created nothing on 192 GB free")
	}
	for _, a := range got {
		if a.StartCharge.MemoryMB != startCharge(32*gb, 0, 30) {
			t.Fatalf("a create carries a start charge of %d MB, want the 30%% it was charged", a.StartCharge.MemoryMB)
		}
	}
}

// staggered is a machine of two agents whose samples were taken 20 seconds
// apart, with six idle 32 GB runners on `on`, no use measured yet, two of them
// started 10 seconds ago: between the two samples. Their pool's median run is
// 2 GB, and a heavy job waiting books each of them whole, so the booking is
// full and only the measured rule can admit. older and newer are the free
// memory past the reserve each agent sampled.
func staggered(t *testing.T, olderFree, newerFree int64, onNewer bool, charges map[string]Reservation) (*hostSet, *store.Pool, *store.Host, *store.Host) {
	t.Helper()
	p := browserPool()
	p.Resources.CPUs = 1
	older := nlAgent("older-agent", "machine", 0)
	newer := nlAgent("newer-agent", "machine", 0)
	of, nf := older.MemoryReserve()+olderFree, newer.MemoryReserve()+newerFree
	older.Usage.MemoryAvailableMB, newer.Usage.MemoryAvailableMB = &of, &nf
	older.Usage.SampledAt = now.Add(-20 * time.Second)
	newer.Usage.SampledAt = now
	on := older
	if onNewer {
		on = newer
	}
	var rs []*store.Runner
	for i := range 6 {
		r := idleOn("r"+string(rune('a'+i)), p, on, 0)
		if i < 2 {
			r.CreatedAt = now.Add(-10 * time.Second)
			started := r.CreatedAt
			r.ContainerStartedAt = &started
		}
		rs = append(rs, r)
	}
	history := map[store.JobUsageKey][]store.JobPeak{key("heavy", p): peaksOf(20, 26*gb)}
	in := &BookingInputs{PoolRuns: map[string][]store.JobPeak{p.ID: peaksOf(100, 2*gb)}}
	waiting := &store.Job{Repo: key("heavy", p).Repo, Workflow: "PR", JobName: "heavy", State: store.JobQueued}
	b := newBooker(BookingHistory, 25, in, history, []*store.Pool{p}, map[string][]*store.Job{p.ID: {waiting}})
	hs := newHostSetFrom([]*store.Host{older, newer}, []*store.Pool{p}, map[string][]*store.Runner{p.ID: rs}, now,
		ledger{overcommitPercent: 300, cpuOvercommitPercent: 400, startChargePercent: 30, book: b, machineLabel: "host",
			admitByMeasure: true, startCharges: charges})
	return hs, p, older, newer
}

func chargesFor(ids ...string) map[string]Reservation {
	out := map[string]Reservation{}
	for _, id := range ids {
		out[id] = Reservation{CPUs: 0.3, MemoryMB: startCharge(32*gb, 0, 30)}
	}
	return out
}

// Review 2, major: growth is judged against the same sample as the machine's
// measurement. The runners started between the samples are on the agent with
// the older sample, and the newer, lower sample is the machine's: it already
// contains them, so they are charged nothing and must be counted as using what
// they were measured using (nothing), not their start charge. Every runner may
// then still grow to the 2 GB median, and one on to 32 GB: 6 x 2 + 30 = 42 GB.
// Counting the two at their charge reserved 38 GB and admitted a 32 GB start
// 3.6 GB short. With or without a record of their charges (a restart, or one
// past its 30 minutes), the answer is the same.
func TestGrowthIsJudgedAgainstTheMachinesSampleWhenItIsTheNewer(t *testing.T) {
	for _, charges := range []map[string]Reservation{chargesFor("ra", "rb", "rc", "rd", "re", "rf"), {}} {
		hs, p, older, newer := staggered(t, 55*gb, 48*gb, false, charges)
		want := int64(6*2*gb + 30*gb)
		if hs.guard(newer.ID) != want || hs.guard(older.ID) != want {
			t.Fatalf("records %d: guard %d / %d MB, want %d on both agents", len(charges), hs.guard(older.ID), hs.guard(newer.ID), want)
		}
		if hs.measuredMemory[newer.ID] != 48*gb-want {
			t.Fatalf("records %d: measurement %d MB, want 48 GB less the guard and no pending charge", len(charges), hs.measuredMemory[newer.ID])
		}
		if got := hs.placeAvoiding(p, 1, nil); len(got) != 0 {
			t.Fatalf("records %d: a 32 GB start was admitted with the guard %d MB past free memory", len(charges), want+startCharge(32*gb, 0, 30)-48*gb)
		}
	}
}

// The other direction: the older sample is the machine's, and the two runners,
// on the agent with the newer sample, started after it. The measurement is
// charged their start charge (recorded) or, with no record, their whole size,
// and growth counts them as already using that much, so nothing is reserved
// twice: the four older runners grow 2 GB each, the two charged 9.6 GB grow
// nothing past their charge, and one runner goes on to its 32 GB limit.
func TestGrowthIsJudgedAgainstTheMachinesSampleWhenItIsTheOlder(t *testing.T) {
	charge := startCharge(32*gb, 0, 30)
	for _, tc := range []struct {
		charges map[string]Reservation
		pending int64
	}{
		{chargesFor("ra", "rb"), 2 * charge},
		{map[string]Reservation{}, 2 * 32 * gb},
	} {
		hs, _, older, newer := staggered(t, 48*gb, 55*gb, true, tc.charges)
		want := int64(4*2*gb + 30*gb)
		if hs.guard(older.ID) != want || hs.guard(newer.ID) != want {
			t.Fatalf("records %d: guard %d / %d MB, want %d", len(tc.charges), hs.guard(older.ID), hs.guard(newer.ID), want)
		}
		// The guard is taken after the measurement's floor at zero.
		if got := hs.measuredMemory[older.ID]; got != max(48*gb-tc.pending, 0)-want {
			t.Fatalf("records %d: measurement %d MB, want 48 GB less %d MB pending (floored at zero) and the guard", len(tc.charges), got, tc.pending)
		}
	}
}
