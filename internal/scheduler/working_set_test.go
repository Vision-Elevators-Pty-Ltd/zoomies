package scheduler

import (
	"testing"

	"github.com/eyupio/zoomies/internal/store"
)

// Review of #20, blocker 1, the review's probe made permanent: five light runs
// of one job measured the new way must not drop a pool's old runs of a heavy
// job that has not been, so the booking of a runner that may yet take the
// heavy job is what it was. Before the fix the pool's booking fell from
// 10,240 MiB to 1,280 MiB.
func TestRemeasuredLightJobsDoNotLowerAPoolsBookingForAHeavyJob(t *testing.T) {
	var peaks []store.JobPeak
	for range 5 {
		peaks = append(peaks, store.JobPeak{Job: "light", MemoryMB: 1024, WorkingSetMB: 1024})
	}
	for range 20 {
		peaks = append(peaks, store.JobPeak{Job: "heavy", MemoryMB: 8192})
	}
	before, _ := historyBooking(peaks, 25)
	after, _ := historyBooking(store.PreferWorkingSet(peaks), 25)
	if after < before {
		t.Fatalf("the pool's booking fell from %d to %d MiB when only the light job was re-measured", before, after)
	}
	key := store.JobUsageKey{Repo: "org/repo", Workflow: "ci", JobName: "heavy", PoolID: "pool"}
	// The heavy job has fewer than five runs of its own; the pool's figure is
	// what protects a runner that may take it.
	jobHistory := map[store.JobUsageKey][]store.JobPeak{key: peaks[5:9]}
	pool := &store.Pool{ID: "pool"}
	demand := map[string][]*store.Job{"pool": {{Repo: key.Repo, Workflow: key.Workflow, JobName: key.JobName}}}
	b := newBooker(BookingHistory, 25, &BookingInputs{PoolRuns: map[string][]store.JobPeak{"pool": store.PreferWorkingSet(peaks)}},
		jobHistory, []*store.Pool{pool}, demand)
	if b.unknown["pool"] < before {
		t.Fatalf("a runner that may take the queued heavy job books %d MiB, below the %d it booked before", b.unknown["pool"], before)
	}
	// A killed run survives the switch with its floor.
	peaks = append(peaks, store.JobPeak{Job: "heavy", MemoryMB: 8192, OOMKilled: true, GrantedMemoryMB: 8192})
	if got, _ := historyBooking(store.PreferWorkingSet(peaks), 25); got < before {
		t.Fatalf("the killed run's floor was lost: %d MiB", got)
	}
}

// Review of #20, blocker 2, the review's probe made permanent: a job whose new
// measurements are partial (readings with a gap: untrusted, -1) keeps the
// raise its old runs give it. Before the fix five such runs at 512 MiB took
// an 8 GiB job's raise need from 10,240 MiB to 640 MiB.
func TestPartialMeasurementsDoNotLowerAJobsRaise(t *testing.T) {
	var peaks []store.JobPeak
	for range 5 {
		peaks = append(peaks, store.JobPeak{MemoryMB: 8192, WorkingSetMB: -1})
	}
	for range 15 {
		peaks = append(peaks, store.JobPeak{MemoryMB: 8192})
	}
	before, _ := raiseNeedMB(peaks, 25)
	after, _ := raiseNeedMB(store.PreferWorkingSet(peaks), 25)
	if before != 10240 || after != before {
		t.Fatalf("raise need %d MiB before, %d after; want 10,240 both", before, after)
	}
	if b, _ := historyBooking(store.PreferWorkingSet(peaks), 25); b < 10240 {
		t.Fatalf("booking fell to %d MiB on partial measurements", b)
	}
}

// With scheduler.working_set_history off the controller hands scheduling the
// store's history untouched: vision.9's rows (store.TestTheHistoriesAreVision9s...)
// plus the two fields the switch alone reads. Nothing here reads them: history
// booking, the raise need, a profile, and a whole booker give the same answers
// for a mixed history with them and without them.
func TestTheWorkingSetFieldsMoveNoSchedulingFigure(t *testing.T) {
	var with []store.JobPeak
	for i := range 8 {
		with = append(with, store.JobPeak{CPUs: 2, MemoryMB: int64(8000 + i), Job: "heavy"})
	}
	for i := range 6 {
		with = append(with, store.JobPeak{CPUs: 1, MemoryMB: int64(1500 + i), WorkingSetMB: 512, Job: "light"})
	}
	for i := range 4 {
		with = append(with, store.JobPeak{CPUs: 4, MemoryMB: int64(9000 + i), WorkingSetMB: -1, Job: "heavy"})
	}
	with = append(with, store.JobPeak{CPUs: 4, MemoryMB: 7000, OOMKilled: true, GrantedMemoryMB: 12288, WorkingSetMB: 6000, Job: "heavy"})
	without := make([]store.JobPeak, len(with))
	for i, p := range with {
		p.WorkingSetMB, p.Job = 0, ""
		without[i] = p
	}
	for _, headroom := range []int{0, 25} {
		a, ka := historyBooking(with, headroom)
		b, kb := historyBooking(without, headroom)
		if a != b || ka != kb {
			t.Fatalf("historyBooking(%d): %d/%v with the fields, %d/%v without", headroom, a, ka, b, kb)
		}
		a, ka = raiseNeedMB(with, headroom)
		b, kb = raiseNeedMB(without, headroom)
		if a != b || ka != kb {
			t.Fatalf("raiseNeedMB(%d): %d/%v with the fields, %d/%v without", headroom, a, ka, b, kb)
		}
	}
	for _, q := range []float64{0.5, 0.9} {
		a, ka := expectedPeak(with, q)
		b, kb := expectedPeak(without, q)
		if a != b || ka != kb {
			t.Fatalf("expectedPeak(%v): %d/%v with, %d/%v without", q, a, ka, b, kb)
		}
	}
	if a, b := Profile(with), Profile(without); a != b {
		t.Fatalf("Profile: %+v with the fields, %+v without", a, b)
	}
	pool := limited("zoomies-pr-browser", 8, 32*gb)
	h := nlAgent("ci-nl-01-pr", "", 49*gb)
	r := usingMB("run_a", pool, h, 3*gb)
	key := store.JobUsageKey{Repo: "o/r", Workflow: "ci", JobName: "heavy", PoolID: pool.ID}
	demand := map[string][]*store.Job{pool.ID: {{Repo: "o/r", Workflow: "ci", JobName: "heavy"}}}
	book := func(peaks []store.JobPeak) (int64, int64, int64) {
		in := &BookingInputs{PoolRuns: map[string][]store.JobPeak{pool.ID: peaks}, RunnerJobs: map[string]store.JobUsageKey{r.ID: key}}
		b := newBooker(BookingHistory, 25, in, map[store.JobUsageKey][]store.JobPeak{key: peaks}, []*store.Pool{pool}, demand)
		booked, _, _ := b.runner(pool, h, r)
		return b.unknown[pool.ID], booked, b.typicalStart(pool, 32*gb, nil)
	}
	u1, r1, s1 := book(with)
	u2, r2, s2 := book(without)
	if u1 != u2 || r1 != r2 || s1 != s2 {
		t.Fatalf("booker: %d/%d/%d with the fields, %d/%d/%d without", u1, r1, s1, u2, r2, s2)
	}
}

// Review 3 of #20, finding 4, through the booking and history_raise this
// history feeds: fifteen recent heavy untrusted runs beside five light
// measured ones book and raise exactly as they did before the switch.
func TestRecentUntrustedHeavyRunsStillBookAfterTheSwitch(t *testing.T) {
	var runs []store.JobPeak
	for range 15 {
		runs = append(runs, store.JobPeak{Job: "build", MemoryMB: 8192, WorkingSetMB: -1})
	}
	for range 5 {
		runs = append(runs, store.JobPeak{Job: "build", MemoryMB: 512, WorkingSetMB: 512, WorkingSetFinal: true, WorkflowPath: ".github/workflows/ci.yml"})
	}
	before, _ := historyBooking(runs, 25)
	beforeRaise, _ := raiseNeedMB(runs, 25)
	after, _ := historyBooking(store.PreferWorkingSet(runs), 25)
	afterRaise, _ := raiseNeedMB(store.PreferWorkingSet(runs), 25)
	if after < before || afterRaise < beforeRaise {
		t.Fatalf("booking %d -> %d MiB, history_raise %d -> %d MiB: the untrusted heavy runs were dropped", before, after, beforeRaise, afterRaise)
	}
}
