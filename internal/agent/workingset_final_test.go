package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/eyupio/zoomies/internal/backend"
)

// Review 3 of #20, finding 1: a remove task took the runner off the host and
// untracked it, and its result carried no working set, so the peak was gone
// before any report had carried it. The result now carries the final peak and
// verdict, and the agent keeps them until a result carrying them is accepted:
// the task the controller redelivers after a lost result finds the workload
// gone, and its answer carries the same working set.
func TestARemoveTaskCarriesTheFinalWorkingSetUntilItIsAccepted(t *testing.T) {
	a, tr, be, clock := newAgent(t, 1)
	r := track(a, "r", "h", true)
	r.phase = backend.PhaseRunning
	r.runningSince = clock.Now()
	r.noteWorkingSet(8<<30, clock.Now())
	be.setWorkloads(running("h", "r"))
	tr.resultErr = errors.New("offline")

	a.handleRemove(context.Background(), Task{ID: "remove", RunnerID: "r", Kind: TaskRemoveRunner}, func() {})
	res := <-tr.results
	if ws := res.WorkingSet; ws == nil || ws.PeakBytes != 8<<30 || ws.Incomplete || !ws.Final {
		t.Fatalf("the remove result carries %+v, want the 8 GiB peak, complete and final", ws)
	}
	if len(a.Runners()) != 0 {
		t.Fatal("the removed runner is still tracked")
	}

	// The result was lost; the redelivered task finds nothing to remove.
	be.setWorkloads()
	tr.mu.Lock()
	tr.resultErr = nil
	tr.mu.Unlock()
	a.handleRemove(context.Background(), Task{ID: "remove", RunnerID: "r", Kind: TaskRemoveRunner}, func() {})
	res = <-tr.results
	if ws := res.WorkingSet; ws == nil || ws.PeakBytes != 8<<30 || !ws.Final {
		t.Fatalf("the redelivered remove carries %+v, want the kept final peak", ws)
	}
	// Accepted: forgotten.
	a.handleRemove(context.Background(), Task{ID: "remove", RunnerID: "r", Kind: TaskRemoveRunner}, func() {})
	if res := <-tr.results; res.WorkingSet != nil {
		t.Fatalf("an accepted working set was sent again: %+v", res.WorkingSet)
	}
}

// A runner removed while it runs is judged up to its removal: a gap since its
// last reading makes the final verdict incomplete.
func TestARemovedRunnersGapIsJudgedAtItsRemoval(t *testing.T) {
	a, tr, be, clock := newAgent(t, 1)
	r := track(a, "r", "h", true)
	r.phase = backend.PhaseRunning
	r.runningSince = clock.Now()
	r.noteWorkingSet(2<<30, clock.Now())
	be.setWorkloads(running("h", "r"))
	clock.advance(workingSetMaxGap + time.Second)
	a.handleRemove(context.Background(), Task{ID: "remove", RunnerID: "r", Kind: TaskRemoveRunner}, func() {})
	if ws := (<-tr.results).WorkingSet; ws == nil || !ws.Incomplete || !ws.Final {
		t.Fatalf("the remove result carries %+v, want an incomplete final verdict", ws)
	}
}

// Kept final working sets of removed runners are bounded in time.
func TestAKeptRemovedWorkingSetExpires(t *testing.T) {
	a, _, _, clock := newAgent(t, 1)
	a.removedWorkingSets["old"] = removedWorkingSet{report: &WorkingSetReport{PeakBytes: 1}, at: clock.Now()}
	clock.advance(removedWorkingSetKeep + time.Second)
	if ws := a.removeWorkingSet("other", clock.Now()); ws != nil || len(a.removedWorkingSets) != 0 {
		t.Fatalf("an expired kept working set survived: %+v %v", ws, a.removedWorkingSets)
	}
}

// Review 3 of #20, finding 3: a new runner read before it was first observed
// running installed its first reading as the last one, and the interval from
// the container's start to that reading was never checked. It is now, once
// the start is known.
func TestAFirstReadingBeforeTheFirstObservationIsCheckedAgainstTheStart(t *testing.T) {
	for _, tc := range []struct {
		name       string
		delay      time.Duration
		incomplete bool
	}{
		{"read 60 s after the start", 60 * time.Second, true},
		{"read 20 s after the start", 20 * time.Second, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, _, be, clock := newAgent(t, 1)
			start := clock.Now()
			a.handleCreate(context.Background(), createTask("create", "r"), func() {})
			r, ok := a.snapshot("r")
			if !ok {
				t.Fatal("create failed")
			}
			clock.advance(tc.delay)
			w := running(r.handle, "r")
			w.Status.StartedAt = start
			be.setWorkloads(w)
			be.stats = backend.Stats{MemoryBytes: 8192 << 20, WorkingSetBytes: 512 << 20, Containers: []string{string(r.handle)}}
			a.sampleStats(context.Background())
			reps, err := a.ReconcileOnce(context.Background())
			if err != nil || len(reps) != 1 {
				t.Fatalf("reconcile: %v %v", reps, err)
			}
			peak, incomplete := workingSetOf(reps[0])
			if peak != 512 || incomplete != tc.incomplete {
				t.Fatalf("peak %d MiB, incomplete %v; want 512, %v", peak, incomplete, tc.incomplete)
			}
		})
	}
}

// A start the daemon did not give cannot be checked against: a reading taken
// before it leaves the run incomplete.
func TestAnUnknownStartAfterAReadingIsIncomplete(t *testing.T) {
	now := time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)
	r := &tracked{runnerID: "r", handle: "h", phase: backend.PhaseRunning}
	r.noteWorkingSet(1<<30, now)
	r.startWorkingSet(time.Time{}, now.Add(time.Second))
	if !r.workingSetIncomplete {
		t.Fatal("a reading before an unknown start was taken as covered")
	}
	// With no reading yet the start is still unknown: whatever ran between
	// the container's real start and now went unread, as far as anyone can
	// tell, so the baseline cannot be established (review of #22, finding 1).
	fresh := &tracked{runnerID: "r", handle: "h", phase: backend.PhaseRunning}
	fresh.startWorkingSet(time.Time{}, now)
	if !fresh.workingSetIncomplete || !fresh.runningSince.Equal(now) {
		t.Fatalf("an unknown start with no reading yet was taken as covered: %+v", fresh)
	}
}

// Review of #22 (abaa0f24), finding 1: a runner whose end was the first thing
// the agent saw of it, or that a remove task took before it was first observed
// running, bypassed markRunning, so its start-to-first-reading interval was
// never checked, and a 60-second blind start went out as a complete, final
// 512 MiB peak. The baseline is now established and checked before every
// final verdict. These are the review's probes, through the real create,
// sample, reconcile and remove paths, in both lifecycle orderings.
func TestTheStartIsCheckedBeforeEveryFinalVerdict(t *testing.T) {
	// created, read, then its end is the first thing observed of it
	firstSeenEnded := func(t *testing.T, delay time.Duration, knownStart bool) *WorkingSetReport {
		a, _, be, clock := newAgent(t, 1)
		start := clock.Now()
		a.handleCreate(context.Background(), createTask("create", "r"), func() {})
		r, ok := a.snapshot("r")
		if !ok {
			t.Fatal("create failed")
		}
		clock.advance(delay)
		w := running(r.handle, "r")
		be.setWorkloads(w)
		be.stats = backend.Stats{MemoryBytes: 8192 << 20, WorkingSetBytes: 512 << 20, Containers: []string{string(r.handle)}}
		a.sampleStats(context.Background())
		clock.advance(5 * time.Second)
		end := exited(r.handle, "r", 0)
		if knownStart {
			end.Status.StartedAt = start
		}
		end.Status.ExitedAt = clock.Now()
		be.setWorkloads(end)
		reps, err := a.ReconcileOnce(context.Background())
		if err != nil || len(reps) != 1 {
			t.Fatalf("reconcile: %v %v", reps, err)
		}
		return reps[0].WorkingSet
	}
	// created, read, then removed by a task before it was first observed
	removedUnseen := func(t *testing.T, delay time.Duration) *WorkingSetReport {
		a, tr, be, clock := newAgent(t, 1)
		a.handleCreate(context.Background(), createTask("create", "r"), func() {})
		r, ok := a.snapshot("r")
		if !ok {
			t.Fatal("create failed")
		}
		clock.advance(delay)
		be.setWorkloads(running(r.handle, "r"))
		be.stats = backend.Stats{MemoryBytes: 8192 << 20, WorkingSetBytes: 512 << 20, Containers: []string{string(r.handle)}}
		a.sampleStats(context.Background())
		a.handleRemove(context.Background(), Task{ID: "remove", RunnerID: "r", Kind: TaskRemoveRunner}, func() {})
		return removeResult(t, tr).WorkingSet
	}
	// created, read, then gone from the host before it was first observed
	vanishedUnseen := func(t *testing.T) *WorkingSetReport {
		a, _, be, clock := newAgent(t, 1)
		a.handleCreate(context.Background(), createTask("create", "r"), func() {})
		r, ok := a.snapshot("r")
		if !ok {
			t.Fatal("create failed")
		}
		clock.advance(10 * time.Second)
		be.setWorkloads(running(r.handle, "r"))
		be.stats = backend.Stats{MemoryBytes: 8192 << 20, WorkingSetBytes: 512 << 20, Containers: []string{string(r.handle)}}
		a.sampleStats(context.Background())
		be.setWorkloads()
		clock.advance(missingGrace + time.Second)
		reps, err := a.ReconcileOnce(context.Background())
		if err != nil || len(reps) != 1 {
			t.Fatalf("reconcile: %v %v", reps, err)
		}
		return reps[0].WorkingSet
	}

	for _, tc := range []struct {
		name       string
		ws         func(t *testing.T) *WorkingSetReport
		incomplete bool
	}{
		{"first seen ended, read 60 s after its start", func(t *testing.T) *WorkingSetReport { return firstSeenEnded(t, 60*time.Second, true) }, true},
		{"first seen ended, read 20 s after its start", func(t *testing.T) *WorkingSetReport { return firstSeenEnded(t, 20*time.Second, true) }, false},
		{"first seen ended, its start unknown", func(t *testing.T) *WorkingSetReport { return firstSeenEnded(t, 20*time.Second, false) }, true},
		{"removed before it was first seen, read 60 s in", func(t *testing.T) *WorkingSetReport { return removedUnseen(t, 60*time.Second) }, true},
		{"removed before it was first seen, read 5 s in", func(t *testing.T) *WorkingSetReport { return removedUnseen(t, 5*time.Second) }, true},
		{"gone from the host before it was first seen", vanishedUnseen, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ws := tc.ws(t)
			if ws == nil || !ws.Final || ws.PeakBytes != 512<<20 || ws.Incomplete != tc.incomplete {
				t.Fatalf("final working set %+v; want the 512 MiB peak, final, incomplete %v", ws, tc.incomplete)
			}
		})
	}
}

// The same holds for a runner that was observed running: its baseline was
// established then, and the final verdict judges the end. A runner observed
// running without a start from the daemon was never covered from its start.
func TestARunnerObservedWithoutAStartIsNeverAFinalMeasurement(t *testing.T) {
	a, tr, be, clock := newAgent(t, 1)
	a.handleCreate(context.Background(), createTask("create", "r"), func() {})
	r, _ := a.snapshot("r")
	be.setWorkloads(running(r.handle, "r")) // no StartedAt
	if _, err := a.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	be.stats = backend.Stats{MemoryBytes: 1 << 30, WorkingSetBytes: 256 << 20, Containers: []string{string(r.handle)}}
	clock.advance(5 * time.Second)
	a.sampleStats(context.Background())
	a.handleRemove(context.Background(), Task{ID: "remove", RunnerID: "r", Kind: TaskRemoveRunner}, func() {})
	if ws := removeResult(t, tr).WorkingSet; ws == nil || !ws.Final || !ws.Incomplete {
		t.Fatalf("final working set %+v; want an incomplete final verdict", ws)
	}
}

// A docker-in-docker pair between its runner's exit and its removal: the
// runner container has stopped and the sidecar is still up. That is the end
// of the job, not a failed reading, in a fast reading and a full sample alike
// (the live controller had 174 of 241 pair runs untrusted on 8 Nov 2026,
// against 13 of 240 single-container runs). A sidecar gone from under a
// running runner is still a failure.
func TestAPairWhoseRunnerHasStoppedIsTheEndNotAFailure(t *testing.T) {
	b := &workingSetBackend{valveBackend: newValveBackend()}
	a := workingSetSampler(t, b, 40<<20, []string{"stopped", "sidecar"})
	a.runners["r"].noteWorkingSet(1<<30, a.now())
	a.sampleWorkingSets(context.Background())
	if r := a.runners["r"]; r.workingSetIncomplete || r.workingSetPeak != 1<<30 {
		t.Fatalf("a pair whose runner stopped: peak %d, incomplete %v; want the earlier peak, complete", r.workingSetPeak, r.workingSetIncomplete)
	}
	if got := b.reads.Load(); got != 1 {
		t.Fatalf("the sidecar of a stopped runner was read too: %d reads", got)
	}

	sidecarUp := backend.Stats{MemoryBytes: 300 << 20, Halves: &backend.PairHalves{
		Daemon: backend.HalfUse{MemoryBytes: 300 << 20, WorkingSetBytes: 200 << 20}}}
	if got := fullSampleReading(sidecarUp); got != readingNone {
		t.Fatalf("a full sample of a pair whose runner stopped read as %d, want no reading", got)
	}
	runnerUp := backend.Stats{MemoryBytes: 300 << 20, Halves: &backend.PairHalves{
		Runner: backend.HalfUse{MemoryBytes: 100 << 20, WorkingSetBytes: 80 << 20},
		Daemon: backend.HalfUse{MemoryBytes: 200 << 20}}}
	if got := fullSampleReading(runnerUp); got != readingFailed {
		t.Fatalf("a running pair with an unknown working set read as %d, want a failed reading", got)
	}
}

// Only a runner whose end has been judged carries a final verdict: an earlier
// report, however good, may yet be followed by a gap.
func TestOnlyAJudgedEndIsFinal(t *testing.T) {
	a, _, be, clock := newAgent(t, 1)
	r := track(a, "r", "h", true)
	r.phase = backend.PhaseRunning
	r.runningSince = clock.Now()
	r.noteWorkingSet(1<<30, clock.Now())
	be.setWorkloads(running("h", "r"))
	reps, _ := a.ReconcileOnce(context.Background())
	if len(reps) != 1 || reps[0].WorkingSet == nil || reps[0].WorkingSet.Final {
		t.Fatalf("a running runner's report: %+v", reps)
	}
	be.setWorkloads(exited("h", "r", 0))
	reps, _ = a.ReconcileOnce(context.Background())
	if len(reps) != 1 || reps[0].WorkingSet == nil || !reps[0].WorkingSet.Final || reps[0].WorkingSet.Incomplete {
		t.Fatalf("an exited runner's report: %+v", reps[0].WorkingSet)
	}
}

// removeResult is the remove task's result, past the create's.
func removeResult(t *testing.T, tr *fakeTransport) TaskResult {
	t.Helper()
	for {
		select {
		case res := <-tr.results:
			if res.TaskID == "remove" {
				return res
			}
		default:
			t.Fatal("the remove task sent no result")
		}
	}
}
