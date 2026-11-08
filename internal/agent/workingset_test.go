package agent

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/eyupio/zoomies/internal/backend"
	"github.com/eyupio/zoomies/internal/store"
)

// workingSetOf is the working set a report carries: its peak in MiB and
// whether it is incomplete; -1 MiB for none at all.
func workingSetOf(rep RunnerReport) (int64, bool) {
	if rep.WorkingSet == nil {
		return -1, false
	}
	return rep.WorkingSet.PeakBytes >> 20, rep.WorkingSet.Incomplete
}

// Re-review of #20, blocker 2: every report of a runner carries its kept peak,
// whatever made it (a live reconcile, the terminal report, the cleanup
// confirmation, a heartbeat), and the runner is forgotten only after a report
// carrying it was accepted. The review's probe: an 8 GiB peak caught between
// full samples was carried by no reconcile report.
func TestEveryReportCarriesTheKeptPeak(t *testing.T) {
	a, _, be, clock := newAgent(t, 1)
	r := track(a, "r", "h", true)
	now := clock.Now()
	r.phase, r.runningSince = backend.PhaseRunning, now
	r.stats = backend.Stats{MemoryBytes: 512 << 20, WorkingSetBytes: 512 << 20, SampledAt: &now}
	r.noteWorkingSet(8<<30, now)

	be.setWorkloads(running("h", "r"))
	reps, err := a.ReconcileOnce(context.Background())
	if err != nil || len(reps) != 1 {
		t.Fatalf("live reconcile: %v %v", reps, err)
	}
	if mb, inc := workingSetOf(reps[0]); mb != 8192 || inc {
		t.Fatalf("live reconcile carried %d MiB (incomplete %v), want 8192", mb, inc)
	}
	if hb := a.Runners(); len(hb) != 1 || hb[0].WorkingSet == nil || hb[0].WorkingSet.PeakBytes != 8<<30 {
		t.Fatalf("the heartbeat's report carried %+v", hb)
	}

	be.setWorkloads(exited("h", "r", 0))
	reps, err = a.ReconcileOnce(context.Background())
	if err != nil || len(reps) != 1 || !reps[0].State.Terminal() {
		t.Fatalf("terminal reconcile: %v %v", reps, err)
	}
	if mb, _ := workingSetOf(reps[0]); mb != 8192 {
		t.Fatalf("the terminal report carried %d MiB, want 8192", mb)
	}
	a.markReported(reps)
	reps, err = a.ReconcileOnce(context.Background())
	if err != nil || len(reps) != 1 {
		t.Fatalf("cleanup: %v %v", reps, err)
	}
	if mb, _ := workingSetOf(reps[0]); mb != 8192 {
		t.Fatalf("the cleanup confirmation carried %d MiB, want 8192", mb)
	}
	a.markReported(reps)
	if _, err := a.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := len(a.Runners()); n != 0 {
		t.Fatalf("%d runners still tracked after the cleanup was acknowledged", n)
	}
}

// The report of a workload that vanished carries the kept peak too.
func TestAVanishedWorkloadsReportCarriesThePeak(t *testing.T) {
	a, _, _, clock := newAgent(t, 1)
	r := track(a, "r", "h", true)
	now := clock.Now()
	r.phase, r.runningSince, r.createdAt = backend.PhaseRunning, now, now.Add(-time.Hour)
	r.noteWorkingSet(8<<30, now)
	reps, _ := a.ReconcileOnce(context.Background())
	if len(reps) != 1 || reps[0].State != store.RunnerRemoved {
		t.Fatalf("reports = %+v, want the vanished runner's", reps)
	}
	if mb, _ := workingSetOf(reps[0]); mb != 8192 {
		t.Fatalf("the vanished workload's report carried %d MiB, want 8192", mb)
	}
}

// Re-review of #20, blocker 2: a failed reading marks the runner incomplete for
// good, and the live reconcile report says so. The review's probe: the live
// report said complete, and the controller stored a trusted 512 MiB. The JSON
// is what the controller reads (controller.TestAReportsWorkingSet...).
func TestAFailedReadingReachesTheReportAsIncomplete(t *testing.T) {
	a, _, be, clock := newAgent(t, 1)
	r := track(a, "r", "h", true)
	now := clock.Now()
	r.phase, r.runningSince = backend.PhaseRunning, now
	r.stats = backend.Stats{MemoryBytes: 8192 << 20, WorkingSetBytes: 512 << 20, SampledAt: &now}
	r.noteWorkingSet(512<<20, now)
	r.noteWorkingSet(readingFailed, now.Add(time.Second))
	be.setWorkloads(running("h", "r"))
	reps, err := a.ReconcileOnce(context.Background())
	if err != nil || len(reps) != 1 {
		t.Fatalf("reconcile: %v %v", reps, err)
	}
	if _, inc := workingSetOf(reps[0]); !inc {
		t.Fatal("the reconcile report did not say the readings were incomplete")
	}
	raw, err := json.Marshal(reps[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"working_set":{"peak_bytes":536870912,"incomplete":true}`) {
		t.Fatalf("the report's JSON is %s", raw)
	}
}

// Re-review of #20, blocker 3: an adopted runner is incomplete, whatever order
// the first reading and the first observation come in: what an earlier agent
// read of it went with that agent. Both of the review's probes.
func TestAnAdoptedRunnerIsIncomplete(t *testing.T) {
	a, _, _, clock := newAgent(t, 1)
	now := clock.Now()
	w := running("h", "r")
	w.Status.StartedAt = now.Add(-time.Hour)
	a.adopt("r", store.BackendDocker, w)
	a.noteWorkingSet("r", "h", 512<<20, now) // the stats loop can run first
	a.markRunning("r", w, backend.Stats{}, now)
	if _, inc := workingSetOf(a.Runners()[0]); !inc {
		t.Fatal("an adopted runner read before its first observation was trusted")
	}

	a2, _, be, clock2 := newAgent(t, 1)
	now = clock2.Now()
	w = running("h", "r")
	w.Status.StartedAt = now.Add(-time.Hour)
	be.setWorkloads(w)
	be.stats = backend.Stats{MemoryBytes: 512 << 20, WorkingSetBytes: 512 << 20, Containers: []string{"h"}}
	a2.adopt("r", store.BackendDocker, w)
	a2.sampleStats(context.Background())
	clock2.advance(30 * time.Second)
	if _, err := a2.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, inc := workingSetOf(a2.Runners()[0]); !inc {
		t.Fatal("adoption, a full sample and a reconcile, in startup order, trusted the unobserved hour")
	}
}

// Re-review of #20, blocker 4: a runner's end is judged for a gap before it is
// marked terminal: up to its exit where the daemon says when, otherwise up to
// when the end was seen. The review's probe: 46 s without a reading, then the
// exit, stayed trusted.
func TestAGapBeforeTheEndMarksTheRunIncomplete(t *testing.T) {
	a, _, be, clock := newAgent(t, 1)
	r := track(a, "r", "h", true)
	now := clock.Now()
	r.phase, r.runningSince = backend.PhaseRunning, now
	r.noteWorkingSet(512<<20, now)
	clock.advance(46 * time.Second)
	be.setWorkloads(exited("h", "r", 0))
	if _, err := a.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, inc := workingSetOf(a.Runners()[0]); !inc {
		t.Fatal("46 s without a reading before the end was seen stayed trusted")
	}

	// Read 5 s before it exited, the end seen 40 s later: covered.
	a2, _, be2, clock2 := newAgent(t, 1)
	r2 := track(a2, "r", "h", true)
	now = clock2.Now()
	r2.phase, r2.runningSince = backend.PhaseRunning, now
	r2.noteWorkingSet(512<<20, now)
	clock2.advance(45 * time.Second)
	w := exited("h", "r", 0)
	w.Status.ExitedAt = now.Add(5 * time.Second)
	be2.setWorkloads(w)
	if _, err := a2.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, inc := workingSetOf(a2.Runners()[0]); inc {
		t.Fatal("a runner read 5 s before its exit was marked for the time until the exit was seen")
	}
	// And an ended runner is not judged again for the time since.
	clock2.advance(time.Hour)
	if _, inc := workingSetOf(a2.Runners()[0]); inc {
		t.Fatal("an ended runner was marked for the time since it ended")
	}
}

// A container that has stopped has nothing to read: its zero usage is not a
// failed reading, so a full sample between its exit and the agent noticing it
// does not throw away a well-covered run.
func TestAStoppedContainersReadingIsNotAFailure(t *testing.T) {
	start := time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)
	r := &tracked{runnerID: "r", handle: "h", phase: backend.PhaseRunning, runningSince: start}
	r.noteWorkingSet(2<<30, start.Add(10*time.Second))
	r.noteWorkingSet(fullSampleReading(backend.Stats{}), start.Add(20*time.Second))
	if rep := r.workingSetReport(start.Add(20 * time.Second)); rep == nil || rep.Incomplete || rep.PeakBytes != 2<<30 {
		t.Fatalf("after a stopped container's sample: %+v", rep)
	}
	if got := fullSampleReading(backend.Stats{MemoryBytes: 1 << 30}); got != readingFailed {
		t.Fatalf("usage with no working set read as %d, want a failed reading", got)
	}
}

// Gaps between readings, at report time, and failed readings each mark a run
// incomplete for good; readings every 10 s keep it complete.
func TestAGapOrAFailedReadingMarksThePeakIncomplete(t *testing.T) {
	start := time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)
	fresh := func() *tracked {
		return &tracked{runnerID: "r", handle: "h", phase: backend.PhaseRunning, runningSince: start}
	}
	r := fresh()
	for i := range 10 {
		r.noteWorkingSet(1<<30, start.Add(time.Duration(i+1)*10*time.Second))
	}
	if rep := r.workingSetReport(start.Add(110 * time.Second)); rep == nil || rep.Incomplete || rep.PeakBytes != 1<<30 {
		t.Fatalf("readings every 10 s: %+v", rep)
	}
	r = fresh()
	r.noteWorkingSet(1<<30, start.Add(10*time.Second))
	r.noteWorkingSet(1<<30, start.Add(10*time.Second+workingSetMaxGap+time.Second))
	if rep := r.workingSetReport(start.Add(2 * time.Minute)); !rep.Incomplete {
		t.Fatal("a gap past workingSetMaxGap between readings was not marked")
	}
	r = fresh()
	r.noteWorkingSet(1<<30, start.Add(10*time.Second))
	if rep := r.workingSetReport(start.Add(10*time.Second + workingSetMaxGap + time.Second)); !rep.Incomplete {
		t.Fatal("a running runner unread since its last reading was not marked at report time")
	}
	r = fresh()
	if rep := r.workingSetReport(start.Add(workingSetMaxGap + time.Second)); rep == nil || !rep.Incomplete {
		t.Fatal("a running runner never read was not marked")
	}
}

// A full sample's reading is kept for the runner's whole life.
func TestAFullSamplesPeakIsKept(t *testing.T) {
	a, _, b, clock := newAgent(t, 1)
	a.runners["r"] = &tracked{runnerID: "r", handle: "h", kind: store.BackendDocker, phase: backend.PhaseRunning, runningSince: clock.Now()}
	b.stats = backend.Stats{MemoryBytes: 8 << 30, WorkingSetBytes: 8 << 30}
	a.sampleStats(context.Background())
	for range 4 {
		clock.advance(30 * time.Second)
		b.stats = backend.Stats{MemoryBytes: 512 << 20, WorkingSetBytes: 512 << 20}
		a.sampleStats(context.Background())
	}
	if mb, inc := workingSetOf(a.Runners()[0]); mb != 8192 || inc {
		t.Fatalf("after two minutes of 512 MiB samples: %d MiB, incomplete %v; want 8192, complete", mb, inc)
	}
}

// A runner's Stats are what they were before the working set was kept: the
// kept peak and the verdict travel beside them, never in them.
func TestTheReportsStatsCarryNothingNew(t *testing.T) {
	now := time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)
	r := &tracked{runnerID: "r", handle: "h", phase: backend.PhaseRunning, runningSince: now,
		stats: backend.Stats{SampledAt: &now, MemoryBytes: 512 << 20}}
	r.noteWorkingSet(8<<30, now)
	raw, err := json.Marshal(r.report().Stats)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"peak", "incomplete"} {
		if strings.Contains(string(raw), key) {
			t.Fatalf("the report's stats carry %q: %s", key, raw)
		}
	}
}

type workingSetBackend struct {
	*valveBackend
	reads   atomic.Int64
	slow    bool
	entered chan struct{}
}

func (b *workingSetBackend) MemoryUsage(ctx context.Context, c string) (backend.MemoryReading, error) {
	b.reads.Add(1)
	if b.entered != nil {
		select {
		case b.entered <- struct{}{}:
		default:
		}
	}
	if b.slow {
		<-ctx.Done()
		return backend.MemoryReading{}, ctx.Err()
	}
	switch c {
	case "unknown":
		return backend.MemoryReading{UsageBytes: 512 << 20}, nil
	case "stopped":
		return backend.MemoryReading{}, nil
	}
	return backend.MemoryReading{UsageBytes: 600 << 20, WorkingSetBytes: 512 << 20}, nil
}

func workingSetSampler(t *testing.T, b *workingSetBackend, bytes int64, containers []string) *Agent {
	t.Helper()
	a, _, _, clock := newAgent(t, 10, func(o *Options) { o.Backends = backend.NewRegistry(b) })
	a.runners["r"] = &tracked{runnerID: "r", handle: "h", kind: store.BackendDocker, phase: backend.PhaseRunning,
		runningSince: clock.Now(), stats: backend.Stats{MemoryBytes: bytes, Containers: containers}}
	return a
}

// Every running runner is read, however little its last full sample used, and
// half a pair is not a reading: the runner is marked incomplete. Every one of
// a pair's containers stopped is nothing to read.
func TestEveryRunningRunnerIsReadAndHalfAPairIsNot(t *testing.T) {
	b := &workingSetBackend{valveBackend: newValveBackend()}
	a := workingSetSampler(t, b, 40<<20, []string{"runner"})
	a.sampleWorkingSets(context.Background())
	if got := b.reads.Load(); got != 1 {
		t.Fatalf("a running runner last seen at 40 MiB was read %d times, want 1", got)
	}
	if got := a.runners["r"].workingSetPeak; got != 512<<20 {
		t.Fatalf("peak = %d MiB, want the 512 MiB read", got>>20)
	}
	for _, half := range []string{"unknown", "stopped"} {
		a := workingSetSampler(t, b, 40<<20, []string{"runner", half})
		a.sampleWorkingSets(context.Background())
		if r := a.runners["r"]; r.workingSetPeak != 0 || !r.workingSetIncomplete {
			t.Fatalf("half a pair (%s): peak %d, incomplete %v; want no peak and incomplete", half, r.workingSetPeak, r.workingSetIncomplete)
		}
	}
	a = workingSetSampler(t, b, 40<<20, []string{"stopped", "stopped"})
	a.sampleWorkingSets(context.Background())
	if r := a.runners["r"]; r.workingSetPeak != 0 || r.workingSetIncomplete {
		t.Fatalf("a stopped pair: peak %d, incomplete %v; want nothing recorded", r.workingSetPeak, r.workingSetIncomplete)
	}
}

// The controller can stop the readings live, through the heartbeat: the loop
// skips its real 10-second ticks while the answer says off, and the next
// heartbeat that says on resumes them (review 3 of #20, finding 7: the first
// version of this test waited 50 ms against a 10-second ticker, and passed
// with the off check removed).
func TestTheReadingsStopAndResumeWhenTheControllerSaysSo(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := &workingSetBackend{valveBackend: newValveBackend()}
		a := workingSetSampler(t, b, 512<<20, []string{"runner"})
		tr := a.tr.(*fakeTransport)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _ = a.workingSetLoop(ctx); close(done) }()
		tick := func(n int) {
			time.Sleep(time.Duration(n)*workingSetInterval + time.Second)
			synctest.Wait()
		}
		say := func(off bool) {
			tr.mu.Lock()
			tr.beatResp.WorkingSetSamplingOff = off
			tr.mu.Unlock()
			if err := a.heartbeat(ctx); err != nil {
				t.Fatal(err)
			}
		}
		tick(1)
		before := b.reads.Load()
		if before != 1 {
			t.Fatalf("first tick read %d times, want 1", before)
		}
		say(true)
		tick(3)
		if got := b.reads.Load(); got != before {
			t.Fatalf("%d readings over three ticks with sampling off", got-before)
		}
		say(false)
		tick(1)
		if got := b.reads.Load(); got != before+1 {
			t.Fatalf("after the controller said on: %d readings, want 1", got-before)
		}
		cancel()
		<-done
	})
}

// A slow daemon does not hold the agent's lock: reports go out while the
// sampler waits, and it stops when asked.
func TestASlowWorkingSetReadDoesNotHoldReports(t *testing.T) {
	b := &workingSetBackend{valveBackend: newValveBackend(), slow: true, entered: make(chan struct{}, 1)}
	a := workingSetSampler(t, b, 512<<20, []string{"runner"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { a.sampleWorkingSets(ctx); close(done) }()
	select {
	case <-b.entered:
	case <-time.After(time.Second):
		t.Fatal("the reader did not start")
	}
	reported := make(chan struct{})
	go func() { a.Runners(); close(reported) }()
	select {
	case <-reported:
	case <-time.After(time.Second):
		t.Fatal("a slow reading blocked the report snapshot")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("the sampler ignored cancellation")
	}
}
