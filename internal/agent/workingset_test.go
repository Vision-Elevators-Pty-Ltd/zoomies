package agent

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eyupio/zoomies/internal/backend"
	"github.com/eyupio/zoomies/internal/store"
)

// Review of #20, blocker 2: a peak a fast reading caught reaches every report,
// running or terminal, though no full sample has carried it. The review's
// probe: 8 GiB caught, 512 MiB in the last full sample, reported as 0.
func TestACaughtPeakReachesTheReportWithoutAFullSample(t *testing.T) {
	now := time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)
	r := &tracked{runnerID: "r", handle: backend.Handle("h"), phase: backend.PhaseRunning, runningSince: now,
		stats: backend.Stats{SampledAt: &now, MemoryBytes: 512 << 20, WorkingSetBytes: 512 << 20}}
	r.noteWorkingSet(512<<20, now)
	a := &Agent{runners: map[string]*tracked{"r": r}}
	a.noteWorkingSet("r", "h", 8<<30, now.Add(10*time.Second))
	for _, terminal := range []bool{false, true} {
		r.terminal = terminal
		got := r.reportAt(now.Add(20 * time.Second)).Stats
		if got.WorkingSetPeakBytes != 8<<30 {
			t.Errorf("terminal=%v: caught 8192 MiB, reported a peak of %d MiB", terminal, got.WorkingSetPeakBytes>>20)
		}
		if got.WorkingSetIncomplete {
			t.Errorf("terminal=%v: a runner read every 10 s was reported incomplete", terminal)
		}
	}
	// The report the agent sends for a workload that vanished carries it too.
	a2, _, _, clock := newAgent(t, 1)
	a2.runners["r"] = r
	r.terminal = false
	r.createdAt = clock.Now().Add(-time.Hour)
	reports, _ := a2.ReconcileOnce(context.Background())
	found := false
	for _, rep := range reports {
		if rep.RunnerID == "r" {
			found = true
			if rep.State != store.RunnerRemoved || rep.Stats.WorkingSetPeakBytes != 8<<30 {
				t.Errorf("the vanished workload's report: state %q, peak %d MiB; want removed and the 8192 MiB caught", rep.State, rep.Stats.WorkingSetPeakBytes>>20)
			}
		}
	}
	if !found {
		t.Fatal("no report for the vanished workload")
	}
}

// Review of #20, blocker 2: a full sample's reading is kept as long as the
// fast ones, for the runner's whole life. The review's probe: an 8 GiB full
// sample was replaced by a 512 MiB one 30 s later.
func TestAFullSamplesPeakIsKept(t *testing.T) {
	a, _, b, clock := newAgent(t, 1)
	a.runners["r"] = &tracked{runnerID: "r", handle: "h", kind: store.BackendDocker, phase: backend.PhaseRunning, runningSince: clock.Now()}
	b.stats = backend.Stats{MemoryBytes: 8 << 30, WorkingSetBytes: 8 << 30}
	a.sampleStats(context.Background())
	if got := a.Runners()[0].Stats.WorkingSetPeakBytes; got != 8<<30 {
		t.Fatalf("first peak = %d MiB", got>>20)
	}
	for range 4 { // two minutes of smaller samples
		clock.advance(30 * time.Second)
		b.stats = backend.Stats{MemoryBytes: 512 << 20, WorkingSetBytes: 512 << 20}
		a.sampleStats(context.Background())
	}
	got := a.Runners()[0].Stats
	if got.WorkingSetPeakBytes != 8<<30 {
		t.Errorf("the 8192 MiB full sample was lost: peak %d MiB", got.WorkingSetPeakBytes>>20)
	}
	if got.WorkingSetIncomplete {
		t.Error("a runner sampled every 30 s was reported incomplete")
	}
}

// A peak counts as a measurement of the job only if the readings covered it:
// a gap of more than workingSetMaxGap while running, a failed or unknown
// reading, or a runner adopted long after its container started marks it
// incomplete, for good.
func TestAGapOrAFailedReadingMarksThePeakIncomplete(t *testing.T) {
	start := time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)
	fresh := func() *tracked {
		return &tracked{runnerID: "r", handle: "h", phase: backend.PhaseRunning, runningSince: start}
	}
	r := fresh()
	for i := range 10 {
		r.noteWorkingSet(1<<30, start.Add(time.Duration(i+1)*10*time.Second))
	}
	if rep := r.reportAt(start.Add(110 * time.Second)).Stats; rep.WorkingSetIncomplete || rep.WorkingSetPeakBytes != 1<<30 {
		t.Fatalf("readings every 10 s: %+v", rep)
	}
	r = fresh()
	r.noteWorkingSet(1<<30, start.Add(10*time.Second))
	r.noteWorkingSet(1<<30, start.Add(10*time.Second+workingSetMaxGap+time.Second))
	if !r.reportAt(start.Add(2 * time.Minute)).Stats.WorkingSetIncomplete {
		t.Fatal("a gap past workingSetMaxGap between readings was not marked")
	}
	r = fresh()
	r.noteWorkingSet(1<<30, start.Add(10*time.Second))
	if !r.reportAt(start.Add(10*time.Second + workingSetMaxGap + time.Second)).Stats.WorkingSetIncomplete {
		t.Fatal("a running runner unread since its last reading was not marked at report time")
	}
	r = fresh()
	r.noteWorkingSet(0, start.Add(5*time.Second)) // a failed or unknown reading
	r.noteWorkingSet(1<<30, start.Add(10*time.Second))
	if !r.reportAt(start.Add(15 * time.Second)).Stats.WorkingSetIncomplete {
		t.Fatal("a failed reading was not marked")
	}
	r = fresh()
	r.runningSince = start.Add(-time.Hour) // adopted: the container started an hour ago
	r.noteWorkingSet(1<<30, start)
	if !r.reportAt(start.Add(time.Second)).Stats.WorkingSetIncomplete {
		t.Fatal("a runner first read an hour after its start was not marked")
	}
	// An ended runner is judged by the readings it had, not by the time since.
	r = fresh()
	r.noteWorkingSet(1<<30, start.Add(10*time.Second))
	r.terminal = true
	if r.reportAt(start.Add(time.Hour)).Stats.WorkingSetIncomplete {
		t.Fatal("an ended runner was marked incomplete for the time since it ended")
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
	if c == "unknown" {
		return backend.MemoryReading{}, nil
	}
	return backend.MemoryReading{WorkingSetBytes: 512 << 20}, nil
}

func workingSetSampler(t *testing.T, b *workingSetBackend, bytes int64, containers []string) *Agent {
	t.Helper()
	a, _, _, clock := newAgent(t, 10, func(o *Options) { o.Backends = backend.NewRegistry(b) })
	a.runners["r"] = &tracked{runnerID: "r", handle: "h", kind: store.BackendDocker, phase: backend.PhaseRunning,
		runningSince: clock.Now(), stats: backend.Stats{MemoryBytes: bytes, Containers: containers}}
	return a
}

// Review of #20, should-fix 5: every running runner is read, however little
// its last full sample used, so a burst that starts between two of them is
// seen. Half a pair is not a reading: the runner is marked incomplete.
func TestEveryRunningRunnerIsReadAndHalfAPairIsNot(t *testing.T) {
	b := &workingSetBackend{valveBackend: newValveBackend()}
	a := workingSetSampler(t, b, 40<<20, []string{"runner"}) // an idle-looking runner
	a.sampleWorkingSets(context.Background())
	if got := b.reads.Load(); got != 1 {
		t.Fatalf("a running runner last seen at 40 MiB was read %d times, want 1", got)
	}
	if got := a.runners["r"].workingSetPeak; got != 512<<20 {
		t.Fatalf("peak = %d MiB, want the 512 MiB read", got>>20)
	}
	a.runners["r"].workingSetPeak = 0
	a.runners["r"].stats.Containers = []string{"runner", "unknown"}
	a.sampleWorkingSets(context.Background())
	if r := a.runners["r"]; r.workingSetPeak != 0 || !r.workingSetIncomplete {
		t.Fatalf("half a pair: peak %d, incomplete %v; want no peak and incomplete", r.workingSetPeak, r.workingSetIncomplete)
	}
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
