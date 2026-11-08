package agent

import (
	"context"
	"sync"
	"time"

	"github.com/eyupio/zoomies/internal/backend"
)

// A job's memory peak is what the controller books and raises runners by, and
// a 30-second stats sample misses a spike that comes and goes between two of
// them. The working set alone is read more often, for every running runner
// whose containers a full sample has named, with the one-shot reading the
// memory valve uses (no CPU interval: the daemon reads the cgroup once).
//
// Every reading, fast or full, raises a peak kept for the runner's whole life,
// and every report of the runner carries it (RunnerReport.WorkingSet, apart
// from Stats, so that nothing that reads a runner's sample sees anything new).
// The runner is ephemeral, so its life is its job; the controller does not
// trust the peak of one that is not.
//
// A peak is a measurement of the job only if the readings covered it. It is
// marked incomplete, for good, as soon as:
//   - more than workingSetMaxGap passes, while it runs, without a reading,
//     counted from the container's start and checked at every reading, every
//     report and when it ends (up to its exit, where the daemon says when);
//   - any reading fails or comes back unknown, half of a pair included;
//   - the runner was adopted by this agent rather than watched from its start.
//
// A reading of a container that has stopped (no usage at all) is not a
// reading: the runner has ended, and its end is judged by the readings it had.
//
// The readings run in a loop of their own, never holding the agent's lock
// over a call to the daemon, each call bounded by workingSetTimeout and each
// round by workingSetBudget, so a slow daemon costs a skipped round and
// nothing else: heartbeats, the memory valve and creates go on. The
// controller can stop them live (scheduler.working_set_sampling = off).
const (
	workingSetInterval = 10 * time.Second
	workingSetBudget   = 8 * time.Second
	workingSetTimeout  = 3 * time.Second
	workingSetWorkers  = 4
	// workingSetMaxGap is the longest a running runner may go without a
	// reading and still count as covered: four fast rounds, one and a half
	// full ones.
	workingSetMaxGap = 45 * time.Second
)

// The kinds of one reading of a runner's working set.
const (
	// readingNone is a runner with nothing to read: its container has
	// stopped. It changes nothing.
	readingNone int64 = 0
	// readingFailed is a reading that failed or came back unknown.
	readingFailed int64 = -1
)

func (a *Agent) workingSetLoop(ctx context.Context) error {
	ticker := time.NewTicker(workingSetInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		if a.workingSetSamplingOff.Load() {
			continue
		}
		a.sampleWorkingSets(ctx)
	}
}

// sampleWorkingSets reads the working set of every running runner whose last
// full sample named its containers, adding up a pair's two.
func (a *Agent) sampleWorkingSets(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, workingSetBudget)
	defer cancel()
	type job struct {
		runnerID   string
		handle     backend.Handle
		reader     backend.MemoryUpdater
		containers []string
	}
	var jobs []job
	a.mu.Lock()
	for id, r := range a.runners {
		if r.terminal || r.hostRemoved || r.handle == "" || r.phase != backend.PhaseRunning || len(r.stats.Containers) == 0 {
			continue
		}
		b, err := a.opts.Backends.Get(r.kind)
		if err != nil {
			continue
		}
		u, ok := b.(backend.MemoryUpdater)
		if !ok {
			continue
		}
		jobs = append(jobs, job{runnerID: id, handle: r.handle, reader: u, containers: append([]string(nil), r.stats.Containers...)})
	}
	a.mu.Unlock()

	work := make(chan job)
	var wg sync.WaitGroup
	for range workingSetWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range work {
				a.noteWorkingSet(j.runnerID, j.handle, readContainers(ctx, j.reader, j.containers), a.now())
			}
		}()
	}
	for _, j := range jobs {
		select {
		case work <- j:
		case <-ctx.Done():
		}
	}
	close(work)
	wg.Wait()
}

// readContainers is one reading of a runner's containers: their working sets
// added up; readingNone where every one has stopped; readingFailed where any
// call failed or any running container's working set is unknown.
func readContainers(ctx context.Context, u backend.MemoryUpdater, containers []string) int64 {
	var total int64
	stopped := 0
	for _, c := range containers {
		if ctx.Err() != nil {
			return readingFailed
		}
		rctx, stop := context.WithTimeout(ctx, workingSetTimeout)
		reading, err := u.MemoryUsage(rctx, c)
		stop()
		switch {
		case err != nil:
			return readingFailed
		case reading.UsageBytes <= 0 && reading.WorkingSetBytes <= 0:
			stopped++
		case reading.WorkingSetBytes <= 0:
			return readingFailed
		default:
			total += reading.WorkingSetBytes
		}
	}
	if stopped == len(containers) {
		return readingNone
	}
	if stopped > 0 {
		return readingFailed // half a pair
	}
	return total
}

// fullSampleReading is a full stats sample as a working-set reading.
func fullSampleReading(s backend.Stats) int64 {
	switch {
	case s.MemoryBytes <= 0 && s.WorkingSetBytes <= 0:
		return readingNone
	case s.WorkingSetBytes <= 0:
		return readingFailed
	default:
		return s.WorkingSetBytes
	}
}

// noteWorkingSet records one reading of a runner (see the reading kinds).
func (a *Agent) noteWorkingSet(runnerID string, handle backend.Handle, reading int64, now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	r := a.runners[runnerID]
	if r == nil || r.handle != handle {
		return
	}
	r.noteWorkingSet(reading, now)
}

// noteWorkingSet is the reading on a tracked runner, under the agent's lock.
// An ended runner takes no more readings: its end has been judged.
func (t *tracked) noteWorkingSet(reading int64, now time.Time) {
	if t.terminal || reading == readingNone {
		return
	}
	t.checkWorkingSetCoverage(now)
	if reading < 0 {
		t.workingSetIncomplete = true
		return
	}
	t.workingSetPeak = max(t.workingSetPeak, reading)
	t.workingSetLastAt = now
}

// checkWorkingSetCoverage marks the runner incomplete if more than
// workingSetMaxGap has passed by until since its last reading, or since its
// container started where there has been none.
func (t *tracked) checkWorkingSetCoverage(until time.Time) {
	since := t.workingSetLastAt
	if since.IsZero() {
		since = t.runningSince
	}
	if !since.IsZero() && until.Sub(since) > workingSetMaxGap {
		t.workingSetIncomplete = true
	}
}

// closeWorkingSet judges a runner's coverage at its end, before it is marked
// terminal: up to its exit where the daemon says when it was, otherwise up to
// now, when its end was seen. A runner that ran is never judged again after.
func (a *Agent) closeWorkingSet(runnerID string, handle backend.Handle, exitedAt, now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	r := a.runners[runnerID]
	if r == nil || r.handle != handle || r.terminal {
		return
	}
	until := now
	if !exitedAt.IsZero() && exitedAt.Before(now) {
		until = exitedAt
	}
	r.checkWorkingSetCoverage(until)
}

// workingSetReport is the working set a report of the runner carries, judged
// for a gap up to now while it still runs. Nil where nothing has been read and
// nothing has gone wrong yet.
func (t *tracked) workingSetReport(now time.Time) *WorkingSetReport {
	if !t.terminal && t.phase == backend.PhaseRunning {
		t.checkWorkingSetCoverage(now)
	}
	if t.workingSetPeak <= 0 && !t.workingSetIncomplete {
		return nil
	}
	return &WorkingSetReport{PeakBytes: t.workingSetPeak, Incomplete: t.workingSetIncomplete}
}

// attachWorkingSets puts each tracked runner's working set on its reports:
// every report the agent sends of a runner carries the peak and the verdict,
// whatever made the report, so none can be lost because the report that got
// through was a lifecycle one.
func (a *Agent) attachWorkingSets(reports []RunnerReport, now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for i := range reports {
		if r := a.runners[reports[i].RunnerID]; r != nil {
			reports[i].WorkingSet = r.workingSetReport(now)
		}
	}
}
