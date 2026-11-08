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
//   - its first reading came more than workingSetMaxGap after the container
//     started, checked once the start is known (a new runner can be read
//     before it is first observed), or the start never became known;
//   - any reading fails or comes back unknown, a running runner's sidecar
//     included;
//   - the runner was adopted by this agent rather than watched from its start.
//
// A reading of a runner whose runner container has stopped (no usage at all)
// is not a reading: the runner has ended, and its end is judged by the
// readings it had. That holds for a docker-in-docker pair whose sidecar is
// still up, which is every pair between its runner's exit and its removal.
//
// The verdict is final (WorkingSetReport.Final) once the runner's end has been
// judged: when its container is seen to have ended, or when a remove task
// takes it off the host, whose result carries it (the runner is then no
// longer tracked) and keeps it until that result is accepted.
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

// readContainers is one reading of a runner's containers, the runner first:
// their working sets added up; readingNone where the runner container has
// stopped, which is the end of its job whatever a sidecar is still doing;
// readingFailed where any call failed or any running container's working set
// is unknown, or a sidecar has stopped under a running runner.
func readContainers(ctx context.Context, u backend.MemoryUpdater, containers []string) int64 {
	var total int64
	for i, c := range containers {
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
			if i == 0 {
				return readingNone
			}
			return readingFailed // a sidecar gone from under a running runner
		case reading.WorkingSetBytes <= 0:
			return readingFailed
		default:
			total += reading.WorkingSetBytes
		}
	}
	if len(containers) == 0 {
		return readingNone
	}
	return total
}

// fullSampleReading is a full stats sample as a working-set reading. A pair
// whose runner half has stopped is no reading, as in readContainers.
func fullSampleReading(s backend.Stats) int64 {
	switch {
	case s.Halves != nil && s.Halves.Runner.MemoryBytes <= 0 && s.Halves.Runner.WorkingSetBytes <= 0:
		return readingNone
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
	if t.workingSetFirstAt.IsZero() {
		t.workingSetFirstAt = now
	}
	t.checkWorkingSetCoverage(now)
	if reading < 0 {
		t.workingSetIncomplete = true
		return
	}
	t.workingSetPeak = max(t.workingSetPeak, reading)
	t.workingSetLastAt = now
}

// startWorkingSet is the container's start, learnt when the runner is first
// observed running: coverage counts from it. A runner can be read before it is
// first observed, a new one included, so its first reading is checked against
// the start here; a start the daemon did not give cannot be checked against
// at all, and is now, which leaves any earlier reading unaccounted for.
func (t *tracked) startWorkingSet(startedAt, now time.Time) {
	if startedAt.IsZero() || startedAt.After(now) {
		t.runningSince = now
		if !t.workingSetFirstAt.IsZero() {
			t.workingSetIncomplete = true
		}
		return
	}
	t.runningSince = startedAt
	if !t.workingSetFirstAt.IsZero() && t.workingSetFirstAt.Sub(startedAt) > workingSetMaxGap {
		t.workingSetIncomplete = true
	}
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
	return &WorkingSetReport{PeakBytes: t.workingSetPeak, Incomplete: t.workingSetIncomplete, Final: t.terminal}
}

// removedWorkingSetKeep bounds how long the final working set of a removed
// runner waits for its remove task's result to be accepted: the controller
// redelivers an unanswered task long before, and a runner it has given up on
// is not worth keeping a peak for.
const removedWorkingSetKeep = time.Hour

type removedWorkingSet struct {
	report *WorkingSetReport
	at     time.Time
}

// removeWorkingSet is the final working set of a runner a remove task is
// taking off the host, judged up to now where its end was not seen, and kept
// (removedWorkingSets) until reportRemoved has had it accepted. A runner that
// is no longer tracked gives what was kept for it, if anything: the task was
// redelivered because its first answer was lost.
func (a *Agent) removeWorkingSet(runnerID string, now time.Time) *WorkingSetReport {
	a.mu.Lock()
	defer a.mu.Unlock()
	for id, kept := range a.removedWorkingSets {
		if now.Sub(kept.at) > removedWorkingSetKeep {
			delete(a.removedWorkingSets, id)
		}
	}
	r := a.runners[runnerID]
	if r == nil {
		return a.removedWorkingSets[runnerID].report
	}
	ws := r.workingSetReport(now)
	if ws == nil {
		return nil
	}
	ws.Final = true
	a.removedWorkingSets[runnerID] = removedWorkingSet{report: ws, at: now}
	return ws
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
