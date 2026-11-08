package agent

import (
	"context"
	"sync"
	"time"

	"github.com/eyupio/zoomies/internal/backend"
)

// A job's memory peak is what the controller books and raises runners by, and
// a 30-second stats sample misses a spike that comes and goes between two of
// them. The working set alone is read more often, for every running runner,
// with the one-shot reading the memory valve uses (no CPU interval, so a call
// is the daemon reading the cgroup once).
//
// Every reading, fast or full, raises a peak kept for the runner's whole life
// (workingSetPeak), and every report the runner sends carries it, so a peak
// survives a job that ends before the next full sample, a full sample that
// fails, and a report the controller did not get. The runner is ephemeral, so
// its life is its job; the controller does not trust the peak of one that is
// not.
//
// A peak is only a measurement of the job if the readings covered it: one is
// marked incomplete (workingSetIncomplete) as soon as more than
// workingSetMaxGap passes, while it runs, without a reading, counted from the
// container's start, and when any reading of it fails or comes back unknown.
// The controller keeps such a run's old figure.
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

func (a *Agent) workingSetLoop(ctx context.Context) error {
	ticker := time.NewTicker(workingSetInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		a.sampleWorkingSets(ctx)
	}
}

// sampleWorkingSets reads the working set of every running runner whose last
// stats sample named its containers, adding up a pair's two. A runner any of
// whose containers could not be read, or read as unknown, is marked
// incomplete: half a pair would read as the runner using less.
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
				var total int64
				for _, c := range j.containers {
					if ctx.Err() != nil {
						total = 0
						break
					}
					rctx, stop := context.WithTimeout(ctx, workingSetTimeout)
					reading, err := j.reader.MemoryUsage(rctx, c)
					stop()
					if err != nil || reading.WorkingSetBytes <= 0 {
						total = 0
						break
					}
					total += reading.WorkingSetBytes
				}
				a.noteWorkingSet(j.runnerID, j.handle, total, a.now())
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

// noteWorkingSet records one reading of a runner's working set, fast or full:
// bytes of zero or less is a reading that failed or came back unknown, and
// marks the runner incomplete.
func (a *Agent) noteWorkingSet(runnerID string, handle backend.Handle, bytes int64, now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	r := a.runners[runnerID]
	if r == nil || r.handle != handle {
		return
	}
	r.noteWorkingSet(bytes, now)
}

// noteWorkingSet is the reading on a tracked runner, under the agent's lock.
func (t *tracked) noteWorkingSet(bytes int64, now time.Time) {
	t.checkWorkingSetCoverage(now)
	if bytes <= 0 {
		t.workingSetIncomplete = true
		return
	}
	t.workingSetPeak = max(t.workingSetPeak, bytes)
	t.workingSetLastAt = now
}

// checkWorkingSetCoverage marks the runner incomplete if it has been running
// longer than workingSetMaxGap since its last reading, or since its container
// started where there has been none.
func (t *tracked) checkWorkingSetCoverage(now time.Time) {
	since := t.workingSetLastAt
	if since.IsZero() {
		since = t.runningSince
	}
	if !since.IsZero() && now.Sub(since) > workingSetMaxGap {
		t.workingSetIncomplete = true
	}
}

// foldWorkingSet puts the runner's kept peak and its coverage into a report's
// stats: every report carries the most any reading saw, whether or not the
// last full sample succeeded. A runner still running is checked for a gap up
// to now; one that has ended is judged by the readings it had.
func (t *tracked) foldWorkingSet(stats *backend.Stats, now time.Time) {
	if !t.terminal && t.phase == backend.PhaseRunning {
		t.checkWorkingSetCoverage(now)
	}
	stats.WorkingSetPeakBytes = max(stats.WorkingSetPeakBytes, stats.WorkingSetBytes, t.workingSetPeak)
	stats.WorkingSetIncomplete = t.workingSetIncomplete
}
