package agent

import (
	"context"
	"sync"
	"time"

	"github.com/eyupio/zoomies/internal/backend"
)

// A job's memory peak is what the controller books and raises runners by, and
// a 30-second stats sample misses a spike that comes and goes between two of
// them. The working set alone is read more often, for runners with work, with
// the one-shot reading the memory valve uses (no CPU interval, so a call is
// the daemon reading the cgroup once), and each stats sample carries the most
// it reached over the minute before.
const (
	workingSetInterval = 10 * time.Second
	workingSetBudget   = 8 * time.Second
	workingSetTimeout  = 3 * time.Second
	workingSetWorkers  = 4
	// workingSetWindow is how far back a stats sample looks: two stats
	// intervals, so a heartbeat that missed one sample still carries the
	// spike in the next. A peak is a maximum, so seeing it twice is harmless.
	workingSetWindow = 2 * statsInterval
	// workingSetFloor is the use below which a runner is not read between
	// stats samples: an idle runner uses 40-100 MB, and reading only the ones
	// above this keeps the extra calls to the runners with a job.
	workingSetFloor = 256 << 20
)

// workingSetSample is one fast reading of a runner's working set.
type workingSetSample struct {
	at    time.Time
	bytes int64
}

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
// stats sample named its containers and used at least workingSetFloor (in
// practice, the ones with a job), adding up a pair's two, and keeps it
// beside the runner. A runner any of whose containers could not be read is
// left out of this round: half a pair would read as the runner using less.
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
		if r.terminal || r.hostRemoved || r.handle == "" || r.phase != backend.PhaseRunning ||
			len(r.stats.Containers) == 0 || r.stats.MemoryBytes < workingSetFloor {
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
				if total > 0 {
					a.noteWorkingSet(j.runnerID, j.handle, total, a.now())
				}
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

// noteWorkingSet keeps one fast reading beside the runner it was taken of,
// dropping readings older than workingSetWindow.
func (a *Agent) noteWorkingSet(runnerID string, handle backend.Handle, bytes int64, now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	r := a.runners[runnerID]
	if r == nil || r.handle != handle {
		return
	}
	r.workingSets = append(pruneWorkingSets(r.workingSets, now), workingSetSample{at: now, bytes: bytes})
}

// pruneWorkingSets drops the readings older than workingSetWindow.
func pruneWorkingSets(s []workingSetSample, now time.Time) []workingSetSample {
	keep := s[:0]
	for _, w := range s {
		if now.Sub(w.at) <= workingSetWindow {
			keep = append(keep, w)
		}
	}
	return keep
}

// workingSetPeak is the most of a stats sample's own working set and the fast
// readings within workingSetWindow of now: what the sample reports as
// WorkingSetPeakBytes. Zero where nothing is known.
func workingSetPeak(stats backend.Stats, fast []workingSetSample, now time.Time) int64 {
	peak := stats.WorkingSetBytes
	for _, w := range fast {
		if now.Sub(w.at) <= workingSetWindow {
			peak = max(peak, w.bytes)
		}
	}
	return peak
}
