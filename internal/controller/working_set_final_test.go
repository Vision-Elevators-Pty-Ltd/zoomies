package controller

import (
	"testing"
	"time"

	"github.com/eyupio/zoomies/internal/agent"
	"github.com/eyupio/zoomies/internal/backend"
	"github.com/eyupio/zoomies/internal/store"
)

func storedWorkingSet(t *testing.T, h *harness, job *store.Job) store.JobPeak {
	t.Helper()
	k := store.JobUsageKey{Repo: job.Repo, Workflow: job.Workflow, JobName: job.JobName, PoolID: job.PoolID}
	hist, err := h.st.JobUsageHistory(h.ctx, []store.JobUsageKey{k})
	if err != nil {
		t.Fatal(err)
	}
	if len(hist[k]) != 1 {
		t.Fatalf("history = %+v", hist)
	}
	return hist[k][0]
}

func busyJob(t *testing.T, h *harness, pool *store.Pool, r *store.Runner, id int64) *store.Job {
	t.Helper()
	start := h.c.Now().Add(-time.Hour)
	job, err := h.st.UpsertJob(h.ctx, &store.Job{GitHubJobID: id, GitHubRunID: id, Repo: "org/repo", Workflow: "CI", JobName: "build",
		State: store.JobInProgress, PoolID: pool.ID, Matched: true, RunnerID: r.ID, QueuedAt: start, StartedAt: &start})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.st.RecordJobUsage(h.ctx, r.ID, 1, 8192); err != nil {
		t.Fatal(err)
	}
	return job
}

func completeJob(t *testing.T, h *harness, job *store.Job) {
	t.Helper()
	done := h.c.Now()
	if _, err := h.st.UpsertJob(h.ctx, &store.Job{GitHubJobID: job.GitHubJobID, State: store.JobCompleted, Conclusion: "success",
		Repo: job.Repo, StartedAt: job.StartedAt, CompletedAt: &done}); err != nil {
		t.Fatal(err)
	}
}

// Review 3 of #20, finding 2: a live report of a runner this side has already
// finished with (failed on a host timeout while its job still runs) returned
// before the recorder, so its verdict that the readings had a gap was lost and
// an earlier trusted peak stood. It is recorded now, after the ownership check
// and before any lifecycle early return.
func TestALiveReportOfAFinishedRunnerStillRecordsItsWorkingSet(t *testing.T) {
	h := newHarness(t)
	_, pool, host := h.fleet()
	r := h.seedRunner(t, pool, host, store.RunnerBusy)
	job := busyJob(t, h, pool, r, 19343)
	now := h.c.Now()
	rep := agent.RunnerReport{RunnerID: r.ID, Phase: backend.PhaseRunning, Stats: backend.Stats{SampledAt: &now, MemoryBytes: 8192 << 20},
		WorkingSet: &agent.WorkingSetReport{PeakBytes: 512 << 20}}
	if err := h.c.ReportRunners(h.ctx, host.ID, []agent.RunnerReport{rep}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.st.TransitionRunner(h.ctx, r.ID, store.RunnerFailed, "host timeout"); err != nil {
		t.Fatal(err)
	}
	rep.WorkingSet = &agent.WorkingSetReport{PeakBytes: 4096 << 20, Incomplete: true}
	if err := h.c.ReportRunners(h.ctx, host.ID, []agent.RunnerReport{rep}); err != nil {
		t.Fatal(err)
	}
	completeJob(t, h, job)
	if p := storedWorkingSet(t, h, job); p.WorkingSetMB != -1 {
		t.Fatalf("stored working set = %d MiB, want -1: the live report's distrust was dropped", p.WorkingSetMB)
	}
}

// A host may not record on a runner it does not own, whatever the report.
func TestAnotherHostsReportRecordsNoWorkingSet(t *testing.T) {
	h := newHarness(t)
	_, pool, host := h.fleet()
	r := h.seedRunner(t, pool, host, store.RunnerBusy)
	job := busyJob(t, h, pool, r, 19344)
	rep := agent.RunnerReport{RunnerID: r.ID, Phase: backend.PhaseRunning, WorkingSet: &agent.WorkingSetReport{Incomplete: true}}
	_ = h.c.ReportRunners(h.ctx, "host_other", []agent.RunnerReport{rep})
	completeJob(t, h, job)
	if p := storedWorkingSet(t, h, job); p.WorkingSetMB != 0 {
		t.Fatalf("another host's report recorded %d", p.WorkingSetMB)
	}
}

// Review 3 of #20, finding 1, the controller's half: a remove task's result
// carries the removed runner's final working set, and it is recorded, sealed.
func TestARemoveResultRecordsTheFinalWorkingSet(t *testing.T) {
	h := newHarness(t)
	_, pool, host := h.fleet()
	r := h.seedRunner(t, pool, host, store.RunnerBusy)
	job := busyJob(t, h, pool, r, 19345)
	completeJob(t, h, job)
	res := agent.TaskResult{TaskID: "task-remove", Kind: agent.TaskRemoveRunner, RunnerID: r.ID, OK: true, State: store.RunnerRemoved,
		CompletedAt: h.c.Now(), WorkingSet: &agent.WorkingSetReport{PeakBytes: 2048 << 20, Final: true}}
	if err := h.c.ReportResult(h.ctx, host.ID, res); err != nil {
		t.Fatal(err)
	}
	if p := storedWorkingSet(t, h, job); p.WorkingSetMB != 2048 || !p.WorkingSetFinal {
		t.Fatalf("stored %+v, want 2048 MiB sealed", p)
	}
}

// Only a final verdict seals: the same peak without it is provisional.
func TestOnlyAFinalReportSealsAWorkingSet(t *testing.T) {
	h := newHarness(t)
	_, pool, host := h.fleet()
	r := h.seedRunner(t, pool, host, store.RunnerBusy)
	job := busyJob(t, h, pool, r, 19346)
	rep := agent.RunnerReport{RunnerID: r.ID, Phase: backend.PhaseRunning, WorkingSet: &agent.WorkingSetReport{PeakBytes: 1024 << 20}}
	if err := h.c.ReportRunners(h.ctx, host.ID, []agent.RunnerReport{rep}); err != nil {
		t.Fatal(err)
	}
	completeJob(t, h, job)
	if p := storedWorkingSet(t, h, job); p.WorkingSetMB != 1024 || p.WorkingSetFinal {
		t.Fatalf("stored %+v, want 1024 MiB unsealed", p)
	}
	rep.Phase = backend.PhaseExited
	rep.WorkingSet.Final = true
	if err := h.c.ReportRunners(h.ctx, host.ID, []agent.RunnerReport{rep}); err != nil {
		t.Fatal(err)
	}
	if p := storedWorkingSet(t, h, job); !p.WorkingSetFinal {
		t.Fatalf("stored %+v, want it sealed by the final report", p)
	}
}
