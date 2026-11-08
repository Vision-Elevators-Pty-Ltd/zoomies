package controller

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/eyupio/zoomies/internal/agent"
	"github.com/eyupio/zoomies/internal/backend"
	"github.com/eyupio/zoomies/internal/config"
	"github.com/eyupio/zoomies/internal/scheduler"
	"github.com/eyupio/zoomies/internal/store"
)

// The failure this was built for, end to end: a heavy job runs on a small
// host, the agent's samples record what it used, a step is killed for memory
// and the runner finishes the job anyway. The job has to come out of it with
// its peaks, marked as the fleet's failure, explained as a memory kill, and
// listed as a problem on the host -- because GitHub will call it a failed
// step like any other.
func TestAJobKilledForMemoryIsRecordedExplainedAndReported(t *testing.T) {
	h := newHarness(t)
	_, pool, host := h.fleet()
	r := h.seedRunner(t, pool, host, store.RunnerBusy)
	started := h.c.Now().Add(-time.Hour)
	job, err := h.st.UpsertJob(h.ctx, &store.Job{
		GitHubJobID: 4242, Repo: "eyupio/zoomies", Workflow: "CI", JobName: "Go (controller)",
		Labels: pool.Labels, State: store.JobInProgress, PoolID: pool.ID, Matched: true,
		RunnerID: r.ID, RunnerName: r.Name, QueuedAt: started, StartedAt: &started, HostID: host.ID,
	})
	if err != nil {
		t.Fatalf("UpsertJob: %v", err)
	}

	at := h.c.Now()
	for _, s := range []backend.Stats{
		{SampledAt: &at, CPUPercent: 390, MemoryBytes: 2900 << 20},
		{SampledAt: &at, CPUPercent: 120, MemoryBytes: 1200 << 20},
	} {
		if err := h.c.ReportRunners(h.ctx, host.ID, []agent.RunnerReport{{RunnerID: r.ID, Stats: s}}); err != nil {
			t.Fatalf("ReportRunners: %v", err)
		}
	}

	done := h.c.Now()
	if _, err := h.st.UpsertJob(h.ctx, &store.Job{GitHubJobID: 4242, State: store.JobCompleted, Conclusion: "failure",
		Repo: job.Repo, StartedAt: &started, CompletedAt: &done}); err != nil {
		t.Fatalf("completing the job: %v", err)
	}
	if err := h.c.ReportRunners(h.ctx, host.ID, []agent.RunnerReport{{
		RunnerID: r.ID, State: store.RunnerRemoved, Fault: store.FaultOutOfMemory,
		Message: "runner exited after its job, but the kernel killed a process in it for its memory limit",
	}}); err != nil {
		t.Fatalf("ReportRunners: %v", err)
	}

	got, err := h.st.GetJob(h.ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !got.OOMKilled || got.FaultKind != store.FaultOutOfMemory || got.PeakMemoryMB != 2900 || got.PeakCPUs != 3.9 {
		t.Fatalf("job = oom %t, fault %q, peaks %v CPU %d MB", got.OOMKilled, got.FaultKind, got.PeakCPUs, got.PeakMemoryMB)
	}

	// The view is the API's shape, and the UI and the MCP tools read these
	// three fields from it.
	raw, err := json.Marshal(NewJobView(got, pool.Name))
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"peak_cpus":3.9`, `"peak_memory_mb":2900`, `"oom_killed":true`} {
		if !strings.Contains(string(raw), field) {
			t.Errorf("the job view has no %s: %s", field, raw)
		}
	}

	events, err := h.st.ListJobEvents(h.ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	kills := 0
	for _, e := range events {
		if e.Kind == store.JobEventOOMKilled {
			kills++
		}
	}
	if kills != 1 {
		t.Fatalf("timeline has %d memory kills, want 1", kills)
	}

	ex, err := h.c.ExplainJob(h.ctx, job.ID, 0)
	if err != nil {
		t.Fatalf("ExplainJob: %v", err)
	}
	if !strings.Contains(ex.Summary, "for its memory limit") || !strings.Contains(ex.Summary, "2.8 GB") {
		t.Errorf("the explanation does not lead with the kill: %q", ex.Summary)
	}

	problems, err := h.c.Problems(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, p := range problems {
		if p.Code == "jobs.oom_killed" {
			found = true
			if !strings.Contains(p.Title, host.ID) {
				t.Errorf("the problem does not name the host: %q", p.Title)
			}
		}
	}
	if !found {
		t.Error("no jobs.oom_killed problem was raised")
	}
}

// A report's working set (RunnerReport.WorkingSet) is recorded on the runner's
// job beside its usage peak, never lowered; the final report of an ended
// runner, which can come after GitHub closed the job, still reaches it. A run
// the agent says its readings did not cover, and a runner that is not
// ephemeral, mark the job's working set untrusted. A report with none records
// nothing. The reports are decoded from the agent's JSON, as the controller
// receives them (agent.TestAFailedReadingReachesTheReportAsIncomplete).
func TestAReportsWorkingSetPeakIsRecordedOnTheRunnersJob(t *testing.T) {
	decode := func(t *testing.T, raw string) agent.RunnerReport {
		t.Helper()
		var rep agent.RunnerReport
		if err := json.Unmarshal([]byte(raw), &rep); err != nil {
			t.Fatal(err)
		}
		return rep
	}
	run := func(t *testing.T, ephemeral bool, reports []string, final string) store.JobPeak {
		t.Helper()
		h := newHarness(t)
		_, pool, host := h.fleet()
		r := &store.Runner{PoolID: pool.ID, HostID: host.ID, Name: "zoomies-ws-" + store.NewSecret(4),
			State: store.RunnerProvisioning, Ephemeral: ephemeral, Labels: pool.Labels}
		if err := h.st.CreateRunner(h.ctx, r); err != nil {
			t.Fatalf("CreateRunner: %v", err)
		}
		for _, to := range []store.RunnerState{store.RunnerRegistering, store.RunnerIdle, store.RunnerBusy} {
			out, err := h.st.TransitionRunner(h.ctx, r.ID, to, "")
			if err != nil {
				t.Fatalf("TransitionRunner(%s): %v", to, err)
			}
			r = out
		}
		started := h.c.Now().Add(-time.Hour)
		if _, err := h.st.UpsertJob(h.ctx, &store.Job{
			GitHubJobID: 4343, Repo: "eyupio/zoomies", Workflow: "CI", JobName: "build",
			Labels: pool.Labels, State: store.JobInProgress, PoolID: pool.ID, Matched: true,
			RunnerID: r.ID, RunnerName: r.Name, QueuedAt: started, StartedAt: &started, HostID: host.ID,
		}); err != nil {
			t.Fatalf("UpsertJob: %v", err)
		}
		at := h.c.Now()
		for _, raw := range reports {
			rep := decode(t, raw)
			rep.RunnerID, rep.Stats.SampledAt = r.ID, &at
			if err := h.c.ReportRunners(h.ctx, host.ID, []agent.RunnerReport{rep}); err != nil {
				t.Fatalf("ReportRunners: %v", err)
			}
		}
		done := h.c.Now()
		if _, err := h.st.UpsertJob(h.ctx, &store.Job{GitHubJobID: 4343, State: store.JobCompleted, Conclusion: "success",
			Repo: "eyupio/zoomies", StartedAt: &started, CompletedAt: &done}); err != nil {
			t.Fatalf("completing the job: %v", err)
		}
		rep := decode(t, final)
		rep.RunnerID, rep.State, rep.Phase = r.ID, store.RunnerRemoved, backend.PhaseExited
		if err := h.c.ReportRunners(h.ctx, host.ID, []agent.RunnerReport{rep}); err != nil {
			t.Fatalf("ReportRunners: %v", err)
		}
		key := store.JobUsageKey{Repo: "eyupio/zoomies", Workflow: "CI", JobName: "build", PoolID: pool.ID}
		got, err := h.st.JobUsageHistory(h.ctx, []store.JobUsageKey{key})
		if err != nil {
			t.Fatal(err)
		}
		if len(got[key]) != 1 {
			t.Fatalf("history = %+v, want one run", got[key])
		}
		return got[key][0]
	}
	live := []string{
		`{"stats":{"cpu_percent":100,"memory_bytes":9437184000,"working_set_bytes":5242880000},"working_set":{"peak_bytes":6815744000}}`,
		`{"stats":{"cpu_percent":100,"memory_bytes":3145728000},"working_set":{"peak_bytes":6815744000}}`,
	}
	final := `{"working_set":{"peak_bytes":7549747200}}`
	t.Run("trusted, with the peak the job ended on", func(t *testing.T) {
		if p := run(t, true, live, final); p.MemoryMB != 9000 || p.WorkingSetMB != 7200 {
			t.Fatalf("run = %+v, want 9000 MB with a 7200 MB working set from the final report", p)
		}
	})
	t.Run("readings that did not cover it", func(t *testing.T) {
		if p := run(t, true, live, `{"working_set":{"peak_bytes":7549747200,"incomplete":true}}`); p.WorkingSetMB != -1 {
			t.Fatalf("run = %+v, want an untrusted working set", p)
		}
	})
	t.Run("an agent that sends none", func(t *testing.T) {
		old := []string{`{"stats":{"cpu_percent":100,"memory_bytes":9437184000}}`}
		if p := run(t, true, old, `{}`); p.WorkingSetMB != 0 || p.MemoryMB != 9000 {
			t.Fatalf("run = %+v, want the usage peak and no working set", p)
		}
	})
	t.Run("not ephemeral", func(t *testing.T) {
		if p := run(t, false, live, final); p.WorkingSetMB != -1 {
			t.Fatalf("run = %+v, want an untrusted working set", p)
		}
	})
}

// scheduler.working_set_history off hands scheduling the store's histories
// untouched, the same rows the store returns, which are vision.9's
// (store.TestTheHistoriesAreVision9s...); on, each goes through the switch.
func TestTheWorkingSetHistorySettingGatesWhatSchedulingReads(t *testing.T) {
	h := newHarness(t)
	_, pool, host := h.fleet()
	h.c.UpdateConfig(func(c *config.Config) { c.Scheduler.MemoryOvercommitPercent = 300 })
	n := 0
	record := func(mb, ws int64) {
		n++
		r := h.seedRunner(t, pool, host, store.RunnerBusy)
		started := h.c.Now().Add(-time.Duration(100-n) * time.Minute)
		if _, err := h.st.UpsertJob(h.ctx, &store.Job{GitHubJobID: int64(7000 + n), GitHubRunID: int64(9000 + n), Repo: "eyupio/zoomies", Workflow: "CI",
			JobName: "build", Labels: pool.Labels, State: store.JobInProgress, PoolID: pool.ID, Matched: true,
			RunnerID: r.ID, QueuedAt: started, StartedAt: &started, HostID: host.ID}); err != nil {
			t.Fatal(err)
		}
		if err := h.st.SetWorkflowRunPath(h.ctx, "eyupio/zoomies", int64(9000+n), ".github/workflows/ci.yml"); err != nil {
			t.Fatal(err)
		}
		if err := h.st.RecordJobUsage(h.ctx, r.ID, 1, mb); err != nil {
			t.Fatal(err)
		}
		if ws > 0 {
			if err := h.st.RecordJobWorkingSet(h.ctx, r.ID, ws, true, true, h.c.Now()); err != nil {
				t.Fatal(err)
			}
		}
		done := started.Add(time.Minute)
		if _, err := h.st.UpsertJob(h.ctx, &store.Job{GitHubJobID: int64(7000 + n), State: store.JobCompleted,
			Conclusion: "success", Repo: "eyupio/zoomies", StartedAt: &started, CompletedAt: &done}); err != nil {
			t.Fatal(err)
		}
	}
	for range 10 {
		record(8192, 0)
	}
	for range 5 {
		record(8192, 512)
	}
	queued := h.c.Now().Add(-time.Minute)
	if _, err := h.st.UpsertJob(h.ctx, &store.Job{GitHubJobID: 7999, Repo: "eyupio/zoomies", Workflow: "CI", JobName: "build",
		Labels: pool.Labels, State: store.JobQueued, QueuedAt: queued}); err != nil {
		t.Fatal(err)
	}
	key := store.JobUsageKey{Repo: "eyupio/zoomies", Workflow: "CI", JobName: "build", PoolID: pool.ID}
	raw, err := h.st.JobUsageHistory(h.ctx, []store.JobUsageKey{key})
	if err != nil {
		t.Fatal(err)
	}
	rawPools, err := h.st.PoolUsageHistory(h.ctx, []string{pool.ID}, scheduler.PoolBookingRuns)
	if err != nil {
		t.Fatal(err)
	}
	if h.c.cfg().Scheduler.WorkingSetHistory != "off" {
		t.Fatalf("scheduler.working_set_history defaults to %q, want off", h.c.cfg().Scheduler.WorkingSetHistory)
	}
	s, err := h.c.snapshot(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(s.JobHistory[key], raw[key]) || s.Booking == nil || !reflect.DeepEqual(s.Booking.PoolRuns[pool.ID], rawPools[pool.ID]) {
		t.Fatalf("with the setting off scheduling read something other than the store's rows:\n job %+v\n pool %+v", s.JobHistory[key], s.Booking)
	}
	for _, p := range s.JobHistory[key] {
		if p.MemoryMB != 8192 {
			t.Fatalf("with the setting off a run was read at %d MB, want its 8192 MB usage peak", p.MemoryMB)
		}
	}
	h.c.UpdateConfig(func(c *config.Config) { c.Scheduler.WorkingSetHistory = "on" })
	s, err = h.c.snapshot(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := s.JobHistory[key]; len(got) != 5 || got[0].MemoryMB != 512 {
		t.Fatalf("with the setting on the job history is %+v, want its five working-set runs", got)
	}
}

// scheduler.working_set_sampling off reaches agents in the heartbeat; on, the
// default, sends nothing new.
func TestTheWorkingSetSamplingSettingReachesAgents(t *testing.T) {
	h := newHarness(t)
	_, _, host := h.fleet()
	resp, err := h.c.Heartbeat(h.ctx, host.ID, agent.HeartbeatRequest{})
	if err != nil || resp.WorkingSetSamplingOff {
		t.Fatalf("by default: %+v, %v; want sampling on", resp, err)
	}
	h.c.UpdateConfig(func(c *config.Config) { c.Scheduler.WorkingSetSampling = "off" })
	if resp, err = h.c.Heartbeat(h.ctx, host.ID, agent.HeartbeatRequest{}); err != nil || !resp.WorkingSetSamplingOff {
		t.Fatalf("with sampling off: %+v, %v; want it said", resp, err)
	}
}
