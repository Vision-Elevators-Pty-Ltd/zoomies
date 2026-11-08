package store

import (
	"context"
	"strings"
	"testing"
	"time"
)

// The old figure stands until a job has WorkingSetMinRuns trusted runs
// measured the new way; from then on only those runs are read, at their
// working set, and a run the kernel killed is kept whatever it was measured
// with. Untrusted runs (below zero) never count.
func TestPreferWorkingSetSwitchesOnlyOnceEnoughTrustedRunsHaveIt(t *testing.T) {
	old := func(mb int64) JobPeak { return JobPeak{MemoryMB: mb} }
	ws := func(mb, w int64) JobPeak { return JobPeak{MemoryMB: mb, WorkingSetMB: w} }
	killed := JobPeak{MemoryMB: 15000, OOMKilled: true, GrantedMemoryMB: 16384}

	few := []JobPeak{ws(9000, 6000), ws(9000, 6000), ws(9000, 6000), ws(9000, 6000), old(12000), old(12000)}
	if got := PreferWorkingSet(few); len(got) != len(few) || got[0].MemoryMB != 9000 || got[4].MemoryMB != 12000 {
		t.Fatalf("four new runs switched the history: %+v", got)
	}
	enough := append([]JobPeak{ws(9000, 6100)}, few...)
	enough = append(enough, killed)
	got := PreferWorkingSet(enough)
	if len(got) != 6 {
		t.Fatalf("history = %+v, want the five working-set runs and the killed one", got)
	}
	for _, p := range got[:5] {
		if p.MemoryMB != p.WorkingSetMB {
			t.Fatalf("a run kept its old figure: %+v", p)
		}
	}
	if k := got[5]; !k.OOMKilled || k.MemoryMB != 15000 || k.GrantedMemoryMB != 16384 {
		t.Fatalf("the killed run was changed or dropped: %+v", k)
	}
	four := []JobPeak{ws(1, 1), ws(1, 1), ws(1, 1), ws(1, 1), {MemoryMB: 2, WorkingSetMB: 2, OOMKilled: true}, old(5)}
	if got := PreferWorkingSet(four); len(got) != len(four) || got[5].MemoryMB != 5 {
		t.Fatalf("a killed run counted towards switching: %+v", got)
	}
	// Review of #20, blocker 2: runs whose readings had a gap are untrusted
	// (-1) and never switch a job, however many.
	var partial []JobPeak
	for range 10 {
		partial = append(partial, JobPeak{MemoryMB: 8192, WorkingSetMB: -1})
	}
	partial = append(partial, old(8192))
	if got := PreferWorkingSet(partial); len(got) != len(partial) || got[0].MemoryMB != 8192 {
		t.Fatalf("untrusted runs switched the history: %+v", got)
	}
}

// Review of #20, blocker 1: in a pool's history, which mixes jobs, each job is
// decided on its own. Five light runs re-measured switch the light job, and
// leave every old run of the heavy job, which has none, at its old figure.
func TestPreferWorkingSetDecidesEachJobOfAPoolOnItsOwn(t *testing.T) {
	var pool []JobPeak
	for range 5 {
		pool = append(pool, JobPeak{Job: "light", MemoryMB: 1500, WorkingSetMB: 1024})
	}
	for range 20 {
		pool = append(pool, JobPeak{Job: "heavy", MemoryMB: 8192})
	}
	got := PreferWorkingSet(pool)
	if len(got) != 25 {
		t.Fatalf("pool history = %d runs, want all 25", len(got))
	}
	heavy := 0
	for _, p := range got {
		switch p.Job {
		case "light":
			if p.MemoryMB != 1024 {
				t.Fatalf("a light run was not read at its working set: %+v", p)
			}
		case "heavy":
			heavy++
			if p.MemoryMB != 8192 {
				t.Fatalf("a heavy run was changed: %+v", p)
			}
		}
	}
	if heavy != 20 {
		t.Fatalf("%d heavy runs kept, want 20", heavy)
	}
}

// The working set goes on the job in progress, or on the one the runner has
// just completed when its final report comes after GitHub closed the job, and
// is never lowered. An untrusted report marks the job for good.
func TestRecordJobWorkingSetFindsTheRunnersJobAndKeepsUntrustedForGood(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := versionsEpoch.Add(48 * time.Hour)
	running := usageJob(t, s, 1, "build", "pool_a", "run_1", JobInProgress, now.Add(-time.Hour))
	for _, w := range []int64{6000, 3000} {
		if err := s.RecordJobWorkingSet(ctx, "run_1", w, true, now); err != nil {
			t.Fatal(err)
		}
	}
	peak := func(id string) int64 {
		t.Helper()
		var v int64
		if err := s.read.QueryRowContext(ctx, `SELECT peak_working_set_mb FROM jobs WHERE id = ?`, id).Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	if got := peak(running.ID); got != 6000 {
		t.Fatalf("running job peak = %d, want 6000", got)
	}
	// Completed ten minutes ago: a late report still reaches it.
	done := now.Add(-10 * time.Minute)
	started := now.Add(-time.Hour)
	if _, _, err := s.ApplyJob(ctx, &Job{GitHubJobID: 5001, State: JobCompleted, Conclusion: "success",
		Repo: "eyupio/zoomies", Workflow: "CI", JobName: "build", StartedAt: &started, CompletedAt: &done}); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordJobWorkingSet(ctx, "run_1", 8192, true, now); err != nil {
		t.Fatal(err)
	}
	if got := peak(running.ID); got != 8192 {
		t.Fatalf("after a late final report the peak = %d, want 8192", got)
	}
	// Past WorkingSetLateReport it does not.
	if err := s.RecordJobWorkingSet(ctx, "run_1", 9999, true, now.Add(WorkingSetLateReport)); err != nil {
		t.Fatal(err)
	}
	if got := peak(running.ID); got != 8192 {
		t.Fatalf("a report past the late window changed the peak to %d", got)
	}
	// Untrusted, for good: a later trusted report does not undo it.
	if err := s.RecordJobWorkingSet(ctx, "run_1", 0, false, now); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordJobWorkingSet(ctx, "run_1", 9000, true, now); err != nil {
		t.Fatal(err)
	}
	if got := peak(running.ID); got != -1 {
		t.Fatalf("an untrusted job's peak = %d, want -1 for good", got)
	}
	// A runner with no job in reach records nothing and fails nothing.
	if err := s.RecordJobWorkingSet(ctx, "run_none", 5000, true, now); err != nil {
		t.Fatal(err)
	}
}

// The store's histories return every run at its usage peak, as before; the
// switch (PreferWorkingSet), which the controller applies only under
// scheduler.working_set_history = on, reads them job by job.
func TestTheHistoriesKeepUsagePeaksAndTheSwitchReadsThemJobByJob(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	n := 0
	record := func(job string, mb, wsMB int64, trusted bool, at time.Time) {
		n++
		runner := "run_" + strings.Repeat("w", n)
		usageJob(t, s, n, job, "pool_a", runner, JobInProgress, at)
		if err := s.RecordJobUsage(ctx, runner, 1, mb); err != nil {
			t.Fatal(err)
		}
		if wsMB > 0 || !trusted {
			if err := s.RecordJobWorkingSet(ctx, runner, wsMB, trusted, at); err != nil {
				t.Fatal(err)
			}
		}
		done := at.Add(time.Hour)
		if _, _, err := s.ApplyJob(ctx, &Job{GitHubJobID: int64(5000 + n), State: JobCompleted, Conclusion: "success",
			Repo: "eyupio/zoomies", Workflow: "CI", JobName: job, StartedAt: &at, CompletedAt: &done}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 10 {
		record("heavy", 8192, 0, true, versionsEpoch.Add(time.Duration(i)*time.Minute))
	}
	for i := range 6 {
		record("heavy", 8192, 512, false, versionsEpoch.Add(30*time.Minute+time.Duration(i)*time.Minute))
	}
	for i := range 5 {
		record("light", 1500, 1024, true, versionsEpoch.Add(time.Hour+time.Duration(i)*time.Minute))
	}
	pools, err := s.PoolUsageHistory(ctx, []string{"pool_a"}, 100)
	if err != nil {
		t.Fatal(err)
	}
	raw := pools["pool_a"]
	for _, p := range raw {
		if p.MemoryMB != 8192 && p.MemoryMB != 1500 {
			t.Fatalf("the store returned %+v; every run must be at its usage peak", p)
		}
	}
	h, l := 0, 0
	for _, p := range PreferWorkingSet(raw) {
		switch p.MemoryMB {
		case 8192:
			h++
		case 1024:
			l++
		default:
			t.Fatalf("switched pool run read %+v", p)
		}
	}
	if h != 16 || l != 5 {
		t.Fatalf("switched pool history: %d heavy and %d light runs, want 16 and 5", h, l)
	}
}
