package store

import (
	"context"
	"strings"
	"testing"
	"time"
)

// Review 3 of #20, finding 4: fifteen recent heavy runs whose working set is
// untrusted and five light trusted ones used to become the five light ones
// alone, and a job booked at 8192 MiB was booked at 640. Every run that is not
// measured the new way now keeps its old figure; only a record from before the
// working set was measured at all, older than every measured run, is dropped.
func TestASwitchedJobKeepsItsRecentUnmeasuredRunsAtTheirOldFigure(t *testing.T) {
	var runs []JobPeak
	for range 15 {
		runs = append(runs, JobPeak{Job: "build", MemoryMB: 8192, WorkingSetMB: -1})
	}
	for range 5 {
		runs = append(runs, measured(JobPeak{Job: "build", MemoryMB: 512, WorkingSetMB: 512}))
	}
	got := PreferWorkingSet(runs)
	heavy := 0
	for _, p := range got {
		if p.MemoryMB == 8192 {
			heavy++
		}
	}
	if len(got) != 20 || heavy != 15 {
		t.Fatalf("switched history kept %d runs, %d at 8192 MiB; want all 20, 15 heavy", len(got), heavy)
	}

	// Newest first: measured, then an older agent's run (0), then measured,
	// then a provisional peak, then records from before any measurement.
	mixed := []JobPeak{
		measured(JobPeak{MemoryMB: 4000, WorkingSetMB: 1000}),
		{MemoryMB: 7000},
		measured(JobPeak{MemoryMB: 4000, WorkingSetMB: 1000}),
		{MemoryMB: 6000, WorkingSetMB: 900, WorkflowPath: ".github/workflows/ci.yml"}, // not final
		measured(JobPeak{MemoryMB: 4000, WorkingSetMB: 1000}),
		measured(JobPeak{MemoryMB: 4000, WorkingSetMB: 1000}),
		measured(JobPeak{MemoryMB: 4000, WorkingSetMB: 1000}),
		{MemoryMB: 9000},
		{MemoryMB: 9000},
	}
	got = PreferWorkingSet(mixed)
	want := []int64{1000, 7000, 1000, 6000, 1000, 1000, 1000}
	if len(got) != len(want) {
		t.Fatalf("history = %+v, want %v", got, want)
	}
	for i := range want {
		if got[i].MemoryMB != want[i] {
			t.Fatalf("run %d reads %d, want %d (%+v)", i, got[i].MemoryMB, want[i], got)
		}
	}
}

// Review 3 of #20, finding 6: a job's history is named by display names two
// definitions can share. A job whose runs name two workflow files does not
// switch; a run whose workflow file is unknown, or whose run had another job of
// the same name, is not measured.
func TestAJobThatIsNotIdentifiedDoesNotSwitch(t *testing.T) {
	five := func(path string) []JobPeak {
		var out []JobPeak
		for range 5 {
			out = append(out, JobPeak{MemoryMB: 4000, WorkingSetMB: 1000, WorkingSetFinal: true, WorkflowPath: path})
		}
		return out
	}
	twoFiles := append(five(".github/workflows/light.yml"), JobPeak{MemoryMB: 16000, WorkflowPath: ".github/workflows/heavy.yml"})
	if got := PreferWorkingSet(twoFiles); got[0].MemoryMB != 4000 || got[5].MemoryMB != 16000 || len(got) != 6 {
		t.Fatalf("a job of two workflow files switched: %+v", got)
	}
	if got := PreferWorkingSet(five("")); got[0].MemoryMB != 4000 {
		t.Fatalf("runs with no known workflow file switched: %+v", got)
	}
	// Five runs whose file is unknown beside one whose file is known: the
	// five are not measured, so nothing switches.
	unknown := append(five(""), JobPeak{MemoryMB: 4000, WorkingSetMB: 1000, WorkingSetFinal: true, WorkflowPath: ".github/workflows/ci.yml"})
	if got := PreferWorkingSet(unknown); got[0].MemoryMB != 4000 {
		t.Fatalf("runs with no known workflow file counted as measured: %+v", got)
	}
	shared := five(".github/workflows/ci.yml")
	shared[0].SharedName = true
	if got := PreferWorkingSet(shared); got[1].MemoryMB != 4000 {
		t.Fatalf("four measured runs and one with a shared name switched: %+v", got)
	}
	if got := PreferWorkingSet(five(".github/workflows/ci.yml")); got[0].MemoryMB != 1000 {
		t.Fatalf("five identified measured runs did not switch: %+v", got)
	}
}

// Review 3 of #20, finding 5: a trusted provisional peak, then the runner's
// final verdict that its readings had a gap, held through a disconnect and
// arriving after WorkingSetLateReport. The distrust reaches the job however
// late, and the provisional peak was never sealed anyway.
func TestLateDistrustInvalidatesAProvisionalPeak(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	at := versionsEpoch
	j := usageJob(t, s, 1, "build", "pool_a", "run_1", JobInProgress, at)
	if err := s.RecordJobUsage(ctx, "run_1", 1, 8192); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordJobWorkingSet(ctx, "run_1", 512, true, false, at); err != nil {
		t.Fatal(err)
	}
	done := at.Add(time.Hour)
	if _, _, err := s.ApplyJob(ctx, &Job{GitHubJobID: 5001, State: JobCompleted, Conclusion: "success", CompletedAt: &done}); err != nil {
		t.Fatal(err)
	}
	if sealed(t, s, j.ID) {
		t.Fatal("a provisional peak was sealed")
	}
	if err := s.RecordJobWorkingSet(ctx, "run_1", 8192, false, true, done.Add(WorkingSetLateReport+time.Second)); err != nil {
		t.Fatal(err)
	}
	var ws int64
	if err := s.read.QueryRowContext(ctx, `SELECT peak_working_set_mb FROM jobs WHERE id = ?`, j.ID).Scan(&ws); err != nil {
		t.Fatal(err)
	}
	if ws != -1 {
		t.Fatalf("late distrust left the working set at %d, want -1", ws)
	}
	// A late trusted final verdict, past the window, seals nothing.
	j2 := usageJob(t, s, 2, "build", "pool_a", "run_2", JobInProgress, at)
	if err := s.RecordJobWorkingSet(ctx, "run_2", 512, true, false, at); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ApplyJob(ctx, &Job{GitHubJobID: 5002, State: JobCompleted, Conclusion: "success", CompletedAt: &done}); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordJobWorkingSet(ctx, "run_2", 512, true, true, done.Add(WorkingSetLateReport+time.Second)); err != nil {
		t.Fatal(err)
	}
	if sealed(t, s, j2.ID) {
		t.Fatal("a final verdict past the late window sealed the run")
	}
}

// The histories read each run's identity: its workflow file from the run
// lookup, and whether another job of the same run attempt had its name.
func TestTheHistoriesReadEachRunsIdentity(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	for i, runner := range []string{"run_a", "run_b"} {
		usageJob(t, s, i+1, "build", "pool_a", runner, JobCompleted, versionsEpoch.Add(time.Duration(i)*time.Hour))
		if err := s.RecordJobUsage(ctx, runner, 1, 4096); err != nil {
			t.Fatal(err)
		}
	}
	// Both completed already; usage on a completed job is not recorded, so set it.
	if _, err := s.write.ExecContext(ctx, `UPDATE jobs SET peak_memory_mb = 4096, working_set_final = 1, peak_working_set_mb = 1000`); err != nil {
		t.Fatal(err)
	}
	identify(t, s, "run_a", ".github/workflows/ci.yml")
	// run_b shares run_a's run and attempt, under the same name.
	if _, err := s.write.ExecContext(ctx, `UPDATE jobs SET github_run_id = (SELECT github_run_id FROM jobs WHERE runner_id = 'run_a') WHERE runner_id = 'run_b'`); err != nil {
		t.Fatal(err)
	}
	k := JobUsageKey{Repo: "eyupio/zoomies", Workflow: "CI", JobName: "build"}
	hist, err := s.JobUsageHistory(ctx, []JobUsageKey{k})
	if err != nil {
		t.Fatal(err)
	}
	k.PoolID = "pool_a"
	runs := hist[k]
	if len(runs) != 2 {
		t.Fatalf("history = %+v", hist)
	}
	for _, p := range runs {
		if p.WorkflowPath != ".github/workflows/ci.yml" || !p.SharedName || !p.WorkingSetFinal || WorkingSetMeasured(p) {
			t.Fatalf("run %+v: want the workflow file, a shared name, final, and not measured", p)
		}
	}
	pools, err := s.PoolUsageHistory(ctx, []string{"pool_a"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range pools["pool_a"] {
		if p.WorkflowPath != ".github/workflows/ci.yml" || !p.SharedName {
			t.Fatalf("pool run %+v lacks its identity", p)
		}
	}
	var runID int64
	if err := s.read.QueryRowContext(ctx, `SELECT github_run_id FROM jobs WHERE runner_id = 'run_a'`).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	if got, err := s.WorkflowRunPath(ctx, "eyupio/zoomies", runID); err != nil || got != ".github/workflows/ci.yml" {
		t.Fatalf("WorkflowRunPath = %q, %v", got, err)
	}
	if got, err := s.WorkflowRunPath(ctx, "eyupio/zoomies", runID+1); err != nil || got != "" {
		t.Fatalf("WorkflowRunPath of an unknown run = %q, %v", got, err)
	}
}

// The identity the histories read costs a primary-key seek of
// workflow_run_paths and a seek of idx_jobs_run per run, never a scan: both
// statements run on every scheduling pass.
func TestTheHistoriesIdentityColumnsSeekTheirIndexes(t *testing.T) {
	s := newTestStore(t)
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{jobUsageHistorySQL, []any{"r", "w", "j", 10}},
		{poolUsageHistorySQL, []any{"pool_a", 10}},
	} {
		rows, err := s.read.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+q.sql, q.args...)
		if err != nil {
			t.Fatal(err)
		}
		var plan []string
		for rows.Next() {
			var id, parent, notused int
			var detail string
			if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
				t.Fatal(err)
			}
			plan = append(plan, detail)
		}
		rows.Close()
		joined := strings.Join(plan, "\n")
		if strings.Contains(joined, "SCAN") || !strings.Contains(joined, "idx_jobs_run") || !strings.Contains(joined, "SEARCH runs USING PRIMARY KEY") {
			t.Fatalf("plan:\n%s", joined)
		}
	}
}
