package store

import (
	"context"
	"strings"
	"testing"
	"time"
)

// History booking books a runner whose job is not yet known at its pool's
// recent runs, whichever jobs they were, newest first and bounded; and a runner
// whose job is known at that job, found from the job GitHub says it is running.
func TestPoolUsageHistoryIsThePoolsRecentMeasuredRunsOfAnyJob(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	n := 0
	record := func(job, pool string, mb int64, at time.Time, finish bool) string {
		n++
		runner := "run_" + strings.Repeat("y", n)
		usageJob(t, s, n, job, pool, runner, JobInProgress, at)
		if mb > 0 {
			if err := s.RecordJobUsage(ctx, runner, 1, mb); err != nil {
				t.Fatal(err)
			}
		}
		if finish {
			done := at.Add(time.Hour)
			if _, _, err := s.ApplyJob(ctx, &Job{GitHubJobID: int64(5000 + n), State: JobCompleted, Conclusion: "success",
				Repo: "eyupio/zoomies", Workflow: "CI", JobName: job, StartedAt: &at, CompletedAt: &done}); err != nil {
				t.Fatal(err)
			}
		}
		return runner
	}
	for i := range 6 {
		record([]string{"visual", "draft"}[i%2], "pool_a", int64(1000+i), versionsEpoch.Add(time.Duration(i)*time.Hour), true)
	}
	record("visual", "pool_a", 0, versionsEpoch.Add(10*time.Hour), true) // never measured
	record("visual", "pool_b", 9000, versionsEpoch, true)
	running := record("draft", "pool_a", 2000, versionsEpoch.Add(11*time.Hour), false)

	got, err := s.PoolUsageHistory(ctx, []string{"pool_a", "pool_a", "pool_none"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	a := got["pool_a"]
	if len(a) != 4 || a[0].MemoryMB != 1005 || a[3].MemoryMB != 1002 {
		t.Fatalf("pool_a = %+v, want its four newest measured, finished runs", a)
	}
	if _, ok := got["pool_none"]; ok || len(got) != 1 {
		t.Fatalf("history = %+v, want only pool_a", got)
	}

	keys, err := s.RunningJobKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if k := keys[running]; len(keys) != 1 || k.JobName != "draft" || k.PoolID != "pool_a" || k.Repo != "eyupio/zoomies" || k.Workflow != "CI" {
		t.Fatalf("running job keys = %+v, want only %s running draft on pool_a", keys, running)
	}
}

// It runs for every pool on every pass, so it walks the pool's index.
func TestPoolUsageHistorySeeksThePoolsIndex(t *testing.T) {
	s := newTestStore(t)
	rows, err := s.read.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+poolUsageHistorySQL, "pool_a", 10)
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN: %v", err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if joined := strings.Join(plan, "\n"); !strings.Contains(joined, "idx_jobs_pool") || strings.Contains(joined, "TEMP B-TREE") {
		t.Fatalf("the pool history lookup does not walk its index:\n%s", joined)
	}
}
