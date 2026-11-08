package store

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"
)

// vision9JobUsageHistory and vision9PoolUsageHistory are JobUsageHistory and
// PoolUsageHistory exactly as 1.4.1-vision.9 (17330ad) shipped them, statements
// and all, kept here so that a test can hold today's to them.
func vision9JobUsageHistory(s *Store, ctx context.Context, keys []JobUsageKey) (map[JobUsageKey][]JobPeak, error) {
	const q = `SELECT pool_id, peak_cpus, peak_memory_mb, oom_killed, granted_memory_mb FROM jobs
	WHERE repo = ? AND workflow = ? AND job_name = ? AND state = 'completed'
	AND (peak_memory_mb > 0 OR peak_cpus > 0 OR oom_killed = 1)
	ORDER BY completed_at DESC LIMIT ?`
	out := map[JobUsageKey][]JobPeak{}
	type triple struct{ repo, workflow, job string }
	seen := map[triple]bool{}
	for _, k := range keys {
		t := triple{k.Repo, k.Workflow, k.JobName}
		if seen[t] {
			continue
		}
		seen[t] = true
		rows, err := s.read.QueryContext(ctx, q, t.repo, t.workflow, t.job, JobUsageHistoryLimit*3)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var p JobPeak
			var pool string
			var oom int
			if err := rows.Scan(&pool, &p.CPUs, &p.MemoryMB, &oom, &p.GrantedMemoryMB); err != nil {
				rows.Close()
				return nil, err
			}
			p.OOMKilled = oom == 1
			k := JobUsageKey{Repo: t.repo, Workflow: t.workflow, JobName: t.job, PoolID: pool}
			if len(out[k]) < JobUsageHistoryLimit {
				out[k] = append(out[k], p)
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func vision9PoolUsageHistory(s *Store, ctx context.Context, poolIDs []string, limit int) (map[string][]JobPeak, error) {
	const q = `SELECT peak_cpus, peak_memory_mb, oom_killed, granted_memory_mb FROM jobs
	WHERE pool_id = ? AND state = 'completed' AND (peak_memory_mb > 0 OR oom_killed = 1)
	ORDER BY queued_at DESC LIMIT ?`
	out := map[string][]JobPeak{}
	if limit <= 0 {
		return out, nil
	}
	seen := map[string]bool{}
	for _, id := range poolIDs {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		rows, err := s.read.QueryContext(ctx, q, id, limit)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var p JobPeak
			var oom int
			if err := rows.Scan(&p.CPUs, &p.MemoryMB, &oom, &p.GrantedMemoryMB); err != nil {
				rows.Close()
				return nil, err
			}
			p.OOMKilled = oom == 1
			out[id] = append(out[id], p)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// withoutWorkingSet is a history with the fields vision.9 did not have
// cleared (all json:"-"), which nothing outside the switch reads.
func withoutWorkingSet[K comparable](h map[K][]JobPeak) map[K][]JobPeak {
	out := make(map[K][]JobPeak, len(h))
	for k, peaks := range h {
		cp := make([]JobPeak, len(peaks))
		for i, p := range peaks {
			p.WorkingSetMB, p.Job = 0, ""
			p.WorkingSetFinal, p.WorkflowPath, p.SharedName = false, "", false
			cp[i] = p
		}
		out[k] = cp
	}
	return out
}

// The histories the store returns are vision.9's, run for run and in the same
// order, whatever working sets have been recorded: old rows, trusted ones,
// untrusted (-1) ones, runs killed for memory, several jobs on several pools,
// more runs than a history keeps, and jobs still in progress. Only the two new
// fields differ, and they are read by the switch alone, which the controller
// applies only under scheduler.working_set_history = on.
func TestTheHistoriesAreVision9sWhateverWorkingSetsAreRecorded(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	n := 0
	at := versionsEpoch
	record := func(job, pool string, cpus float64, mb, ws int64, trusted, oom, finish bool) {
		n++
		at = at.Add(time.Minute)
		runner := fmt.Sprintf("run_g%03d", n)
		usageJob(t, s, n, job, pool, runner, JobInProgress, at)
		if mb > 0 || cpus > 0 {
			if err := s.RecordJobUsage(ctx, runner, cpus, mb); err != nil {
				t.Fatal(err)
			}
		}
		if ws > 0 || !trusted {
			if err := s.RecordJobWorkingSet(ctx, runner, ws, trusted, true, at); err != nil {
				t.Fatal(err)
			}
		}
		if finish {
			done := at.Add(time.Duration(n) * time.Second)
			if _, _, err := s.ApplyJob(ctx, &Job{GitHubJobID: int64(5000 + n), State: JobCompleted, Conclusion: "success",
				Repo: "eyupio/zoomies", Workflow: "CI", JobName: job, StartedAt: &at, CompletedAt: &done}); err != nil {
				t.Fatal(err)
			}
		}
		if oom {
			if _, _, err := s.MarkJobOOMKilled(ctx, runner, "killed"); err != nil {
				t.Fatal(err)
			}
		}
	}
	for i := range 30 { // more than a history keeps: old rows
		record("heavy", "pool_a", 4, int64(8000+i), 0, true, false, true)
	}
	for i := range 6 { // trusted working sets of the light job
		record("light", "pool_a", 1, int64(1500+i), int64(1000+i), true, false, true)
	}
	for i := range 6 { // untrusted heavy runs, recent
		record("heavy", "pool_a", 4, int64(9000+i), 0, false, false, true)
	}
	for i := range 6 { // trusted runs of the heavy job, lighter
		record("heavy", "pool_a", 4, int64(8100+i), 512, true, false, true)
	}
	record("heavy", "pool_a", 4, 7000, 6000, true, true, true) // killed for memory
	record("light", "pool_b", 1, 1700, 1100, true, false, true)
	record("light", "pool_b", 0, 0, 0, true, true, true)        // killed, no sample
	record("light", "pool_a", 1, 1600, 900, true, false, false) // still in progress

	keys := []JobUsageKey{{Repo: "eyupio/zoomies", Workflow: "CI", JobName: "heavy"}, {Repo: "eyupio/zoomies", Workflow: "CI", JobName: "light"}}
	want, err := vision9JobUsageHistory(s, ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.JobUsageHistory(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(withoutWorkingSet(got), want) {
		t.Fatalf("JobUsageHistory differs from vision.9's:\n got %+v\nwant %+v", withoutWorkingSet(got), want)
	}
	for _, limit := range []int{5, 100} {
		wantPools, err := vision9PoolUsageHistory(s, ctx, []string{"pool_a", "pool_b", "pool_none"}, limit)
		if err != nil {
			t.Fatal(err)
		}
		gotPools, err := s.PoolUsageHistory(ctx, []string{"pool_a", "pool_b", "pool_none"}, limit)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(withoutWorkingSet(gotPools), wantPools) {
			t.Fatalf("PoolUsageHistory(%d) differs from vision.9's:\n got %+v\nwant %+v", limit, withoutWorkingSet(gotPools), wantPools)
		}
	}
	// And the working sets were recorded: the fields carry them, unread.
	untrusted, trusted := 0, 0
	for _, p := range got[JobUsageKey{Repo: "eyupio/zoomies", Workflow: "CI", JobName: "heavy", PoolID: "pool_a"}] {
		switch {
		case p.WorkingSetMB < 0:
			untrusted++
		case p.WorkingSetMB > 0:
			trusted++
		}
	}
	if untrusted != 6 || trusted != 7 {
		t.Fatalf("heavy runs: %d untrusted and %d trusted recorded, want 6 and 7", untrusted, trusted)
	}
}
