package store

import (
	"context"
	"strings"
	"testing"
	"time"
)

// The old figure stands until a job has WorkingSetMinRuns runs measured the
// new way; from then on only those runs are read, at their working set, and a
// run the kernel killed is kept whatever it was measured with.
func TestPreferWorkingSetSwitchesOnlyOnceEnoughRunsHaveIt(t *testing.T) {
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
	// A killed run does not count towards the five: it says nothing about the
	// new measurement.
	four := []JobPeak{ws(1, 1), ws(1, 1), ws(1, 1), ws(1, 1), {MemoryMB: 2, WorkingSetMB: 2, OOMKilled: true}, old(5)}
	if got := PreferWorkingSet(four); len(got) != len(four) || got[5].MemoryMB != 5 {
		t.Fatalf("a killed run counted towards switching: %+v", got)
	}
}

// The working set is recorded beside the usage peak, only on the job in
// progress and never lowered, and both histories read it through
// PreferWorkingSet.
func TestTheHistoriesReadTheWorkingSetOnceEnoughRunsHaveIt(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	n := 0
	record := func(job string, mb, wsMB int64, at time.Time) string {
		n++
		runner := "run_" + strings.Repeat("w", n)
		usageJob(t, s, n, job, "pool_a", runner, JobInProgress, at)
		if err := s.RecordJobUsage(ctx, runner, 1, mb); err != nil {
			t.Fatal(err)
		}
		for _, w := range []int64{wsMB, wsMB / 2, 0} { // a later, smaller sample lowers nothing
			if err := s.RecordJobWorkingSet(ctx, runner, w); err != nil {
				t.Fatal(err)
			}
		}
		done := at.Add(time.Hour)
		if _, _, err := s.ApplyJob(ctx, &Job{GitHubJobID: int64(5000 + n), State: JobCompleted, Conclusion: "success",
			Repo: "eyupio/zoomies", Workflow: "CI", JobName: job, StartedAt: &at, CompletedAt: &done}); err != nil {
			t.Fatal(err)
		}
		if err := s.RecordJobWorkingSet(ctx, runner, 99999); err != nil { // finished: not charged
			t.Fatal(err)
		}
		return runner
	}
	for i := range 10 { // ten old runs, then five new ones
		record("build", 12000, 0, versionsEpoch.Add(time.Duration(i)*time.Minute))
	}
	key := JobUsageKey{Repo: "eyupio/zoomies", Workflow: "CI", JobName: "build", PoolID: "pool_a"}
	read := func() ([]JobPeak, []JobPeak) {
		jobs, err := s.JobUsageHistory(ctx, []JobUsageKey{key})
		if err != nil {
			t.Fatal(err)
		}
		pools, err := s.PoolUsageHistory(ctx, []string{"pool_a"}, 100)
		if err != nil {
			t.Fatal(err)
		}
		return jobs[key], pools["pool_a"]
	}
	for i := range 4 {
		record("build", 12000, 7000, versionsEpoch.Add(time.Hour+time.Duration(i)*time.Minute))
	}
	if j, p := read(); len(j) != 14 || j[0].MemoryMB != 12000 || len(p) != 14 || p[0].MemoryMB != 12000 {
		t.Fatalf("with four new runs the history switched: job %+v pool %+v", j, p)
	}
	record("build", 12000, 7000, versionsEpoch.Add(2*time.Hour))
	j, p := read()
	if len(j) != 5 || len(p) != 5 {
		t.Fatalf("with five new runs: job %d runs, pool %d, want 5 and 5", len(j), len(p))
	}
	for _, r := range append(j, p...) {
		if r.MemoryMB != 7000 || r.WorkingSetMB != 7000 {
			t.Fatalf("a run read %+v, want its 7000 MB working set", r)
		}
	}
}
