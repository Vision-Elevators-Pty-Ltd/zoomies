package store

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"
)

// The attention view answers the question an operator arrives with (which
// failures have I not seen that are still standing), so it has to keep a
// failure only while all three remain true: it failed, nobody has acknowledged
// it, and no later attempt has replaced it. Each seeded job below is one way
// of failing one of those.
func TestAttentionKeepsOnlyTheFailuresNobodyHasDealtWith(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	s := newTestStoreAt(t, func() time.Time { return now })
	ctx := context.Background()
	started := now.Add(-time.Minute)
	done := now.Add(-30 * time.Second)
	old := now.Add(-8 * 24 * time.Hour)
	oldStart := old.Add(time.Second)
	oldDone := old.Add(time.Minute)

	seed := []*Job{
		{GitHubJobID: 1, GitHubRunID: 100, Repo: "acme/widgets", JobName: "recent-failure", State: JobCompleted, Conclusion: "failure", QueuedAt: now.Add(-2 * time.Hour), StartedAt: &started, CompletedAt: &done, RunAttempt: 1},
		{GitHubJobID: 2, GitHubRunID: 101, Repo: "acme/widgets", JobName: "recent-fleet", State: JobInProgress, QueuedAt: now.Add(-time.Hour), StartedAt: &started},
		{GitHubJobID: 3, GitHubRunID: 102, Repo: "acme/widgets", JobName: "known", State: JobCompleted, Conclusion: "failure", QueuedAt: now.Add(-3 * time.Hour), StartedAt: &started, CompletedAt: &done, RunAttempt: 1},
		{GitHubJobID: 4, GitHubRunID: 103, Repo: "acme/widgets", JobName: "fixed-by-rerun", State: JobCompleted, Conclusion: "failure", QueuedAt: now.Add(-5 * time.Hour), StartedAt: &started, CompletedAt: &done, RunAttempt: 1},
		{GitHubJobID: 5, GitHubRunID: 103, Repo: "acme/widgets", JobName: "fixed-by-rerun", State: JobCompleted, Conclusion: "success", QueuedAt: now.Add(-4 * time.Hour), StartedAt: &started, CompletedAt: &done, RunAttempt: 2},
		{GitHubJobID: 6, GitHubRunID: 104, Repo: "acme/widgets", JobName: "still-failing", State: JobCompleted, Conclusion: "failure", QueuedAt: now.Add(-6 * time.Hour), StartedAt: &started, CompletedAt: &done, RunAttempt: 1},
		{GitHubJobID: 7, GitHubRunID: 104, Repo: "acme/widgets", JobName: "still-failing", State: JobCompleted, Conclusion: "timed_out", QueuedAt: now.Add(-5 * time.Hour), StartedAt: &started, CompletedAt: &done, RunAttempt: 2},
		{GitHubJobID: 8, GitHubRunID: 105, Repo: "acme/widgets", JobName: "old", State: JobCompleted, Conclusion: "failure", QueuedAt: old, StartedAt: &oldStart, CompletedAt: &oldDone, RunAttempt: 1},
		{GitHubJobID: 9, GitHubRunID: 106, Repo: "acme/widgets", JobName: "cancelled", State: JobCompleted, Conclusion: "cancelled", QueuedAt: now.Add(-2 * time.Hour), StartedAt: &started, CompletedAt: &done, RunAttempt: 1},
		{GitHubJobID: 10, GitHubRunID: 107, Repo: "acme/widgets", JobName: "other-run", State: JobCompleted, Conclusion: "failure", QueuedAt: now.Add(-90 * time.Minute), StartedAt: &started, CompletedAt: &done, RunAttempt: 1},
	}
	for _, j := range seed {
		if _, err := s.UpsertJob(ctx, j); err != nil {
			t.Fatalf("seeding %s (job %d): %v", j.JobName, j.GitHubJobID, err)
		}
	}
	fleet, err := s.GetJobByGitHubID(ctx, 2)
	if err != nil {
		t.Fatalf("GetJobByGitHubID: %v", err)
	}
	if _, _, err := s.SetJobRunnerFault(ctx, fleet.ID, "runner exited with code 137", FaultOutOfMemory); err != nil {
		t.Fatalf("SetJobRunnerFault: %v", err)
	}
	known, err := s.GetJobByGitHubID(ctx, 3)
	if err != nil {
		t.Fatalf("GetJobByGitHubID: %v", err)
	}
	if _, _, err := s.AcknowledgeJob(ctx, known.ID, "alice"); err != nil {
		t.Fatalf("AcknowledgeJob: %v", err)
	}
	still, err := s.GetJobByGitHubID(ctx, 7)
	if err != nil {
		t.Fatalf("GetJobByGitHubID: %v", err)
	}
	if still.RunAttempt != 2 {
		t.Fatalf("the later attempt did not survive the upsert: %+v", still)
	}

	got, total, err := s.ListJobs(ctx, JobFilter{AttentionOnly: true}, Page{Sort: "queued_at", Desc: true})
	if err != nil {
		t.Fatalf("ListJobs attention: %v", err)
	}
	var names []string
	for _, j := range got {
		names = append(names, j.JobName)
	}
	want := []string{"recent-fleet", "other-run", "recent-failure", "still-failing"}
	if total != len(want) || len(names) != len(want) {
		t.Fatalf("attention listed %v (total %d), want %v", names, total, want)
	}
	for i, name := range want {
		if names[i] != name {
			t.Fatalf("attention[%d] = %q, want %q (order: newest queued first)", i, names[i], name)
		}
	}

	// The acknowledged job is still a failure and still in the failed view;
	// only the attention view has stopped asking. The superseded attempt is
	// in it too, because history is not what the attention view is for.
	if _, total, err := s.ListJobs(ctx, JobFilter{FailedOnly: true}, Page{}); err != nil || total != 8 {
		t.Fatalf("failed view total = %d (err %v), want 8", total, err)
	}
	if left, _, err := s.ListJobs(ctx, JobFilter{AttentionOnly: true, Repos: []string{"nope"}}, Page{}); err != nil || len(left) != 0 {
		t.Fatalf("attention for another repository returned %v (err %v), want none", left, err)
	}

	// The same question read at the run's level, which is what the Workflows
	// page's "Needs attention" view asks: a run is kept when any of its jobs
	// is one of these, and the run whose second attempt passed is not.
	runs, runTotal, err := s.ListWorkflowRuns(ctx, JobFilter{AttentionOnly: true}, Page{})
	if err != nil {
		t.Fatalf("ListWorkflowRuns attention: %v", err)
	}
	var runIDs []int64
	for _, r := range runs {
		runIDs = append(runIDs, r.GitHubRunID)
	}
	slices.Sort(runIDs)
	if runTotal != 4 || !slices.Equal(runIDs, []int64{100, 101, 104, 107}) {
		t.Fatalf("attention runs = %v (total %d), want 100, 101, 104 and 107", runIDs, runTotal)
	}
}

// An acknowledgement names the person who looked, and the first one wins: a
// second press of the button is not a second triage. Clearing it asks the
// question again, and a re-delivery of the same webhook must not silently
// erase either decision.
func TestAcknowledgingKeepsTheFirstNameAndSurvivesRedelivery(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	later := now.Add(time.Hour)
	clock := now
	s := newTestStoreAt(t, func() time.Time { return clock })
	ctx := context.Background()
	done := now.Add(-time.Minute)

	j, err := s.UpsertJob(ctx, &Job{GitHubJobID: 1, GitHubRunID: 10, Repo: "acme/widgets", JobName: "red", State: JobCompleted, Conclusion: "failure", QueuedAt: now.Add(-time.Hour), CompletedAt: &done})
	if err != nil {
		t.Fatalf("UpsertJob: %v", err)
	}

	got, recorded, err := s.AcknowledgeJob(ctx, j.ID, "alice")
	if err != nil || !recorded {
		t.Fatalf("AcknowledgeJob = %+v, %v, %v; want the first acknowledgement recorded", got, recorded, err)
	}
	if got.AcknowledgedAt == nil || !got.AcknowledgedAt.Equal(now) || got.AcknowledgedBy != "alice" {
		t.Fatalf("acknowledgement = %v by %q, want %v by alice", got.AcknowledgedAt, got.AcknowledgedBy, now)
	}

	clock = later
	got, recorded, err = s.AcknowledgeJob(ctx, j.ID, "bob")
	if err != nil || recorded {
		t.Fatalf("second AcknowledgeJob recorded = %v (err %v), want false", recorded, err)
	}
	if got.AcknowledgedBy != "alice" || !got.AcknowledgedAt.Equal(now) {
		t.Fatalf("a second acknowledgement replaced the first: %v by %q", got.AcknowledgedAt, got.AcknowledgedBy)
	}

	if _, _, err := s.ApplyJob(ctx, &Job{GitHubJobID: 1, State: JobCompleted, Conclusion: "failure", CompletedAt: &done}); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	got, err = s.GetJob(ctx, j.ID)
	if err != nil || got.AcknowledgedBy != "alice" {
		t.Fatalf("a redelivery erased the acknowledgement: %+v (err %v)", got, err)
	}

	got, recorded, err = s.ClearJobAcknowledgement(ctx, j.ID)
	if err != nil || !recorded || got.AcknowledgedAt != nil || got.AcknowledgedBy != "" {
		t.Fatalf("ClearJobAcknowledgement = %+v, %v, %v; want the acknowledgement gone", got, recorded, err)
	}
	if _, recorded, err := s.ClearJobAcknowledgement(ctx, j.ID); err != nil || recorded {
		t.Fatalf("clearing twice recorded = %v (err %v), want false", recorded, err)
	}

	if _, _, err := s.AcknowledgeJob(ctx, "job_missing", "alice"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("acknowledging a job that does not exist: %v, want ErrNotFound", err)
	}
}

// An empty name is what an instance with authentication off has, and it must
// not turn into a person called "": the drawer reads the timestamp alone.
func TestAcknowledgingWithoutANameStillRecordsTheMoment(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	j, err := s.UpsertJob(ctx, &Job{GitHubJobID: 1, State: JobCompleted, Conclusion: "failure"})
	if err != nil {
		t.Fatalf("UpsertJob: %v", err)
	}
	got, recorded, err := s.AcknowledgeJob(ctx, j.ID, "")
	if err != nil || !recorded {
		t.Fatalf("AcknowledgeJob = %v, %v; want recorded", recorded, err)
	}
	if got.AcknowledgedAt == nil {
		t.Fatal("an acknowledgement with no name lost its time")
	}
}

func TestAttentionFollowsNewRunsWithoutCrossingBranchesOrJobs(t *testing.T) {
	for _, tc := range []struct {
		name, branch, workflow, job, conclusion string
		wantOld                                 bool
	}{
		{"fixed on new commit", "feature", "CI", "Test", "success", false},
		{"new failure replaces old", "feature", "CI", "Test", "failure", false},
		{"cancelled is not a fix", "feature", "CI", "Test", "cancelled", true},
		{"skipped is not a fix", "feature", "CI", "Test", "skipped", true},
		{"neutral is not a fix", "feature", "CI", "Test", "neutral", true},
		{"newer run in progress", "feature", "CI", "Test", "", false},
		{"other branch", "other", "CI", "Test", "success", true},
		{"other workflow", "feature", "Deploy", "Test", "success", true},
		{"other matrix job", "feature", "CI", "Test (arm)", "success", true},
		{"missing branch", "", "CI", "Test", "success", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now().UTC()
			s := newTestStoreAt(t, func() time.Time { return now })
			ctx := context.Background()
			old := &Job{GitHubJobID: 1, GitHubRunID: 1, Repo: "acme/repo", Workflow: "CI", JobName: "Test", HeadBranch: "feature", State: JobCompleted, Conclusion: "failure", QueuedAt: now.Add(-time.Hour)}
			if _, err := s.UpsertJob(ctx, old); err != nil {
				t.Fatal(err)
			}
			later := &Job{GitHubJobID: 2, GitHubRunID: 2, Repo: "acme/repo", Workflow: tc.workflow, JobName: tc.job, HeadBranch: tc.branch, State: JobCompleted, Conclusion: tc.conclusion, QueuedAt: now.Add(-time.Minute)}
			if tc.conclusion == "" {
				later.State = JobInProgress
			}
			if _, err := s.UpsertJob(ctx, later); err != nil {
				t.Fatal(err)
			}
			rows, _, err := s.ListJobs(ctx, JobFilter{AttentionOnly: true}, Page{})
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, row := range rows {
				if row.GitHubJobID == 1 {
					found = true
				}
			}
			if found != tc.wantOld {
				t.Fatalf("old failure visible = %v, want %v", found, tc.wantOld)
			}
		})
	}
}

func TestGitHubAttentionObservationsDoNotAcknowledgeOrHideNewFailures(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	s := newTestStoreAt(t, func() time.Time { return now })
	ctx := context.Background()
	completed := now.Add(-time.Minute)
	job := &Job{GitHubJobID: 1, GitHubRunID: 1, Repo: "acme/repo", State: JobCompleted, Conclusion: "failure", QueuedAt: now.Add(-time.Hour), CompletedAt: &completed}
	if _, err := s.UpsertJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	assertCount := func(want int) {
		t.Helper()
		_, n, err := s.ListJobs(ctx, JobFilter{AttentionOnly: true}, Page{})
		if err != nil || n != want {
			t.Fatalf("attention count = %d, error %v; want %d", n, err, want)
		}
	}
	assertCount(1)
	for _, reason := range []string{"closed", "superseded", "fixed"} {
		if err := s.RecordJobAttentionRun(ctx, job.Repo, 1, reason, now, true); err != nil {
			t.Fatal(err)
		}
		assertCount(0)
	}
	// A reopened PR becomes actionable without undoing a person's triage.
	if err := s.RecordJobAttentionRun(ctx, job.Repo, 1, "", now, true); err != nil {
		t.Fatal(err)
	}
	assertCount(1)
	if err := s.RecordJobAttentionRun(ctx, job.Repo, 1, "fixed", now, true); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	completed = now
	fresh := &Job{GitHubJobID: 2, GitHubRunID: 1, Repo: job.Repo, State: JobCompleted, Conclusion: "failure", QueuedAt: now, CompletedAt: &completed}
	if _, err := s.UpsertJob(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	assertCount(1)
	// A network failure must not advance the evidence timestamp and apply an
	// old success to a failure that arrived after it.
	if err := s.RecordJobAttentionRun(ctx, job.Repo, 1, "", now, false); err != nil {
		t.Fatal(err)
	}
	assertCount(1)
	all, total, err := s.ListJobs(ctx, JobFilter{FailedOnly: true}, Page{})
	if err != nil || total != 2 {
		t.Fatalf("history = %d, %v", total, err)
	}
	for _, row := range all {
		if row.AcknowledgedAt != nil {
			t.Fatal("GitHub observation forged an acknowledgement")
		}
	}
	if _, err := s.PruneJobAttentionRuns(ctx); err != nil {
		t.Fatal(err)
	}
}

// legacyUnreplacedJobSQL is the replacement test as it was first written: one
// NOT EXISTS with the two kinds of later job joined by an OR. It is kept here,
// and only here, as the definition the split into two has to agree with. The
// arguments leave out one half or the other, so a test can tell that a fixture
// really contains both kinds.
func legacyUnreplacedJobSQL(laterAttempt, laterRun bool) string {
	var kinds []string
	if laterAttempt {
		kinds = append(kinds, `(later.github_run_id = jobs.github_run_id AND later.github_run_id != 0
				AND later.run_attempt > jobs.run_attempt)`)
	}
	if laterRun {
		kinds = append(kinds, `(jobs.github_run_id != 0 AND later.github_run_id != 0
				AND jobs.head_branch != '' AND jobs.workflow != ''
				AND later.head_branch = jobs.head_branch AND later.workflow = jobs.workflow
				AND later.github_run_id != jobs.github_run_id
				AND later.queued_at > jobs.queued_at)`)
	}
	return `NOT EXISTS (
		SELECT 1 FROM jobs later
		WHERE later.repo = jobs.repo AND later.job_name = jobs.job_name
			AND (later.state != 'completed' OR later.conclusion NOT IN ('cancelled','skipped','neutral')
				OR later.runner_fault != '' OR later.fault_kind != '')
			AND (` + strings.Join(kinds, " OR ") + `))`
}

// The replacement test was split in two so that SQLite can find a later
// attempt through the run and a later run through the branch. NOT EXISTS (A OR
// B) is NOT EXISTS (A) AND NOT EXISTS (B) on paper; this holds the SQL to it,
// on a small pseudo-random fleet whose few repositories, runs, branches and
// workflows collide constantly, so every combination of the two kinds of later
// job, the guards on missing metadata and the jobs that do not count as a
// replacement turn up many times over.
func TestSplittingTheReplacementTestKeepsTheSameJobs(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	s := newTestStoreAt(t, func() time.Time { return now })
	ctx := context.Background()
	rng := rand.New(rand.NewPCG(7, 11))
	pick := func(from ...string) string { return from[rng.IntN(len(from))] }
	for i := 1; i <= 600; i++ {
		state, conclusion := "completed", pick("success", "failure", "failure", "cancelled", "skipped", "neutral", "timed_out")
		if rng.IntN(8) == 0 {
			state, conclusion = pick("queued", "in_progress", "waiting"), ""
		}
		fault, kind := "", ""
		if rng.IntN(10) == 0 {
			fault, kind = "runner exited with code 137", "out_of_memory"
		}
		_, err := s.write.ExecContext(ctx, `INSERT INTO jobs
			(id, github_job_id, github_run_id, repo, workflow, job_name, head_branch, run_attempt,
			 state, conclusion, runner_fault, fault_kind, queued_at)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			fmt.Sprintf("job_%d", i), i, rng.IntN(6), pick("acme/a", "acme/b"), pick("", "CI", "CI", "Deploy"),
			pick("Test", "Lint"), pick("", "main", "main", "feature"), rng.IntN(4), state, conclusion, fault, kind,
			ms(now.Add(-time.Duration(rng.IntN(12))*time.Minute)))
		if err != nil {
			t.Fatalf("seeding job %d: %v", i, err)
		}
	}
	unreplaced := func(test string) []int64 {
		t.Helper()
		rows, err := s.read.QueryContext(ctx, `SELECT github_job_id FROM jobs WHERE `+test+` ORDER BY github_job_id`)
		if err != nil {
			t.Fatalf("%v in %s", err, test)
		}
		defer rows.Close()
		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
		}
		return ids
	}

	single, split := unreplaced(legacyUnreplacedJobSQL(true, true)), unreplaced(unreplacedJobSQL())
	if !slices.Equal(single, split) {
		t.Fatalf("splitting the replacement test changed which jobs are unreplaced: %d before, %d after", len(single), len(split))
	}
	// A fixture in which only one kind of later job ever mattered would pass
	// the comparison above whatever the second half did.
	byAttempt, byRun := unreplaced(legacyUnreplacedJobSQL(true, false)), unreplaced(legacyUnreplacedJobSQL(false, true))
	if len(single) == 0 || len(byAttempt) <= len(single) || len(byRun) <= len(single) {
		t.Fatalf("the fixture does not need both kinds of later job: %d unreplaced, %d if only a later attempt replaced, %d if only a later run did",
			len(single), len(byAttempt), len(byRun))
	}
}

// Every check of a run costs GitHub requests the runner registrations share the
// installation's quota with, and the loop makes a fixed number of checks an
// hour whenever enough runs are waiting, so what it spends them on matters. A
// run GitHub has settled (its PR closed, a newer commit, a passing re-run) is
// looked at again only in case that changes, and an hour is soon enough for a
// PR to be reopened; a run it has not settled, or has said nothing about, is
// the one a person is waiting on and keeps the short interval. A failure newer
// than the observation is not settled by it, so it is not made to wait either.
func TestSettledRunsAreCheckedAgainHourlyAndTheRestEveryFiveMinutes(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Millisecond)
	clock := start
	s := newTestStoreAt(t, func() time.Time { return clock })
	ctx := context.Background()
	failure := func(id, run int64, completed time.Time) {
		t.Helper()
		job := &Job{GitHubJobID: id, GitHubRunID: run, Repo: "acme/repo", State: JobCompleted, Conclusion: "failure",
			QueuedAt: completed.Add(-time.Minute), CompletedAt: &completed}
		if _, err := s.UpsertJob(ctx, job); err != nil {
			t.Fatalf("UpsertJob: %v", err)
		}
	}
	earlier := start.Add(-10 * time.Minute)
	failure(1, 1, earlier) // closed
	failure(2, 2, earlier) // checked, still failing
	failure(3, 3, earlier) // fixed, and then a new failure
	failure(4, 4, earlier) // never checked
	for run, reason := range map[int64]string{1: "closed", 2: "", 3: "fixed"} {
		if err := s.RecordJobAttentionRun(ctx, "acme/repo", run, reason, start, true); err != nil {
			t.Fatalf("RecordJobAttentionRun: %v", err)
		}
	}
	failure(5, 3, start.Add(30*time.Second))

	dueAt := func(after time.Duration) []int64 {
		t.Helper()
		clock = start.Add(after)
		runs, err := s.JobAttentionRunsToCheck(ctx, 10)
		if err != nil {
			t.Fatalf("JobAttentionRunsToCheck: %v", err)
		}
		var ids []int64
		for _, r := range runs {
			ids = append(ids, r.RunID)
		}
		slices.Sort(ids)
		return ids
	}
	if got := dueAt(time.Minute); !slices.Equal(got, []int64{4}) {
		t.Fatalf("a minute later the runs due are %v, want only the one never checked", got)
	}
	if got := dueAt(6 * time.Minute); !slices.Equal(got, []int64{2, 3, 4}) {
		t.Fatalf("six minutes later the runs due are %v, want every run but the closed one", got)
	}
	if got := dueAt(59 * time.Minute); !slices.Equal(got, []int64{2, 3, 4}) {
		t.Fatalf("just under an hour later the runs due are %v, want the closed run still left alone", got)
	}
	if got := dueAt(61 * time.Minute); !slices.Equal(got, []int64{1, 2, 3, 4}) {
		t.Fatalf("an hour later the runs due are %v, want the closed run looked at again", got)
	}
}

// Finding 65: the prune seeks idx_jobs_run for each record. Written plainly it
// walked idx_jobs_repo instead, every job of the repository in the window per
// record, and took 75 s on the live controller.
func TestPruningJobAttentionRunsSeeksTheRunIndex(t *testing.T) {
	s := newTestStore(t)
	rows, err := s.read.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+pruneJobAttentionRunsSQL, int64(0))
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
	joined := strings.Join(plan, "\n")
	if !strings.Contains(joined, "idx_jobs_run") || strings.Contains(joined, "idx_jobs_repo") {
		t.Fatalf("the prune does not seek idx_jobs_run:\n%s", joined)
	}
}

// The prune forgets exactly the records of runs with no job left in the
// window, and says how many.
func TestPruningJobAttentionRunsKeepsRunsInTheWindow(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := s.Now()
	recent := &Job{GitHubJobID: 101, GitHubRunID: 1, Repo: "acme/repo", State: JobCompleted, Conclusion: "failure", QueuedAt: now.Add(-time.Hour)}
	old := &Job{GitHubJobID: 102, GitHubRunID: 2, Repo: "acme/repo", State: JobCompleted, Conclusion: "failure", QueuedAt: now.Add(-AttentionWindow - time.Hour)}
	for _, j := range []*Job{recent, old} {
		if _, err := s.UpsertJob(ctx, j); err != nil {
			t.Fatal(err)
		}
	}
	for _, rec := range []struct {
		repo string
		run  int64
	}{{"acme/repo", 1}, {"acme/repo", 2}, {"acme/repo", 3}, {"other/repo", 1}} {
		if err := s.RecordJobAttentionRun(ctx, rec.repo, rec.run, "", now, true); err != nil {
			t.Fatal(err)
		}
	}
	n, err := s.PruneJobAttentionRuns(ctx)
	if err != nil || n != 3 {
		t.Fatalf("pruned %d, %v; want the three with no job in the window", n, err)
	}
	var left []string
	rows, err := s.read.QueryContext(ctx, `SELECT repo || '#' || run_id FROM job_attention_runs ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatal(err)
		}
		left = append(left, k)
	}
	if len(left) != 1 || left[0] != "acme/repo#1" {
		t.Fatalf("left %v, want only acme/repo#1", left)
	}
}
