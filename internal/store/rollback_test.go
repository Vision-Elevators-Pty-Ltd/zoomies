package store

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// downSQL is a rollback script's statements, without the sqlite3 shell's dot
// commands and comments, as the shell would run them.
func downSQL(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var keep []string
	for _, line := range strings.Split(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") || strings.HasPrefix(trimmed, ".") {
			continue
		}
		keep = append(keep, line)
	}
	return strings.Join(keep, "\n")
}

// Review of #20, blocker 3: a build refuses a database whose ledger names a
// migration it does not have, so rolling back from vision.10 needs the down
// script. After it the database is the schema before 0135, column and ledger
// row both gone, and opening it with this build again re-applies 0135 as on
// any older database. (The proof with the real vision.9 binary is in the PR.)
func TestTheDownScriptFor0135TakesTheSchemaBackAndItReapplies(t *testing.T) {
	path := t.TempDir() + "/zoomies.db"
	ctx := context.Background()
	s, err := Open(ctx, Options{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	usageJob(t, s, 1, "build", "pool_a", "run_1", JobInProgress, versionsEpoch)
	if err := s.RecordJobWorkingSet(ctx, "run_1", 4096, true, true, versionsEpoch); err != nil {
		t.Fatal(err)
	}
	// From vision.11, 0136's script first, as its own comment says.
	if _, err := s.write.ExecContext(ctx, downSQL(t, "../../deploy/rollback/0136_job_working_set_final.down.sql")); err != nil {
		t.Fatalf("the 0136 down script failed: %v", err)
	}
	if _, err := s.write.ExecContext(ctx, downSQL(t, "../../deploy/rollback/0135_job_working_set_peak.down.sql")); err != nil {
		t.Fatalf("the down script failed: %v", err)
	}
	cols := map[string]bool{}
	rows, err := s.read.QueryContext(ctx, `SELECT name FROM pragma_table_info('jobs')`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		cols[n] = true
	}
	rows.Close()
	if cols["peak_working_set_mb"] || !cols["peak_memory_mb"] {
		t.Fatalf("after the down script the jobs columns are %v", cols)
	}
	applied, err := s.AppliedMigrations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if last := applied[len(applied)-1].Name; last != "0134_kennel_fix_publications.sql" {
		t.Fatalf("after the down script the ledger ends at %s, want 0134", last)
	}
	var jobs int
	if err := s.read.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs`).Scan(&jobs); err != nil || jobs != 1 {
		t.Fatalf("the down script lost jobs: %d, %v", jobs, err)
	}
	s.Close()

	again, err := Open(ctx, Options{Path: path})
	if err != nil {
		t.Fatalf("reopening after the down script: %v", err)
	}
	defer again.Close()
	applied, err = again.AppliedMigrations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if last := applied[len(applied)-1].Name; last != "0136_job_working_set_final.sql" {
		t.Fatalf("reopening did not re-apply 0135 and 0136: ledger ends at %s", last)
	}
}

// Review 3 of #20: 0136's down script takes the database back to what
// vision.10 opens (column, table and ledger row gone, every job kept), and
// reopening re-applies 0136, whose one-off re-check marks untrusted (-1) every
// working-set peak recorded without a final verdict, as vision.10 recorded
// them all, and leaves the old figure and a sealed peak alone. A second
// application changes nothing.
func TestTheDownScriptFor0136TakesTheSchemaBackAndReapplyingInvalidatesUnsealedPeaks(t *testing.T) {
	path := t.TempDir() + "/zoomies.db"
	ctx := context.Background()
	s, err := Open(ctx, Options{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	usageJob(t, s, 1, "build", "pool_a", "run_1", JobInProgress, versionsEpoch)
	usageJob(t, s, 2, "build", "pool_a", "run_2", JobInProgress, versionsEpoch)
	if err := s.RecordJobUsage(ctx, "run_1", 1, 8192); err != nil {
		t.Fatal(err)
	}
	if err := s.SetWorkflowRunPath(ctx, "eyupio/zoomies", 77, ".github/workflows/ci.yml"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.write.ExecContext(ctx, downSQL(t, "../../deploy/rollback/0136_job_working_set_final.down.sql")); err != nil {
		t.Fatalf("the down script failed: %v", err)
	}
	var tables int
	if err := s.read.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE name = 'workflow_run_paths'`).Scan(&tables); err != nil || tables != 0 {
		t.Fatalf("workflow_run_paths left behind: %d, %v", tables, err)
	}
	applied, err := s.AppliedMigrations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if last := applied[len(applied)-1].Name; last != "0135_job_working_set_peak.sql" {
		t.Fatalf("after the down script the ledger ends at %s, want 0135", last)
	}
	// What vision.10 records: a trusted peak with no verdict.
	if _, err := s.write.ExecContext(ctx, `UPDATE jobs SET peak_working_set_mb = 4096 WHERE runner_id = 'run_1'`); err != nil {
		t.Fatal(err)
	}
	s.Close()

	reopen := func() *Store {
		t.Helper()
		again, err := Open(ctx, Options{Path: path})
		if err != nil {
			t.Fatalf("reopening: %v", err)
		}
		return again
	}
	again := reopen()
	var ws, old, final int64
	if err := again.read.QueryRowContext(ctx, `SELECT peak_working_set_mb, peak_memory_mb, working_set_final FROM jobs WHERE runner_id = 'run_1'`).Scan(&ws, &old, &final); err != nil {
		t.Fatal(err)
	}
	if ws != -1 || old != 8192 || final != 0 {
		t.Fatalf("after 0136 the vision.10 peak is %d (old %d, final %d), want -1 beside the old 8192", ws, old, final)
	}
	// The standalone re-check, run twice: a completed job's unsealed peak
	// becomes -1, a sealed one is left alone.
	if err := again.RecordJobWorkingSet(ctx, "run_2", 2048, true, true, versionsEpoch); err != nil {
		t.Fatal(err)
	}
	usageJob(t, again, 3, "build", "pool_a", "run_3", JobCompleted, versionsEpoch)
	if _, err := again.write.ExecContext(ctx, `UPDATE jobs SET peak_working_set_mb = 3000 WHERE runner_id = 'run_3'`); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int{2, 3} {
		done := versionsEpoch.Add(time.Hour)
		if _, _, err := again.ApplyJob(ctx, &Job{GitHubJobID: int64(5000 + id), State: JobCompleted, Conclusion: "success", CompletedAt: &done}); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if _, err := again.write.ExecContext(ctx, downSQL(t, "../../deploy/maintenance/working_set_recheck.sql")); err != nil {
			t.Fatalf("the re-check failed: %v", err)
		}
	}
	for runner, want := range map[string]int64{"run_2": 2048, "run_3": -1} {
		if err := again.read.QueryRowContext(ctx, `SELECT peak_working_set_mb FROM jobs WHERE runner_id = ?`, runner).Scan(&ws); err != nil || ws != want {
			t.Fatalf("after the re-check %s reads %d (%v), want %d", runner, ws, err, want)
		}
	}
	again.Close()
}
