package store

import (
	"context"
	"os"
	"strings"
	"testing"
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
	if err := s.RecordJobWorkingSet(ctx, "run_1", 4096, true, versionsEpoch); err != nil {
		t.Fatal(err)
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
	if last := applied[len(applied)-1].Name; last != "0135_job_working_set_peak.sql" {
		t.Fatalf("reopening did not re-apply 0135: ledger ends at %s", last)
	}
}
