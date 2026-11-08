package store

import (
	"context"
	"time"
)

type JobAttentionRun struct {
	Repo  string
	RunID int64
}

// How long a run is left alone before GitHub is asked about it again.
//
// Every check costs several requests against the installation's quota, which
// runner registration shares, and the loop makes a fixed number of checks an
// hour once enough runs are waiting; so the checks go to the runs a person is
// waiting on. A run GitHub has settled is looked at again in case that changes
// (a closed PR can reopen, checks can be run again) but an hour is soon enough
// for that, and it is most of the runs in a fleet with a week of failures.
const (
	attentionRecheck        = 5 * time.Minute
	attentionSettledRecheck = time.Hour
)

// JobAttentionRunsToCheck rotates through runs, including ones previously
// settled on GitHub: a closed PR can reopen and checks can be run again.
// Those wait longer between checks than the rest, so that they do not use
// up the quota the rest are checked with.
func (s *Store) JobAttentionRunsToCheck(ctx context.Context, limit int) ([]JobAttentionRun, error) {
	now := s.Now()
	rows, err := s.read.QueryContext(ctx, jobAttentionRunsToCheckSQL(),
		ms(now.Add(-AttentionWindow)), ms(now.Add(-attentionSettledRecheck)), ms(now.Add(-attentionRecheck)), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []JobAttentionRun{}
	for rows.Next() {
		var r JobAttentionRun
		if err := rows.Scan(&r.Repo, &r.RunID); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// jobAttentionRunsToCheckSQL is a function so that a test can ask SQLite how it
// reads it: the replacement test inside is the part that is expensive to get
// wrong, and a plan is only visible from the statement.
func jobAttentionRunsToCheckSQL() string {
	return `SELECT jobs.repo, jobs.github_run_id
		FROM jobs LEFT JOIN job_attention_runs a ON a.repo=jobs.repo AND a.run_id=jobs.github_run_id
		WHERE jobs.github_run_id != 0 AND jobs.acknowledged_at IS NULL
			AND jobs.queued_at >= ? AND ` + failedJobSQL() + ` AND ` + unreplacedJobSQL() + `
			AND COALESCE(a.attempted_at,0) < CASE WHEN ` + settledOnGitHubSQL("a", "jobs") + ` THEN ? ELSE ? END
		GROUP BY jobs.repo, jobs.github_run_id
		ORDER BY COALESCE(a.attempted_at,0), MAX(jobs.queued_at) DESC, jobs.repo, jobs.github_run_id LIMIT ?`
}

// RecordJobAttentionRun records only evidence, never an acknowledgement. On
// a failed read, retain the last known disposition and rotate to the next run.
func (s *Store) RecordJobAttentionRun(ctx context.Context, repo string, run int64, reason string, checked time.Time, known bool) error {
	_, err := s.exec(ctx, `INSERT INTO job_attention_runs(repo,run_id,reason,checked_at,attempted_at) VALUES(?,?,?,CASE WHEN ? THEN ? ELSE 0 END,?)
		ON CONFLICT(repo,run_id) DO UPDATE SET reason=CASE WHEN ? THEN excluded.reason ELSE job_attention_runs.reason END,
		checked_at=CASE WHEN ? THEN excluded.checked_at ELSE job_attention_runs.checked_at END,
		attempted_at=excluded.attempted_at WHERE excluded.attempted_at >= job_attention_runs.attempted_at`, repo, run, reason, known, ms(checked), ms(checked), known, known)
	return err
}

// pruneJobAttentionRunsSQL is a const so a query-plan test can pin it. The
// subquery must seek idx_jobs_run by run and repository: written plainly, the
// planner preferred idx_jobs_repo (repository and queued_at) and walked every
// job of the repository in the window for each row, which took 75 s on the
// live controller's 1,457 rows (8 Nov 2026) and never finished inside the
// sweep's budget. The unary plus keeps queued_at out of the index choice.
const pruneJobAttentionRunsSQL = `DELETE FROM job_attention_runs WHERE NOT EXISTS
	(SELECT 1 FROM jobs WHERE jobs.github_run_id=job_attention_runs.run_id AND jobs.repo=job_attention_runs.repo
	AND +jobs.queued_at >= ?)`

// PruneJobAttentionRuns forgets the GitHub checks of runs with no job left in
// the attention window, and says how many it forgot.
func (s *Store) PruneJobAttentionRuns(ctx context.Context) (int64, error) {
	res, err := s.exec(ctx, pruneJobAttentionRunsSQL, ms(s.Now().Add(-AttentionWindow)))
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return n, nil
}
