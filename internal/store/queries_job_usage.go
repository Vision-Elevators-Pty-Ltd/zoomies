package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// JobUsageKey names one job the way its history is kept: the same job of the
// same workflow in the same repository, run by the same pool. The pool is part
// of it because the same job on a docker-in-docker pool measures its daemon
// too, and on a pool with a smaller limit is capped by it.
type JobUsageKey struct {
	Repo     string `json:"repo"`
	Workflow string `json:"workflow"`
	JobName  string `json:"job_name"`
	PoolID   string `json:"pool_id"`
}

// JobPeak is one finished run of a job: the most it was measured using, and
// whether the kernel killed it for memory.
type JobPeak struct {
	CPUs      float64 `json:"cpus"`
	MemoryMB  int64   `json:"memory_mb"`
	OOMKilled bool    `json:"oom_killed"`
	// GrantedMemoryMB is the memory limit the run was killed at, when it was killed.
	// The sampled peak is the most a 30-second sample saw, so a run killed by a
	// spike between two samples can read well under the limit it hit, and the
	// limit is the one figure that is known to have been too little.
	GrantedMemoryMB int64 `json:"granted_memory_mb,omitempty"`
	// WorkingSetMB is the run's working-set peak (peak_working_set_mb): its
	// usage less all reclaimable page cache. Zero for a run measured only the
	// old way, and below zero for one whose working set is not trusted (the
	// readings had a gap, failed, or the runner was not ephemeral): both keep
	// the old figure. PreferWorkingSet decides which figure MemoryMB carries.
	WorkingSetMB int64 `json:"-"`
	// WorkingSetFinal is whether the agent's final verdict on the run's
	// coverage was recorded with the peak trusted (working_set_final). Without
	// it a positive WorkingSetMB is provisional and is not read.
	WorkingSetFinal bool `json:"-"`
	// WorkflowPath is the workflow file the run was of, where the run lookup
	// has said (workflow_run_paths); empty where it has not.
	WorkflowPath string `json:"-"`
	// SharedName is whether another job of the same run attempt had the same
	// job name: two definitions, or matrix legs, that a display name does not
	// tell apart.
	SharedName bool `json:"-"`
	// Job names the job the run was of (repo, workflow and job name), for a
	// pool's history, which mixes jobs: PreferWorkingSet decides each job's
	// runs on their own. Empty where the runs are all of one job.
	Job string `json:"-"`
}

// WorkingSetMinRuns is how many runs of a job, or of a pool, must have a
// working-set peak before its history is read from those runs alone. It is
// scheduler.BookingMinSamples, the fewest runs a booking is taken from, held
// equal by a test: a history that switches must still be one a booking can be
// taken from.
const WorkingSetMinRuns = 5

// PreferWorkingSet is runs as the history should read them under
// scheduler.working_set_history = on, newest first as given, decided job by
// job (JobPeak.Job; runs with none are one job). The store's histories return
// every run at its usage peak (MemoryMB is peak_memory_mb) and never call it;
// the controller does, and only with the setting on, so with it off the
// history is exactly what it was before the working set was recorded.
//
// A run is measured the new way (WorkingSetMeasured) only where its peak is
// trusted and final, and its job is identified by more than display names: the
// run lookup named its workflow file, and no other job of the run shared its
// name.
//
// A job switches once at least WorkingSetMinRuns of its runs are measured the
// new way, and only if every run of it that names a workflow file names the
// same one: display names two workflow files share are one job to the history,
// and must not let one's light runs speak for the other's heavy ones. In a job
// that switches, a measured run's MemoryMB becomes its working set, and every
// other run is kept at its old figure, which is never smaller (an untrusted or
// provisional peak, a run on an older agent, a run whose job is not
// identified), except a run measured only the old way that is older than every
// measured run and proven to be of the same definition as the measured runs:
// it names their workflow file and no other job of its run shared its name. That
// is a record from before the working set was measured at all, which the
// measured runs since have replaced. An older run whose workflow file is
// unknown, or whose name was shared, may be another definition under the same
// display names, and the measured runs say nothing of it: it keeps its old
// figure, however heavy, until it ages out of the history (review of #22,
// finding 2: five identified 512 MiB runs had erased fifteen unidentified
// 16 GiB ones). A job that does not switch keeps every run as it was.
//
// A run the kernel killed for memory is always kept as it was: the limit it
// hit is the one figure known to have been too little.
func PreferWorkingSet(peaks []JobPeak) []JobPeak {
	type cohort struct {
		measured int
		oldest   int // index of the oldest measured run
		path     string
		paths    int
	}
	jobs := map[string]*cohort{}
	for i, p := range peaks {
		c := jobs[p.Job]
		if c == nil {
			c = &cohort{}
			jobs[p.Job] = c
		}
		if p.WorkflowPath != "" && p.WorkflowPath != c.path {
			c.path = p.WorkflowPath
			c.paths++
		}
		if WorkingSetMeasured(p) {
			c.measured++
			c.oldest = i
		}
	}
	switches := func(c *cohort) bool { return c.measured >= WorkingSetMinRuns && c.paths == 1 }
	switched := false
	for _, c := range jobs {
		switched = switched || switches(c)
	}
	if !switched {
		return peaks
	}
	out := make([]JobPeak, 0, len(peaks))
	for i, p := range peaks {
		c := jobs[p.Job]
		switch {
		case !switches(c), p.OOMKilled:
			out = append(out, p)
		case WorkingSetMeasured(p):
			p.MemoryMB = p.WorkingSetMB
			out = append(out, p)
		case p.WorkingSetMB == 0 && i > c.oldest && p.WorkflowPath == c.path && !p.SharedName:
			// From before the working set was measured, and of the very
			// definition the measured runs are of: replaced.
		default:
			out = append(out, p)
		}
	}
	return out
}

// WorkingSetMeasured is whether a run counts as measured the new way: a
// trusted working-set peak with the agent's final verdict on its coverage, of
// a job identified by its workflow file and a name no other job of its run
// had, and not killed for memory.
func WorkingSetMeasured(p JobPeak) bool {
	return p.WorkingSetMB > 0 && p.WorkingSetFinal && p.WorkflowPath != "" && !p.SharedName && !p.OOMKilled
}

// JobUsageHistoryLimit is how many recent runs of one job a profile is taken
// from. Enough for a ninetieth percentile to mean something, few enough that a
// job whose needs changed a fortnight ago is profiled on what it is now.
const JobUsageHistoryLimit = 20

// RecordJobUsage raises the peaks of the job in progress on a runner to one
// sample of that runner's usage.
//
// Only a job GitHub says is in progress is touched, so the samples of a runner
// still starting or already finished are never charged to a job, and the
// state predicate is what lets the planner find the row through the queued and
// in-progress index rather than walking every job -- this runs on every
// heartbeat that carries a sample. MAX keeps a late or repeated sample from
// lowering a peak.
func (s *Store) RecordJobUsage(ctx context.Context, runnerID string, cpus float64, memoryMB int64) error {
	if runnerID == "" || (cpus <= 0 && memoryMB <= 0) {
		return nil
	}
	_, err := s.exec(ctx, `UPDATE jobs SET peak_cpus = MAX(peak_cpus, ?), peak_memory_mb = MAX(peak_memory_mb, ?)
		WHERE state = 'in_progress' AND runner_id = ?`, max(cpus, 0), max(memoryMB, 0), runnerID)
	return err
}

// WorkingSetLateReport is how long after its job completed a runner's report
// may still raise that job's working-set peak, or give it its final verdict:
// the agent's final report of an ended runner can arrive after GitHub has
// closed the job, and it carries the peak the job ended on. A verdict that
// arrives later leaves the run unsealed, so the history keeps it at its old
// figure; distrust is recorded however late it comes.
const WorkingSetLateReport = 15 * time.Minute

// RecordJobWorkingSet records one report of a runner's working set on its
// job, in MB.
//
// A trusted report raises the working-set peak of the job in progress on the
// runner or, where there is none, the one it completed within
// WorkingSetLateReport of now, and never lowers it; final (the agent's verdict
// on the runner's end) seals it (working_set_final), which is what lets the
// history read it at all (WorkingSetMeasured).
//
// trusted false marks the job's working set untrusted, for good (-1): a run
// whose readings had a gap or failed, or a runner that is not ephemeral, is
// never counted as measured the new way. It reaches the runner's job however
// late it comes (the job in progress, or else the runner's newest, which for
// an ephemeral runner is the only one it ran), so a verdict held through a
// disconnect still undoes a peak an earlier report made look good.
func (s *Store) RecordJobWorkingSet(ctx context.Context, runnerID string, memoryMB int64, trusted, final bool, now time.Time) error {
	if runnerID == "" || (memoryMB <= 0 && trusted) {
		return nil
	}
	if !trusted {
		_, err := s.exec(ctx, `UPDATE jobs SET peak_working_set_mb = -1
			WHERE id = (SELECT id FROM jobs WHERE runner_id = ?
				ORDER BY state = 'in_progress' DESC, queued_at DESC LIMIT 1)`, runnerID)
		return err
	}
	_, err := s.exec(ctx, `UPDATE jobs SET
			working_set_final = CASE WHEN peak_working_set_mb < 0 THEN working_set_final ELSE MAX(working_set_final, ?) END,
			peak_working_set_mb = CASE WHEN peak_working_set_mb < 0 THEN -1 ELSE MAX(peak_working_set_mb, ?) END
		WHERE id = (SELECT id FROM jobs WHERE runner_id = ?
			AND (state = 'in_progress' OR (state = 'completed' AND completed_at >= ?))
			ORDER BY state = 'in_progress' DESC, queued_at DESC LIMIT 1)`,
		boolInt(final), memoryMB, runnerID, ms(now.Add(-WorkingSetLateReport)))
	return err
}

// SetWorkflowRunPath records the workflow file a run was of, from the run
// lookup: what tells two workflows with one display name apart in a job's
// history (PreferWorkingSet). An empty path records nothing.
func (s *Store) SetWorkflowRunPath(ctx context.Context, repo string, runID int64, path string) error {
	if repo == "" || runID == 0 || path == "" {
		return nil
	}
	_, err := s.exec(ctx, `INSERT INTO workflow_run_paths (repo, github_run_id, path) VALUES (?, ?, ?)
		ON CONFLICT (repo, github_run_id) DO UPDATE SET path = excluded.path WHERE path != excluded.path`, repo, runID, path)
	return err
}

// WorkflowRunPath is the workflow file SetWorkflowRunPath recorded for a run,
// or "" where none has been.
func (s *Store) WorkflowRunPath(ctx context.Context, repo string, runID int64) (string, error) {
	var path string
	err := s.read.QueryRowContext(ctx, `SELECT path FROM workflow_run_paths WHERE repo = ? AND github_run_id = ?`, repo, runID).Scan(&path)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return path, err
}

// RecordJobHalfPeaks raises the per-container peaks of the job in progress on a runner
// to one sample of a docker-in-docker pair, in megabytes. It is the same statement
// shape as RecordJobUsage -- only a job GitHub says is in progress, MAX so a late sample
// never lowers a peak -- and is written only for a pair, so a pool that is not one pays
// nothing for it.
func (s *Store) RecordJobHalfPeaks(ctx context.Context, runnerID string, runnerMB, daemonMB int64) error {
	if runnerID == "" || (runnerMB <= 0 && daemonMB <= 0) {
		return nil
	}
	_, err := s.exec(ctx, `UPDATE jobs SET peak_runner_memory_mb = MAX(peak_runner_memory_mb, ?),
		peak_daemon_memory_mb = MAX(peak_daemon_memory_mb, ?)
		WHERE state = 'in_progress' AND runner_id = ?`, max(runnerMB, 0), max(daemonMB, 0), runnerID)
	return err
}

// MarkJobOOMKilled records that the kernel killed something in a runner for
// its memory limit, on the job that runner ran last, and returns that job and
// whether this call is the one that marked it.
//
// The job may already be over. A step killed with exit 137 leaves a runner
// that finishes the job, and the container reports the kill only when it
// exits -- after GitHub has already said the job failed, like any test. The
// fault is recorded all the same, because the whole point is that it was not
// the workflow's: the category is set when the job has none, and its sentence
// only then, so a runner fault recorded first keeps its own words.
func (s *Store) MarkJobOOMKilled(ctx context.Context, runnerID, message string) (*Job, bool, error) {
	if runnerID == "" {
		return nil, false, nil
	}
	var out *Job
	var marked bool
	err := s.tx(ctx, func(tx *sql.Tx) error {
		j, err := scanJob(tx.QueryRowContext(ctx, `SELECT `+jobCols+` FROM jobs WHERE runner_id = ?
			ORDER BY queued_at DESC LIMIT 1`, runnerID))
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if j.OOMKilled {
			out = j
			return nil
		}
		if _, err := tx.ExecContext(ctx, `UPDATE jobs SET oom_killed = 1,
			fault_kind = CASE WHEN fault_kind = '' THEN ? ELSE fault_kind END,
			runner_fault = CASE WHEN runner_fault = '' THEN ? ELSE runner_fault END
			WHERE id = ?`, FaultOutOfMemory, message, j.ID); err != nil {
			return err
		}
		marked = true
		out, err = scanJob(tx.QueryRowContext(ctx, `SELECT `+jobCols+` FROM jobs WHERE id = ?`, j.ID))
		return err
	})
	return out, marked, err
}

// JobUsageHistory returns the most recent measured runs of each job named,
// newest first, split by the pool that ran them. Keys are matched on their
// repository, workflow and job name; the pool in the answer's keys is the
// pool each run was on.
func (s *Store) JobUsageHistory(ctx context.Context, keys []JobUsageKey) (map[JobUsageKey][]JobPeak, error) {
	out := map[JobUsageKey][]JobPeak{}
	type triple struct{ repo, workflow, job string }
	seen := map[triple]bool{}
	for _, k := range keys {
		t := triple{k.Repo, k.Workflow, k.JobName}
		if seen[t] {
			continue
		}
		seen[t] = true
		if err := s.jobUsageHistory(ctx, t.repo, t.workflow, t.job, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// jobUsageHistorySQL is a const so a query-plan test can pin the statement
// that ships: it must seek idx_jobs_usage_profile, not scan the table, because
// it runs for every distinct queued job on every scheduling pass.
const jobUsageHistorySQL = `SELECT jobs.pool_id, jobs.peak_cpus, jobs.peak_memory_mb, jobs.oom_killed, jobs.granted_memory_mb,
	` + workingSetIdentityCols + ` FROM jobs
	LEFT JOIN workflow_run_paths runs ON runs.repo = jobs.repo AND runs.github_run_id = jobs.github_run_id
	WHERE jobs.repo = ? AND jobs.workflow = ? AND jobs.job_name = ? AND jobs.state = 'completed'
	AND (jobs.peak_memory_mb > 0 OR jobs.peak_cpus > 0 OR jobs.oom_killed = 1)
	ORDER BY jobs.completed_at DESC LIMIT ?`

// workingSetIdentityCols is what a history reads of a run's working set and
// the identity of its job, for PreferWorkingSet: the peak, its final verdict,
// the run's workflow file, and whether another job of the same run attempt
// had the same name (a seek of idx_jobs_run, a handful of rows a run).
const workingSetIdentityCols = `jobs.peak_working_set_mb, jobs.working_set_final, COALESCE(runs.path, ''),
	EXISTS (SELECT 1 FROM jobs same WHERE same.github_run_id = jobs.github_run_id AND same.repo = jobs.repo
		AND same.run_attempt = jobs.run_attempt AND same.job_name = jobs.job_name AND same.id != jobs.id)`

// scanWorkingSetIdentity is the Scan targets of workingSetIdentityCols.
func scanWorkingSetIdentity(p *JobPeak, final, shared *int) []any {
	return []any{&p.WorkingSetMB, final, &p.WorkflowPath, shared}
}

func (s *Store) jobUsageHistory(ctx context.Context, repo, workflow, job string, out map[JobUsageKey][]JobPeak) error {
	// Several pools can share a job's history, so a little more than one
	// profile's worth is read and each pool keeps its own newest.
	rows, err := s.read.QueryContext(ctx, jobUsageHistorySQL, repo, workflow, job, JobUsageHistoryLimit*3)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var p JobPeak
		var pool string
		var oom, final, shared int
		if err := rows.Scan(append([]any{&pool, &p.CPUs, &p.MemoryMB, &oom, &p.GrantedMemoryMB}, scanWorkingSetIdentity(&p, &final, &shared)...)...); err != nil {
			return err
		}
		p.OOMKilled = oom == 1
		p.WorkingSetFinal, p.SharedName = final == 1, shared == 1
		k := JobUsageKey{Repo: repo, Workflow: workflow, JobName: job, PoolID: pool}
		if len(out[k]) < JobUsageHistoryLimit {
			out[k] = append(out[k], p)
		}
	}
	return rows.Err()
}

// poolUsageHistorySQL is a const so a query-plan test can pin the statement
// that ships: it runs for every pool on every scheduling pass under
// scheduler.memory_booking=history, so it walks the pool's index newest first
// and stops at the limit rather than sorting the pool's whole history.
const poolUsageHistorySQL = `SELECT jobs.peak_cpus, jobs.peak_memory_mb, jobs.oom_killed, jobs.granted_memory_mb, jobs.repo, jobs.workflow, jobs.job_name,
	` + workingSetIdentityCols + ` FROM jobs
	LEFT JOIN workflow_run_paths runs ON runs.repo = jobs.repo AND runs.github_run_id = jobs.github_run_id
	WHERE jobs.pool_id = ? AND jobs.state = 'completed' AND (jobs.peak_memory_mb > 0 OR jobs.oom_killed = 1)
	ORDER BY jobs.queued_at DESC LIMIT ?`

// PoolUsageHistory returns each named pool's most recent measured runs, newest
// first, at most limit of them per pool: what the pool's jobs used, whichever
// jobs they were. A pool with none measured is absent.
func (s *Store) PoolUsageHistory(ctx context.Context, poolIDs []string, limit int) (map[string][]JobPeak, error) {
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
		rows, err := s.read.QueryContext(ctx, poolUsageHistorySQL, id, limit)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var p JobPeak
			var oom, final, shared int
			var repo, workflow, job string
			if err := rows.Scan(append([]any{&p.CPUs, &p.MemoryMB, &oom, &p.GrantedMemoryMB, &repo, &workflow, &job}, scanWorkingSetIdentity(&p, &final, &shared)...)...); err != nil {
				rows.Close()
				return nil, err
			}
			p.OOMKilled = oom == 1
			p.WorkingSetFinal, p.SharedName = final == 1, shared == 1
			p.Job = repo + "\x00" + workflow + "\x00" + job
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

// runningJobKeysQuery is RunnersWithAJob's question with the job's name kept:
// which job each runner is running.
var runningJobKeysQuery = hot(`SELECT runner_id, repo, workflow, job_name, pool_id FROM jobs
	WHERE state = 'in_progress' AND runner_id != '' ORDER BY queued_at`)

// RunningJobKeys returns, by runner ID, the job GitHub says each runner is
// running, named the way its history is kept. A runner with two jobs in
// progress, which GitHub does not do, is given the newer.
func (s *Store) RunningJobKeys(ctx context.Context) (map[string]JobUsageKey, error) {
	rows, err := s.hotRead().Query(ctx, runningJobKeysQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]JobUsageKey{}
	for rows.Next() {
		var runner string
		var k JobUsageKey
		if err := rows.Scan(&runner, &k.Repo, &k.Workflow, &k.JobName, &k.PoolID); err != nil {
			return nil, err
		}
		out[runner] = k
	}
	return out, rows.Err()
}

// OutputTailLines and OutputTailRunes bound what a job keeps of its runner's
// last output: enough to show what killed it, little enough to keep for every
// failed job, and never one enormous line.
const (
	OutputTailLines = 40
	OutputTailRunes = 400
)

// SetJobOutputTail keeps a runner's last lines on the job it was running,
// found the way MarkJobOOMKilled finds it: the runner's newest job, because
// the runner row loses its job when the runner ends and the job may already
// be over. It returns the job's ID, or "" with no error when the runner had
// no job, which is a runner that died before it took one.
func (s *Store) SetJobOutputTail(ctx context.Context, runnerID string, lines []string) (string, error) {
	if runnerID == "" || len(lines) == 0 {
		return "", nil
	}
	if len(lines) > OutputTailLines {
		lines = lines[len(lines)-OutputTailLines:]
	}
	kept := make(StringSlice, 0, len(lines))
	for _, l := range lines {
		if r := []rune(l); len(r) > OutputTailRunes {
			l = string(r[:OutputTailRunes])
		}
		kept = append(kept, l)
	}
	var jobID string
	err := s.tx(ctx, func(tx *sql.Tx) error {
		err := tx.QueryRowContext(ctx, `SELECT id FROM jobs WHERE runner_id = ? ORDER BY queued_at DESC LIMIT 1`, runnerID).Scan(&jobID)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE jobs SET output_tail = ? WHERE id = ?`, kept, jobID)
		return err
	})
	return jobID, err
}

// RecordJobScratchPeaks raises how full each in-memory folder of the job in
// progress on a runner has been to one sample of its folders. The same shape
// as RecordJobUsage: only a job GitHub says is in progress, and a peak is only
// ever raised, so a late or repeated sample lowers nothing. It is a read and a
// write in one transaction because the column holds every folder at once.
func (s *Store) RecordJobScratchPeaks(ctx context.Context, runnerID string, sample []ScratchPeak) error {
	if runnerID == "" || len(sample) == 0 {
		return nil
	}
	return s.tx(ctx, func(tx *sql.Tx) error {
		var id string
		var have ScratchPeaks
		err := tx.QueryRowContext(ctx, `SELECT id, scratch_peaks FROM jobs WHERE state = 'in_progress' AND runner_id = ?
			ORDER BY queued_at DESC LIMIT 1`, runnerID).Scan(&id, &have)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE jobs SET scratch_peaks = ? WHERE id = ?`, have.Raise(sample), id)
		return err
	})
}

// FailedJobsWithScratchSince is the jobs that failed since a moment and had an
// in-memory folder measured while they ran, newest first, at most limit of
// them. Whether a folder was full is the caller's reading of the peaks; this
// only finds the jobs that have any.
func (s *Store) FailedJobsWithScratchSince(ctx context.Context, since time.Time, limit int) ([]*Job, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.read.QueryContext(ctx, `SELECT `+jobCols+` FROM jobs WHERE state = 'completed' AND `+failedConclusionSQL()+`
		AND scratch_peaks != '' AND completed_at >= ? ORDER BY completed_at DESC LIMIT ?`, ms(since), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}
