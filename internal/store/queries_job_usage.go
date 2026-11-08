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
	// old way. PreferWorkingSet decides which figure MemoryMB carries.
	WorkingSetMB int64 `json:"working_set_mb,omitempty"`
}

// WorkingSetMinRuns is how many runs of a job, or of a pool, must have a
// working-set peak before its history is read from those runs alone. It is
// scheduler.BookingMinSamples, the fewest runs a booking is taken from, held
// equal by a test: a history that switches must still be one a booking can be
// taken from.
const WorkingSetMinRuns = 5

// PreferWorkingSet is one job's (or one pool's) runs as the history should
// read them, newest first as given.
//
// Once at least WorkingSetMinRuns of them carry a working-set peak, the runs
// that do are kept with MemoryMB set to it, and the runs measured only the old
// way are dropped: Docker's figure counts the file cache a build re-reads, and
// a history that kept mixing the two would keep booking and raising by the
// inflated runs for as long as they were among the newest twenty. Until then
// every run keeps the old figure, which is never smaller, so a job with too few
// new runs is read exactly as before.
//
// A run the kernel killed for memory is always kept as it was: the limit it
// hit is the one figure known to have been too little, however the run was
// sampled.
func PreferWorkingSet(peaks []JobPeak) []JobPeak {
	n := 0
	for _, p := range peaks {
		if p.WorkingSetMB > 0 && !p.OOMKilled {
			n++
		}
	}
	if n < WorkingSetMinRuns {
		return peaks
	}
	out := make([]JobPeak, 0, len(peaks))
	for _, p := range peaks {
		switch {
		case p.OOMKilled:
			out = append(out, p)
		case p.WorkingSetMB > 0:
			p.MemoryMB = p.WorkingSetMB
			out = append(out, p)
		}
	}
	return out
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

// RecordJobWorkingSet raises the working-set peak of the job in progress on a
// runner to one sample of it, in MB, the way RecordJobUsage raises its usage
// peak: only a job GitHub says is in progress, and never lowered.
func (s *Store) RecordJobWorkingSet(ctx context.Context, runnerID string, memoryMB int64) error {
	if runnerID == "" || memoryMB <= 0 {
		return nil
	}
	_, err := s.exec(ctx, `UPDATE jobs SET peak_working_set_mb = MAX(peak_working_set_mb, ?)
		WHERE state = 'in_progress' AND runner_id = ?`, memoryMB, runnerID)
	return err
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
const jobUsageHistorySQL = `SELECT pool_id, peak_cpus, peak_memory_mb, oom_killed, granted_memory_mb, peak_working_set_mb FROM jobs
	WHERE repo = ? AND workflow = ? AND job_name = ? AND state = 'completed'
	AND (peak_memory_mb > 0 OR peak_cpus > 0 OR oom_killed = 1)
	ORDER BY completed_at DESC LIMIT ?`

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
		var oom int
		if err := rows.Scan(&pool, &p.CPUs, &p.MemoryMB, &oom, &p.GrantedMemoryMB, &p.WorkingSetMB); err != nil {
			return err
		}
		p.OOMKilled = oom == 1
		k := JobUsageKey{Repo: repo, Workflow: workflow, JobName: job, PoolID: pool}
		if len(out[k]) < JobUsageHistoryLimit {
			out[k] = append(out[k], p)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for k, peaks := range out {
		if k.Repo == repo && k.Workflow == workflow && k.JobName == job {
			out[k] = PreferWorkingSet(peaks)
		}
	}
	return nil
}

// poolUsageHistorySQL is a const so a query-plan test can pin the statement
// that ships: it runs for every pool on every scheduling pass under
// scheduler.memory_booking=history, so it walks the pool's index newest first
// and stops at the limit rather than sorting the pool's whole history.
const poolUsageHistorySQL = `SELECT peak_cpus, peak_memory_mb, oom_killed, granted_memory_mb, peak_working_set_mb FROM jobs
	WHERE pool_id = ? AND state = 'completed' AND (peak_memory_mb > 0 OR oom_killed = 1)
	ORDER BY queued_at DESC LIMIT ?`

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
			var oom int
			if err := rows.Scan(&p.CPUs, &p.MemoryMB, &oom, &p.GrantedMemoryMB, &p.WorkingSetMB); err != nil {
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
		if peaks, ok := out[id]; ok {
			out[id] = PreferWorkingSet(peaks)
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
