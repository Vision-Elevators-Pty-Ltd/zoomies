-- What a job's working-set peak needs before the history may read it
-- (1.4.1-vision.11, review 3 of #20):
--
-- working_set_final is 1 once the agent's final verdict on the runner's
-- coverage (its end judged: exited, vanished, or removed by a task) was
-- recorded with the peak trusted. A peak without it is a provisional one, which
-- a gap or failure later in the run could still have made untrustworthy, and
-- the history keeps such a run at its old figure.
--
-- workflow_run_paths is the workflow file each run was of (.github/workflows/
-- ci.yml), from the run lookup the controller already makes for its number. A
-- job's history is kept by repository, workflow name and job name, all display
-- names that two definitions can share; the history reads a job's working set
-- only where its runs name one workflow file, and no run had two jobs of the
-- one name.
--
-- Every working-set peak recorded before this migration (by 1.4.1-vision.10,
-- which had neither the final verdict nor the fixes of review 3) is marked
-- untrusted (-1): none of them was recorded with proof of its run's whole
-- coverage, and fixing future reports cannot repair them. The old figure,
-- peak_memory_mb, is kept for every run. Idempotent: it changes nothing a
-- second time, and deploy/maintenance/working_set_recheck.sql re-checks the
-- rows recorded since.
--
-- Rolling back to an older build needs deploy/rollback/0136_job_working_set_final.down.sql
-- first.
ALTER TABLE jobs ADD COLUMN working_set_final INTEGER NOT NULL DEFAULT 0;

CREATE TABLE workflow_run_paths (
    repo          TEXT    NOT NULL,
    github_run_id INTEGER NOT NULL,
    path          TEXT    NOT NULL,
    PRIMARY KEY (repo, github_run_id)
) WITHOUT ROWID;

UPDATE jobs SET peak_working_set_mb = -1 WHERE peak_working_set_mb > 0 AND working_set_final = 0;
