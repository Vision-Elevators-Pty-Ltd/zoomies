-- Re-checks the working-set peaks recorded so far against the rules of
-- 1.4.1-vision.11 and marks untrusted (-1) every one that can never pass them:
-- a positive peak on a completed job that has no final verdict, more than
-- fifteen minutes (store.WorkingSetLateReport) after the job completed, by
-- which time no report can seal it any more. The history never reads such a
-- peak anyway (it needs working_set_final = 1); this makes the record say so.
--
-- Idempotent: a second run changes nothing. It never touches peak_memory_mb or
-- a job in progress. Migration 0136 already did the same for every peak
-- recorded before it. Safe against the live database with the controller
-- running (one short write transaction); after a backup:
--
--   sqlite3 /var/lib/zoomies/zoomies.db < working_set_recheck.sql
--
-- It prints how many peaks it found before and after.
.bail on
SELECT 'before', COUNT(*) FROM jobs WHERE state = 'completed' AND peak_working_set_mb > 0 AND working_set_final = 0
    AND completed_at < CAST(strftime('%s', 'now', '-15 minutes') AS INTEGER) * 1000;
BEGIN IMMEDIATE;
UPDATE jobs SET peak_working_set_mb = -1
    WHERE state = 'completed' AND peak_working_set_mb > 0 AND working_set_final = 0
    AND completed_at < CAST(strftime('%s', 'now', '-15 minutes') AS INTEGER) * 1000;
COMMIT;
SELECT 'after', COUNT(*) FROM jobs WHERE state = 'completed' AND peak_working_set_mb > 0 AND working_set_final = 0
    AND completed_at < CAST(strftime('%s', 'now', '-15 minutes') AS INTEGER) * 1000;
