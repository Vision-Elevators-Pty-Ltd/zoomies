-- Takes a database back from 1.4.1-vision.11 to the schema 1.4.1-vision.10 can
-- open: removes what migration 0136 added (jobs.working_set_final and the
-- workflow_run_paths table) and its row in the migration ledger. A build
-- refuses a database whose ledger names a migration it does not have, so
-- swapping the binary back alone is not enough. To go further back, to
-- vision.9, run 0135_job_working_set_peak.down.sql after this one.
--
-- Run it with the controller stopped, after a backup, against the controller's
-- database (the deployed one is /var/lib/zoomies/zoomies.db):
--
--   systemctl stop zoomies
--   sqlite3 /var/lib/zoomies/zoomies.db < 0136_job_working_set_final.down.sql
--
-- then install the older binary and start it. What is lost is each job's
-- final-verdict mark and each run's workflow file. The working-set peaks 0136
-- marked untrusted stay untrusted (-1): vision.10 records them and reads them
-- only under scheduler.working_set_history = on, which stays off. Running
-- vision.11 again re-applies the migration. Needs SQLite 3.35 or later for
-- DROP COLUMN.
.bail on
BEGIN IMMEDIATE;
DROP TABLE workflow_run_paths;
ALTER TABLE jobs DROP COLUMN working_set_final;
DELETE FROM schema_migrations WHERE name = '0136_job_working_set_final.sql';
COMMIT;
