-- Takes a database back from 1.4.1-vision.10 to the schema 1.4.1-vision.9 (and
-- vision.8) can open: removes the column migration 0135 added and its row in the
-- migration ledger. A build refuses a database whose ledger names a migration it
-- does not have, so swapping the binary back alone is not enough.
--
-- Run it with the controller stopped, after a backup, against the controller's
-- database (the deployed one is /var/lib/zoomies/zoomies.db):
--
--   systemctl stop zoomies
--   sqlite3 /var/lib/zoomies/zoomies.db < 0135_job_working_set_peak.down.sql
--
-- then install the older binary and start it. What is lost is only each job's
-- working-set peak; every other row is kept. Running vision.10 again re-applies
-- the migration. Needs SQLite 3.35 or later for DROP COLUMN.
.bail on
BEGIN IMMEDIATE;
ALTER TABLE jobs DROP COLUMN peak_working_set_mb;
DELETE FROM schema_migrations WHERE name = '0135_job_working_set_peak.sql';
COMMIT;
