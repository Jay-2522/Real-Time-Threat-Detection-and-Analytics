-- Redis projections were removed; discard their obsolete completed and pending work.
BEGIN;
DELETE FROM pipeline_outbox WHERE target = 'redis';
COMMIT;
