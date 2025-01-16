
-----------TERMINATE RUNNING TASKS----------------------
select pg_terminate_backend(pid)
from (
  SELECT
      pid
  FROM pg_stat_activity
     WHERE query <> '<insufficient privilege>'
     AND state <> 'idle'
     AND pid <> pg_backend_pid()
     AND query_start < now() - interval '1 minute'
     ORDER BY query_start DESC) t;

SELECT schemaname, relname, seq_scan, seq_tup_read, idx_scan, seq_tup_read / seq_scan AS avg
FROM pg_stat_user_tables
WHERE seq_scan > 0 
ORDER BY seq_tup_read DESC;

-----VACUUM DB-----
-- VACUUM [FULL] [FREEZE] [VERBVOSE] [table]
-- VACUUM [FULL] [FREEZE] [VERBOSE] ANALYZE [table[(column [,...])]]
VACUUM;