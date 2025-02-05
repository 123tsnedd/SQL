DROP TABLE IF EXISTS user_log_partition;
CREATE TABLE IF NOT EXISTS user_log_partition(
  ts 			TIMESTAMPTZ PRIMARY KEY,
  devcice VARCHAR(50),
  ec 			VARCHAR(50),
  msg 		JSONB
	)PARTITION BY RANGE(ts)
  ;
CREATE INDEX idx_user_log_partition on user_log_partition (ts);

CREATE TABLE user_log_2024_11
PARTITION OF user_log_partition
FOR VALUES FROM ('2024-11-01') TO ('2024-12-01')
;

CREATE TABLE user_log_2024_12
PARTITION OF user_log_partition
FOR VALUES FROM ('2024-12-01') TO ('2025-01-01')
;


CREATE OR REPLACE FUNCTION insert_user_logs()
RETURNS TRIGGER AS $$
DECLARE
  partition_ts TIMESTAMPTZ;
  partition_device TEXT;
  partition_msg JSONB;
  partition_ec TEXT;
  partition_exists BOOLEAN;
  partition_table_name TEXT;
	BEGIN
  partition_ts := NEW.ts;
  partition_device := NEW.device;
  partition_msg := NEW.msg;
  partition_ec := NEW.ec;
  partition_table_name := 'user_log_partition_' || TO_CHAR(partition_ts, 'YYYY_MM');

  	IF NEW.ec = 'user_log' THEN
    	RETURN NEW;
    ELSE 
      RETURN new;
    END IF;
  -- check if exists
  SELECT EXISTS(
    SELECT 1 FROM pg_catalog.pg_tables WHERE tablename = partition_table_name
    )INTO partition_exists;
  IF partition_exists THEN
    'INSERT INTO %I (ts, device, msg, ec) VALUES ($1, $2, $3, $4)',
    partition_table_name
    ) USING partition_ts, partition_device, partition_msg, partition_ec;
  END IF;
  
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--create trigger
CREATE TRIGGER insert_user_logs
AFTER INSERT ON user_log
FOR EACH ROW
EXECUTE FUNCTION insert_user_logs();

INSERT INTO user_log_partition (
  SELECT * FROM user_log
  WHERE ts >= '2024-11-01'
  AND ec = 'user_log'
  );
  
  truncate table user_log_partition;

SELECT * FROM user_log_partition
WHERE ts BETWEEN ('2024-11-18') and ('2024-11-20');