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
	BEGIN
  	IF NEW.ec = 'user_log' THEN
    	INSERT INTO user_log_partition (ts, device, msg, ec)
      VALUES (NEW.ts, NEW.device, NEW.msg, NEW.ec);
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