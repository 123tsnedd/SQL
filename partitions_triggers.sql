--------- TELEM PARTION PARAMS -----------------
DROP FUNCTION insert_into_partitioned CASCADE;
-- verify existing partitions
SELECT relname
FROM pg_inherits
JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
JOIN pg_class child ON pg_inherits.inhrelid = child.oid
WHERE parent.relname = 'telem_partition';

-- Drop and re-create the main partitioned table
DROP TABLE IF EXISTS telem_partition CASCADE;
CREATE TABLE telem_partition (
  ts TIMESTAMPTZ,
  device VARCHAR(50),
  msg JSONB,
  ec VARCHAR(20)
) PARTITION BY RANGE (ts);

-- Create an index on the specific partition
CREATE INDEX idx_telem_2024_11_device ON telem_partition (ts);

--DEFINE oct PARTITION
DROP TABLE telem_2024_10;
CREATE TABLE telem_2024_10 PARTITION OF telem_partition
	FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');

-- Define the November 2024 partition
DROP TABLE telem_2024_11;
CREATE TABLE telem_2024_11 PARTITION OF telem_partition
  FOR VALUES FROM ('2024-11-01') TO ('2024-12-01'); -- Exclusive upper bound

-- December 2024 partition
DROP TABLE telem_2024_12;
CREATE TABLE telem_2024_12 PARTITION OF telem_partition
FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');


-- Insert data into the parent partitioned table
INSERT INTO telem_partition (ts, device, msg, ec)
SELECT ts, device, msg, ec
FROM telem
WHERE 
	ts >= '2024-10-01' AND ts <'2025-01-01'
  AND
  	ts IS NOT NULL
  AND
	  ((ec = 'telem_stdcam' AND device IN ('camsci1', 'camsci2', 'camwfs'))
    OR
      (ec = 'telem_telsee' AND device = 'tcsi')
    OR
      (device = 'holoop')
    OR
      (ec = 'telem_stage' AND device IN ('fwsci1', 'fwsci2', 'flipacq', 'stagebs',
                                          'fwpupil', 'fwfpm', 'fwlyot', 'stagescibs', 'flipwfsf'))
    OR
     	(device = 'observers' AND msg -> observing = 'true')
     )
;
  

SELECT * FROM telem_2024_11
LIMIT 100;

-- Drop the old trigger function if it exists
DROP FUNCTION IF EXISTS insert_into_partition;

-- Define the trigger function to insert data into the partitioned table
CREATE OR REPLACE FUNCTION insert_into_partition()
RETURNS TRIGGER AS $$
BEGIN
  -- Insert only if matches specified conditions
  IF NEW.ec = 'telem_stdcam' AND NEW.device IN ('camsci1', 'camsci2', 'camwfs') THEN
    INSERT INTO telem_partition (ts, device, msg, ec)
    VALUES (NEW.ts, NEW.device, NEW.msg, NEW.ec);
  
  ELSIF NEW.ec = 'telem_loopgain' AND NEW.device = 'holoop' THEN
  	INSERT INTO telem_partition (ts, device, msg, ec)
    VALUES (NEW.ts, NEW.device, NEW.msg, NEW.ec);
    
  ELSIF NEW.ec = 'telem_telsee' AND NEW.device = 'tcsi' THEN
    INSERT INTO telem_partition (ts, device, msg, ec)
    VALUES (NEW.ts, NEW.device, NEW.msg, NEW.ec);
  
  ELSIF NEW.ec = 'telem_stage' AND NEW.device IN ('fwsci1', 'fwsci2', 'flipacq',
                                                 'stagebs', 'fwpupil', 'fwfpm',
                                                 'fwlyot', 'stagescibs', 'flipwfsf') THEN
    INSERT INTO telem_partition (ts, device, msg, ec)
    VALUES (NEW.ts, NEW.device, NEW.msg, NEW.ec);
  END IF;
  
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create the trigger on the telem table
CREATE TRIGGER after_insert_telem
AFTER INSERT ON telem
FOR EACH ROW
EXECUTE FUNCTION insert_into_partition();


---------ACTIVE OBSERVATIONS ------------
DROP TABLE active_observing_partition;
--CREATE TABLE observing_partition
CREATE TABLE IF NOT EXISTS active_observing_partition (
  ts TIMESTAMPTZ,
  device VARCHAR(10),
  msg JSONB,
  ec VARCHAR(20)
  )PARTITION BY RANGE (ts)
  ;

-- INDEX TS
CREATE INDEX idx_observing_ts ON active_observing_partition (ts);
  
CREATE TABLE observing_2024_11
PARTITION OF active_observing_partition
FOR VALUES FROM ('2024-11-01') TO ('2024-12-01') --EXLUSING UPPER
;

CREATE TABLE observing_2024_12
PARTITION OF active_observing_partition
FOR VALUES FROM ('2024-12-01') TO ('2025-01-01')
;

INSERT INTO active_observing_partition
SELECT ts, device, msg, ec FROM telem
WHERE device = 'observers' 
AND msg -> 'observing' = 'true'
AND ts >= '2024-11-01'
;

-- create function to insert active observers
CREATE OR REPLACE FUNCTION insert_active_observers()
RETURNS TRIGGER AS $$
BEGIN
	IF NEW.msg -> 'observing' = 'true' THEN
  INSERT INTO active_observing_partition (ts, device, msg, ec)
  VALUES (NEW.ts, NEW.device, NEW.msg, NEW.ec);
  END IF;
  
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- create the trigger
CREATE TRIGGER insert_active_observers
AFTER INSERT ON telem
FOR EACH ROW
EXECUTE FUNCTION insert_active_observers()
;


-------USER_LOG PARTITION------------
CREATE TABLE IF NOT EXISTS user_log_partition (
  ts      TIMESTAMPTZ PRIMARY KEY,
  device  VARCHAR(50),
  ec      VARCHAR(50),
  msg     JSONB
) PARTITION BY RANGE (ts)
;
CREATE INDEX idx_user_log_partition ON user_log_partition (ts);

-- NOV partition
CREATE TABLE user_log_2024_11
PARTITION OF user_log_partition
FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');

-- DEC partition
CREATE TABLE user_log_2024_12
PARTITION OF user_log_partition
FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');

--CREATE FUNTION INSERT INTO USER_LOG
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

-- trigger--
CREATE TRIGGER insert_user_logs
AFTER INSERT ON user_log
FOR EACH ROW
EXECUTE FUNCTION insert_user_logs();

--insert current user-log data
INSERT INTO user_log_partition(
  SELECT * FROM user_log
  WHERE ts >= '2024-11-01'
  AND ec = 'user_log'
)