;;DROP FUNCTION insert_into_partitioned CASCADE;
-- verify existing partitions
SELECT relname AS partition_name
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

--DEFINE oct PARTITION
DROP TABLE telem_2024_10;
CREATE TABLE telem_2024_10 PARTITION OF telem_partition
	FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');

-- Define the November 2024 partition
DROP TABLE telem_2024_11;
CREATE TABLE IF NOT EXISTS telem_2024_11 PARTITION OF telem_partition
  FOR VALUES FROM ('2024-11-01') TO ('2024-12-01'); -- Exclusive upper bound

-- December 2024 partition
DROP TABLE telem_2024_12;
CREATE TABLE IF NOT EXISTS telem_2024_12 PARTITION OF telem_partition
FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');

-- Create an index on the specific partition
--CREATE INDEX idx_telem_2024_11_device ON telem_2024_11 (device);

-- Insert data into the parent partitioned table
INSERT INTO telem_partition (ts, device, msg, ec)
SELECT ts, device, msg, ec
FROM telem
WHERE
		(ts between '2024-11-01' AND '2025-01-01')
  AND
  	ts IS NOT NULL
  AND
	  (
      (ec = 'telem_stdcam' AND device IN ('camsci1', 'camsci2', 'camwfs'))
    OR
      (ec = 'telem_telsee' AND device = 'tcsi')
    OR
      (device = 'holoop')
    OR
      (ec = 'telem_stage' AND device IN ('fwsci1', 'fwsci2', 'flipacq', 'stagebs',
                                          'fwpupil', 'fwfpm', 'fwlyot', 'stagescibs', 'flipwfsf'))
    OR
     	(device = 'observers')-- AND msg -> 'observing' = 'true')
     )
;
  

SELECT * FROM telem_partition
LIMIT 100;

-- Drop the old trigger function if it exists
DROP FUNCTION IF EXISTS insert_into_partition;

-- Define the trigger function to insert data into the partitioned table
CREATE OR REPLACE FUNCTION insert_into_partition()
RETURNS TRIGGER AS $$
DECLARE 
	partition_ts TIMESTAMP;
  partition_device TEXT;
  partition_msg JSONB;
  partition_ec TEXT;
BEGIN
-- assign default vals
	partition_ts := NEW.ts;
  partition_device := NEW.device;
  partition_msg := NEW.msg;
  partition_ec := NEW.ec;
  -- Insert only if matches specified conditions
  IF NEW.device = 'observers' THEN
  	NULL; -- NO OTher action at this time
    
  ELSIF NEW.ec = 'telem_stdcam' AND NEW.device IN ('camsci1', 'camsci2', 'camwfs') THEN
    NULL; -- assigned by defualt will only take what matches statements
  
  ELSIF NEW.ec = 'telem_loopgain' AND NEW.device = 'holoop' THEN
  	NULL;
    
  ELSIF NEW.ec = 'telem_telsee' AND NEW.device = 'tcsi' THEN
    NULL;
  
  ELSIF NEW.ec = 'telem_stage' AND NEW.device IN ('fwsci1', 'fwsci2', 'flipacq',
                                                 'stagebs', 'fwpupil', 'fwfpm',
                                                 'fwlyot', 'stagescibs', 'flipwfsf') THEN
    NULL;
   ELSE
   	-- NO CONditions met, skip insert
    	RETURN NEW;
  END IF;
  -- perform insert here
  	INSERT INTO telem_partition (ts, device, msg, ec)
    VALUES (partition_ts, partition_device, partition_msg, partition_ec);  
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create the trigger on the telem table
CREATE TRIGGER after_insert_telem
AFTER INSERT ON telem
FOR EACH ROW
EXECUTE FUNCTION insert_into_partition();


select * from telem_2024_11
WHERE ts >= '2024-11-10'

LIMIT 1000
;


--think there are duplicates
CREATE TABLE unique_rows AS
WITH CTE AS (
  SELECT *, ROW_NUMBER() 
  	OVER (PARTITION BY ts ORDER BY ts) AS row_num
  FROM telem_partition
)
SELECT *
FROM CTE
WHERE row_num = 1;

--DELETE ROWS FROM telem_partition
DELETE FROM telem_partition;

--insert unique back into partition
INSERT INTO telem_partition
SELECT ts, device, msg, ec
FROM unique_rows;

--drop temp table
DROP TABLE unique_rows;




