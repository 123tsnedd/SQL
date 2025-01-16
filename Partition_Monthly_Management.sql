-- SQL scipt to manage the partition functions and triggers


CREATE OR REPLACE FUNCTION manage_partitions(testing BOOLEAN DEFAULT FALSE)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
	parent_table		TEXT := CASE WHEN testing THEN 'test_telem_partition' ELSE 'telem_partition' END;
	current_month 	DATE := DATE_TRUNC('month', CURRENT_DATE);
  two_months_ago 	DATE := current_month - INTERVAL '2 months';
  next_month 			DATE := current_month + INTERVAL '1 month';
  partition_name_to_delete 					TEXT;
  next_partition_name_to_create 		TEXT;
  current_partition_name_to_create 	TEXT;

BEGIN
	--DEFINE PARTITION NAMES BASED ON CURRENT NAMING CONVENTION 'partition_yyyy-mm'
  partition_name_to_delete := FORMAT('partition_%s', TO_CHAR(two_months_ago, 'YYYY_MM'));
  current_partition_name_to_create := FORMAT('partition_%s', TO_CHAR(current_month, 'YYYY_MM'));
  next_partition_name_to_create := FORMAT('partition_%s', TO_CHAR(next_month, 'YYYY_MM'));
  
  --DROP OLD IF EXISTS
  IF EXISTS(
    SELECT 1
   	FROM information_schema.tables
    WHERE table_name = partition_name_to_delete
    ) 
    THEN
    	EXECUTE FORMAT('DROP TABLE IF EXISTS %I', partition_name_to_delete);
      RAISE NOTICE 'Dropped partition %', partition_name_to_delete;
  END IF;
  
  -- CREATE THIS MONTHS PARTITION IF DOESN'T EXIST
  IF NOT EXISTS(
    SELECT 1
    FROM information_schema.tables
    WHERE table_name = current_partition_name_to_create
    ) THEN
    	EXECUTE FORMAT( 
        'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
        current_partition_name_to_create,
        parent_table,
        TO_CHAR(current_month, 'YYYY-MM-01'),
        TO_CHAR(current_month + INTERVAL '1 month', 'YYYY-MM-01')
        );
      RAISE NOTICE 'Created partition: %', current_partition_name_to_create;
	END IF;
  
  -- CREATE NEXT MONTH PARTITION IF DOESN'T EXIST
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_name = next_partition_name_to_create
    )
    THEN
    	EXECUTE FORMAT('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                     partition_name_to_create,
                     parent_table,
                     TO_CHAR(next_month, 'YYYY-MM-01'),
                     TO_CHAR(next_month + INTERVAL '1 month', 'YYYY-MM-01')
                     );
       RAISE NOTICE 'Created_partition %', next_partition_name_to_create;
   END IF;
END;
$$;
	
-- going to try and automate this to do at top of month 
-- or this will be an option in the observation_log.py 
-- if in script will be an before observing period start to ask if start of observing run

SELECT manage_partitions(FALSE);

SELECT table_name
FROM information_schema.tables
WHERE table_name LIKE 'partition_%';




