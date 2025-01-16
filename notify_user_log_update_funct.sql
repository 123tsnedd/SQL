
-- create trigger to push update when new log is generated

CREATE OR REPLACE FUNCTION note_user_log_update()
RETURNS TRIGGER AS $$
BEGIN 
	-- pg_notify send a notification when a new row is inserted
  -- channel name 'user_log_update'
  -- payload is the new row converted to json format
	PERFORM pg_notify('user_log_update', row_to_json(NEW)::text);
  RETURN NEW; -- return row to continue the INSERT process

END;
$$ LANGUAGE plpgsql;

-- CREATE TRIGGER
CREATE TRIGGER user_log_trigger
AFTER INSERT ON user_log_partition
FOR EACH ROW
EXECUTE FUNCTION note_user_log_update();







