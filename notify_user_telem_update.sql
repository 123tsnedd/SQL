CREATE OR REPLACE FUNCTION note_telem_update()
RETURNS TRIGGER AS $$
BEGIN
	PERFORM pg_notify('telem_updated', row_to_json(NEW)::text);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--create trigger
CREATE TRIGGER telem_update_trigger
AFTER INSERT ON telem_partition
FOR EACH ROW
EXECUTE FUNCTION note_telem_update();