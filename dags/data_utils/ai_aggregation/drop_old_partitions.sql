DO $$
DECLARE
  cutoff timestamptz := date_trunc('month', now()) - interval '6 months';
  r record;
  part_month timestamptz;
BEGIN
  FOR r IN
    SELECT c.relname AS part_name
    FROM pg_inherits i
    JOIN pg_class   p ON p.oid = i.inhparent
    JOIN pg_class   c ON c.oid = i.inhrelid
    WHERE p.relname = 'model_calls'
  LOOP
    IF r.part_name ~ '^model_calls_[0-9]{4}_[0-9]{2}$' THEN
      part_month := to_timestamp(
        substr(r.part_name, 13, 4) || substr(r.part_name, 18, 2), 'YYYYMM'
      );
      IF part_month < cutoff THEN
        EXECUTE format('DROP TABLE IF EXISTS %I;', r.part_name);
        RAISE NOTICE 'Dropped partition %', r.part_name;
      END IF;
    END IF;
  END LOOP;
END $$;
