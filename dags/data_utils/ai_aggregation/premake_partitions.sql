DO $$
DECLARE
  m_start date := date_trunc('month', now())::date;
  i int;
  from_ts timestamptz;
  to_ts   timestamptz;
  part_name text;
  part_qualified text;
BEGIN
  -- current month + next 3 months
  FOR i IN 0..3 LOOP
    from_ts := (m_start + (i||' month')::interval);
    to_ts   := (m_start + ((i+1)||' month')::interval);
    part_name := format('model_calls_%s', to_char(from_ts, 'YYYY_MM'));
    part_qualified := format('public.%I', part_name);

    -- if partition table doesn't exist at all, prepare it as a plain table first
    IF to_regclass(part_qualified) IS NULL THEN
      EXECUTE format('CREATE TABLE %s (LIKE public.model_calls INCLUDING ALL)', part_qualified);
    END IF;

    -- If this table is already attached as a partition, skip the rest
    IF EXISTS (
      SELECT 1
      FROM pg_inherits h
      JOIN pg_class c_child ON c_child.oid = h.inhrelid
      JOIN pg_class c_parent ON c_parent.oid = h.inhparent
      JOIN pg_namespace n ON n.oid = c_child.relnamespace
      WHERE n.nspname = 'public' AND c_parent.relname = 'model_calls' AND c_child.relname = part_name
    ) THEN
      -- ensure index exists anyway
      EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %s (host, ts)', part_name||'_host_ts', part_qualified);
      CONTINUE;
    END IF;

    -- Move rows out of DEFAULT for this month (if any),
    -- so that attaching the partition won't conflict.
    EXECUTE format($ins$
      INSERT INTO %s
      SELECT *
      FROM public.model_calls_default
      WHERE ts >= %L AND ts < %L
    $ins$, part_qualified, from_ts, to_ts);

    EXECUTE format($del$
      DELETE FROM public.model_calls_default
      WHERE ts >= %L AND ts < %L
    $del$, from_ts, to_ts);

    -- Now safely attach as a real partition
    EXECUTE format($att$
      ALTER TABLE public.model_calls
      ATTACH PARTITION %s
      FOR VALUES FROM (%L) TO (%L)
    $att$, part_qualified, from_ts, to_ts);

    -- Create useful index on the new child
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %s (host, ts)', part_name||'_host_ts', part_qualified);
  END LOOP;
END $$;
