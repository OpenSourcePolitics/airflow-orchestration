DO $$
DECLARE
  m_start date := date_trunc('month', now())::date;
  i int;
  from_ts timestamptz;
  to_ts   timestamptz;
  part_name text;
BEGIN
  -- mois courant + 3 mois à venir
  FOR i IN 0..3 LOOP
    from_ts := (m_start + (i||' month')::interval);
    to_ts   := (m_start + ((i+1)||' month')::interval);
    part_name := format('model_calls_%s', to_char(from_ts, 'YYYY_MM'));

    EXECUTE format($q$
      CREATE TABLE IF NOT EXISTS %I
      PARTITION OF public.model_calls
      FOR VALUES FROM (%L) TO (%L);
    $q$, part_name, from_ts, to_ts);

    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (host, ts);',
                   part_name||'_host_ts', part_name);
  END LOOP;
END $$;
