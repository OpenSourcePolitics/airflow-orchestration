WITH params AS (
  SELECT (CURRENT_DATE - INTERVAL '1 day')::date AS d
)
INSERT INTO public.daily_usage AS du
(day, host, content_type, provider, model, calls,
 sum_input_tokens, sum_output_tokens, sum_cost, p95_latency_ms)
SELECT
  p.d AS day,
  coalesce(mc.host,'')            AS host,
  coalesce(mc.content_type,'')    AS content_type,
  coalesce(mc.provider,'openai')  AS provider,
  coalesce(mc.model,'')           AS model,
  count(*)                           AS calls,
  coalesce(sum(mc.input_tokens),0)   AS sum_input_tokens,
  coalesce(sum(mc.output_tokens),0)  AS sum_output_tokens,
  coalesce(sum(mc.cost),0)       AS sum_cost,
  percentile_disc(0.95) WITHIN GROUP (ORDER BY mc.latency_ms) AS p95_latency_ms
FROM params p
JOIN public.model_calls mc
  ON mc.ts >= p.d::timestamptz
 AND mc.ts <  (p.d::timestamptz + interval '1 day')
GROUP BY p.d, host, content_type, provider, model
ON CONFLICT (day, host, content_type, provider, model)
DO UPDATE SET
  calls             = EXCLUDED.calls,
  sum_input_tokens  = EXCLUDED.sum_input_tokens,
  sum_output_tokens = EXCLUDED.sum_output_tokens,
  sum_cost      = EXCLUDED.sum_cost,
  p95_latency_ms    = EXCLUDED.p95_latency_ms;
