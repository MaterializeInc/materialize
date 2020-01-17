CREATE VIEW billing_sum_by_minute_thin AS
SELECT substr, r.value->>'meter', sum((r.value->>'value')::float::int)
FROM
    {source_name},
    jsonb_array_elements(records) AS r
GROUP BY substr((r.value->>'interval_start')::text, 0, 17), r.value->>'meter'
