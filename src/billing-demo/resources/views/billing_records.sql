CREATE VIEW billing_records AS
SELECT
    {source_name}.id,
    r.value->>'interval_start' interval_start,
    r.value->>'interval_end' interval_end,
    r.value->>'meter' meter,
    (r.value->'value')::int value
FROM
    {source_name},
    jsonb_array_elements(records) AS r;
