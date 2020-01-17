CREATE VIEW billing_measurements AS
SELECT
    {source_name}.id,
    m.value->>'resource' resource,
    (m.value->>'measured_value')::int value
FROM
    {source_name},
    jsonb_array_elements(records) AS r,
    jsonb_array_elements(r.value->'measurements') AS m;
