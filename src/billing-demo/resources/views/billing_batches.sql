CREATE VIEW billing_batches AS
SELECT
    {source_name}.id,
    {source_name}.interval_start,
    {source_name}.interval_end
FROM
    {source_name};
