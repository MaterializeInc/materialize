CREATE VIEW sum_by_hour AS
SELECT substr, meter, sum(value)
FROM billing_records
GROUP BY substr(interval_start, 0, 15), meter
