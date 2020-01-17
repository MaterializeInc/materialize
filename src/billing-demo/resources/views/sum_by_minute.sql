CREATE VIEW billing_sum_by_minute AS
SELECT substr, meter, sum(value)
FROM billing_records
GROUP BY substr(interval_start, 0, 17), meter
