CREATE VIEW billing_agg_by_day AS
SELECT substr as day, client_id, meter, cpu_num, memory_gb, disk_gb, sum(value)
FROM billing_records
GROUP BY substr(interval_start, 0, 11), client_id, meter, cpu_num, memory_gb, disk_gb;
