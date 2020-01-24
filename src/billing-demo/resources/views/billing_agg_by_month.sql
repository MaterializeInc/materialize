CREATE VIEW billing_agg_by_month AS
SELECT substr as month, client_id, meter, cpu_num, memory_gb, disk_gb, sum(value)
FROM billing_records
GROUP BY substr(interval_start, 0, 8), client_id, meter, cpu_num, memory_gb, disk_gb;
