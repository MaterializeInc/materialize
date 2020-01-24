CREATE VIEW billing_agg_by_minute AS
SELECT substr as minute, client_id, meter, cpu_num, memory_gb, disk_gb, sum(value)
FROM billing_records
GROUP BY substr(interval_start, 0, 17), client_id, meter, cpu_num, memory_gb, disk_gb;
