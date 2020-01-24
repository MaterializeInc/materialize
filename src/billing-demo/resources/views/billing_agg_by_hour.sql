CREATE VIEW billing_agg_by_hour AS
SELECT substr as hour, client_id, meter, cpu_num, memory_gb, disk_gb, sum(value)
FROM billing_records
GROUP BY substr(interval_start, 0, 14), client_id, meter, cpu_num, memory_gb, disk_gb;
