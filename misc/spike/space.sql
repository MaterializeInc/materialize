SHOW enable_lgalloc;
SHOW enable_columnation_lgalloc;
SET cluster = c;
SELECT space, (operator_id IS NOT NULL) AS attributed, round(sum(bytes)/1048576.0, 1) AS mib
FROM mz_introspection.mz_heap_live_per_operator GROUP BY 1, 2 ORDER BY 3 DESC;
SELECT round(sum(size)/1048576.0, 1) AS arr_size_mib, round(sum(capacity)/1048576.0, 1) AS arr_cap_mib
FROM mz_introspection.mz_arrangement_sizes_per_worker;
SELECT source, metric, value FROM mz_introspection.mz_cluster_replica_resource_usage
WHERE metric ILIKE '%rss%' OR metric ILIKE 'memory.current';
