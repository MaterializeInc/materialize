SET cluster = c;
-- Totals by attribution class.
SELECT CASE WHEN h.operator_id IS NULL THEN 'unattributed'
            WHEN o.id IS NULL THEN 'logging dataflow (unlisted)'
            ELSE 'listed operator' END AS class,
       round(sum(h.bytes)/1048576.0, 1) AS mib
FROM mz_introspection.mz_heap_live_per_operator h
LEFT JOIN mz_introspection.mz_dataflow_operators_per_worker o
  ON h.operator_id = o.id AND h.worker_id = o.worker_id
GROUP BY 1 ORDER BY 2 DESC;
-- Sampled heap vs. arrangement size accounting, per arrangement operator.
WITH heap AS (
  SELECT operator_id, worker_id, sum(bytes) AS bytes
  FROM mz_introspection.mz_heap_live_per_operator
  WHERE operator_id IS NOT NULL GROUP BY 1, 2
), arr AS (
  SELECT operator_id, worker_id, size, capacity
  FROM mz_introspection.mz_arrangement_sizes_per_worker
)
SELECT o.name, d.name AS dataflow,
       round(sum(arr.size)/1048576.0, 2) AS arr_size_mib,
       round(sum(arr.capacity)/1048576.0, 2) AS arr_cap_mib,
       round(sum(coalesce(heap.bytes, 0))/1048576.0, 2) AS heap_mib
FROM arr
JOIN mz_introspection.mz_dataflow_operators_per_worker o
  ON o.id = arr.operator_id AND o.worker_id = arr.worker_id
JOIN mz_introspection.mz_dataflow_operator_dataflows_per_worker od
  ON od.id = arr.operator_id AND od.worker_id = arr.worker_id
JOIN mz_introspection.mz_dataflows d ON d.id = od.dataflow_id
LEFT JOIN heap ON heap.operator_id = arr.operator_id AND heap.worker_id = arr.worker_id
GROUP BY 1, 2 ORDER BY 5 DESC LIMIT 15;
