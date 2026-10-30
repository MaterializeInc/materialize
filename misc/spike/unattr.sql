SET cluster = c;
SELECT round(sum(h.bytes)/1048576.0, 1) AS mib,
       right(s.frames, 260) AS leaf
FROM mz_introspection.mz_heap_live_per_operator h
JOIN mz_introspection.mz_heap_stacks s USING (process_id, stack_id)
WHERE h.operator_id IS NULL
GROUP BY 2 ORDER BY 1 DESC LIMIT 8;
SELECT round(sum(h.bytes)/1048576.0, 1) AS mib,
       right(s.frames, 260) AS leaf
FROM mz_introspection.mz_heap_live_per_operator h
LEFT JOIN mz_introspection.mz_dataflow_operators_per_worker o
  ON h.operator_id = o.id AND h.worker_id = o.worker_id
JOIN mz_introspection.mz_heap_stacks s USING (process_id, stack_id)
WHERE h.operator_id IS NOT NULL AND o.id IS NULL
GROUP BY 2 ORDER BY 1 DESC LIMIT 4;
SELECT count(*) AS live_rows FROM mz_introspection.mz_heap_live_per_operator;
SELECT count(*) AS stacks, round(sum(length(frames))/1048576.0, 1) AS frames_mib FROM mz_introspection.mz_heap_stacks;
