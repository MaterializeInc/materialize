CREATE TABLE t10 (id bigint NOT NULL, v text NOT NULL, n bigint NOT NULL);
INSERT INTO t10 SELECT g, 'v' || g, 0 FROM generate_series(1, 5000000) g;
INSERT INTO t10 SELECT g, 'v' || g, 0 FROM generate_series(5000001, 10000000) g;
CREATE MATERIALIZED VIEW mv10 AS SELECT id, v, n FROM t10;
SELECT count(*) FROM mv10;
SELECT s.shard_id FROM mz_internal.mz_storage_shards s JOIN mz_materialized_views m ON s.object_id = m.id WHERE m.name = 'mv10';
