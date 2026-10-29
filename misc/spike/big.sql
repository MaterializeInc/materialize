-- A few million rows, a join, and a reduction, so user arrangements dominate the heap.
CREATE TABLE facts (k bigint, g bigint, v text);
INSERT INTO facts SELECT x, x % 1000, md5(x::text) FROM generate_series(1, 3000000) AS x;
CREATE TABLE dims (g bigint, label text);
INSERT INTO dims SELECT x, 'label-' || x FROM generate_series(0, 999) AS x;
SET cluster = c;
CREATE INDEX facts_k_idx ON facts (k);
CREATE VIEW per_label AS
  SELECT d.label, count(*) AS n, max(f.v) AS mx
  FROM facts f JOIN dims d ON f.g = d.g GROUP BY d.label;
CREATE INDEX per_label_idx ON per_label (label);
CREATE VIEW distinct_v AS SELECT DISTINCT v FROM facts;
CREATE INDEX distinct_v_idx ON distinct_v (v);
