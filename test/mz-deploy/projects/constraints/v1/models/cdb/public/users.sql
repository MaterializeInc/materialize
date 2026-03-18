CREATE MATERIALIZED VIEW users IN CLUSTER constraint_cluster AS SELECT * FROM cdb.ingest.users_raw;

CREATE PRIMARY KEY users_pk IN CLUSTER constraint_cluster ON users (id);
