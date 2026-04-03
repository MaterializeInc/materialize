CREATE MATERIALIZED VIEW emails IN CLUSTER constraint_cluster AS SELECT * FROM cdb.ingest.emails_raw;

CREATE UNIQUE CONSTRAINT emails_unique IN CLUSTER constraint_cluster ON emails (email);
