CREATE MATERIALIZED VIEW orders IN CLUSTER constraint_cluster AS SELECT * FROM cdb.ingest.orders_raw;

CREATE FOREIGN KEY orders_fk IN CLUSTER constraint_cluster ON orders (user_id) REFERENCES cdb.public.users (id);
