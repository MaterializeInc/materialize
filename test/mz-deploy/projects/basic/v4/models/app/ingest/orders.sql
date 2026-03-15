CREATE TABLE orders FROM SOURCE app.ingest.pg_source (REFERENCE public.orders);

GRANT SELECT ON TABLE app.ingest.orders TO materialize
