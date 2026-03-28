CREATE SOURCE pg_source
    IN CLUSTER ingest
    FROM POSTGRES CONNECTION app.public.pg_conn
    (PUBLICATION 'mz_source')
