CREATE SOURCE pg_source IN CLUSTER quickstart
    FROM POSTGRES CONNECTION pg_conn (
        PUBLICATION 'mz_source'
    )
