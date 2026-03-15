CREATE CONNECTION pg_conn TO POSTGRES (
    HOST 'postgres',
    DATABASE 'postgres',
    USER 'postgres',
    PASSWORD SECRET pgpass
)
