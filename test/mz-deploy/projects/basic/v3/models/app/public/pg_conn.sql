CREATE CONNECTION pg_conn TO POSTGRES (
    HOST 'postgres',
    PORT 5432,
    DATABASE 'postgres',
    USER 'postgres',
    PASSWORD SECRET pgpass
)
