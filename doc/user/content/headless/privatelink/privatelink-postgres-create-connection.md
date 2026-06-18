---
headless: true
---
## Create a source connection

In Materialize, create a source connection that uses the AWS PrivateLink connection you just configured:

```mzsql
CREATE CONNECTION pg_connection TO POSTGRES (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 5432,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass,
    AWS PRIVATELINK privatelink_svc
);
```

This PostgreSQL connection can then be reused across multiple [`CREATE SOURCE`](https://materialize.com/docs/sql/create-source/postgres/) statements.
