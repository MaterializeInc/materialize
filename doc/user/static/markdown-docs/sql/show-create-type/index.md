# SHOW CREATE TYPE
`SHOW CREATE TYPE` returns the DDL statement used to custom create the type.
`SHOW CREATE TYPE` returns the DDL statement used to create the custom type.

## Syntax

```sql
SHOW [REDACTED] CREATE TYPE <type_name>;
```


| Syntax element | Description |
| --- | --- |
| <strong>REDACTED</strong> | If specified, literals will be redacted. |


For available type names names, see [`SHOW TYPES`](/sql/show-types).

## Examples

```sql
SHOW CREATE TYPE point;

```

```nofmt
    name          |    create_sql
------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 point            | CREATE TYPE materialize.public.point AS (x pg_catalog.int4, y pg_catalog.int4);
```

## Privileges

- `USAGE` privileges on the schema containing the table.

## Related pages

- [`SHOW TYPES`](../show-types)
- [`CREATE TYPE`](../create-type)
