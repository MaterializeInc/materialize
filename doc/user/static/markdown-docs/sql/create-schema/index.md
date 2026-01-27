# CREATE SCHEMA
`CREATE SCHEMA` creates a new schema.
`CREATE SCHEMA` creates a new schema.

## Syntax



```mzsql
CREATE SCHEMA [IF NOT EXISTS] <schema_name>;

```

| Syntax element | Description |
| --- | --- |
| `IF NOT EXISTS` | If specified, do not generate an error if a schema of the same name already exists. If not specified, throw an error if a schema of the same name already exists.  |
| `<schema_name>` | A name for the schema. You can specify the database for the schema with a preceding `database_name.schema_name`, e.g. `my_db.my_schema`, otherwise the schema is created in the current database.  |


## Details

By default, each database has a schema called `public`.

For more information, see [Namespaces](../namespaces).

## Examples

```mzsql
CREATE SCHEMA my_db.my_schema;
```
```mzsql
SHOW SCHEMAS FROM my_db;
```
```nofmt
public
my_schema
```

## Privileges

The privileges required to execute this statement are:

- `CREATE` privileges on the containing database.

## Related pages

- [`DROP DATABASE`](../drop-database)
- [`SHOW DATABASES`](../show-databases)
