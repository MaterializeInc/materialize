# CREATE DATABASE
`CREATE DATABASE` creates a new database.
Use `CREATE DATABASE` to create a new database.

## Syntax



```mzsql
CREATE DATABASE [IF NOT EXISTS] <database_name>;

```

| Syntax element | Description |
| --- | --- |
| `IF NOT EXISTS` | If specified, do not generate an error if a database of the same name already exists. If not specified, throw an error if a database of the same name already exists.  |
| `<database_name>` | A name for the database.  |


## Details

Databases can contain schemas. By default, each database has a schema called
`public`. For more information about databases, see
[Namespaces](/sql/namespaces).

## Examples

```mzsql
CREATE DATABASE IF NOT EXISTS my_db;
```
```mzsql
SHOW DATABASES;
```
```nofmt
materialize
my_db
```

## Privileges

The privileges required to execute this statement are:

- `CREATEDB` privileges on the system.

## Related pages

- [`DROP DATABASE`](../drop-database)
- [`SHOW DATABASES`](../show-databases)
