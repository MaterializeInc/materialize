---
title: "ALTER ... RENAME"
description: "`ALTER ... RENAME` renames an item."
menu:
  main:
    parent: 'commands'
---

`ALTER ... RENAME` renames an item, albeit with some [limitations](#details).

Note that renaming schemas and databases are in development. {{% gh 3680 %}}

## Syntax

{{< diagram "alter-rename.svg" >}}

Field | Use
------|-----
_name_ | The identifier of the item you want to rename.
_new&lowbar;name_ | The new [identifier](/sql/identifiers) you want the item to use.

## Details

Because of Materialize's SQL engine, we currently only support renaming items in
limited contexts.

### Rename-able limitations

You cannot rename items if:

- They are not uniquely qualified among all references to their identifier in
  all dependent views.

    For example, if you have:

    - Two views named `v1` in two different databases (`d1`, `d2`), but they
      both use the same schema name (`s1`)
    - Both views named `v1` are used in another view's query

    You can only rename either view named `v1` if every dependent view's query
    that contains references to both views fully qualifies all references, e.g.

    ```sql
    CREATE VIEW v2 AS
    SELECT *
    FROM db1.s1.v1
    JOIN db2.s1.v1
    ON db1.s1.v1.a = db2.s1.v1
    ```

    If these two views were placed in schemas using distinct identifiers, you
    would only need to qualify their references with schemas instead of
    databases and schemas.

- Any dependent query references a database, schema, or column that uses the same identifier.

    In the following examples, `v1` could _not_ be renamed:

    ```sql
    CREATE VIEW v3 AS
    SELECT *
    FROM v1
    JOIN v2
    ON v1.a = v2.v1
    ```

    ```sql
    CREATE VIEW v4 AS
    SELECT *
    FROM v1
    JOIN v1.v2
    ON v1.a = v2.a
    ```

### New name limitations

You cannot rename an item to _any_ identifier used in a dependent query,
whether that identifier is used implicitly or explicitly.

Consider this example:

```sql
CREATE VIEW v5 AS
SELECT *
FROM d1.s1.v2
JOIN v1
ON v1.a = v2.b
```

You could not rename `v1` to:

- `a`
- `b`
- `v2`
- `s1`
- `d1`
- `materialize` or `public` (implicitly referenced by `materialize.public.v1` using the default database and schema)

However, you could rename `v1` to any other [legal identifier](/sql/identifiers).

## Examples

```sql
SHOW VIEWS;
```
```nofmt
 VIEWS
-------
 v1
```
```sql
ALTER VIEW v1 RENAME TO v2;
SHOW VIEWS;
```
```nofmt
 VIEWS
-------
 v2
```

## See also

- [`SHOW CREATE VIEW`](/sql/show-create-view)
- [`SHOW VIEWS`](/sql/show-views)
- [`SHOW SOURCES`](/sql/show-sources)
- [`SHOW INDEXES`](/sql/show-indexes)
- [`SHOW SINKS`](/sql/show-sinks)
