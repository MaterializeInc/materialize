---
title: "Identifiers"
description: "SQL identifiers are names of columns and database objects such as sources and views."
menu:
  main:
    parent: reference
    name: 'SQL identifiers'
    weight: 125
---

In Materialize, identifiers are used to refer to columns and database objects
like sources, views, and indexes.

## Naming restrictions

Materialize has the following naming restrictions for identifiers:

| Position            | Allowed Characters                                                                 |
|---------------------|------------------------------------------------------------------------------------|
| **First character** | ASCII letters (`a`-`z`, `A`-`Z`), underscore (`_`), or any non-ASCII character     |
| **Remaining**       | ASCII letters (`a`-`z`, `A`-`Z`), digits (`0`-`9`), underscores (`_`), dollar sign (`$`), or any non-ASCII character |

To override these restrictions, you can enclose the identifier in double quotes;
e.g., `"123_source"` or `"fun_source_@"`. Inside double quotes, characters are interpreted literally, except for the double-quote character itself. To include a double quote within a double-quoted identifier, escape it by writing two adjacent double quotes, as in "includes""quote".

{{< note >}}
The identifiers `"."` and `".."` are not allowed.
{{</ note >}}

## Case sensitivity

Materialize performs case folding (the caseless comparison of text) for identifiers, which means that identifiers are effectively case-insensitive (`foo` is the same as `FOO` is the same as `fOo`). This can cause issues when column names come from data sources which do support case-sensitive names, such as Avro-formatted sources.

To avoid conflicts, double-quote all field names (`"field_name"`) when working with case-sensitive sources.

## Renaming restrictions

You cannot rename an item if any of the following are true:

- **It is not uniquely qualified across all dependent references.**

  For example, suppose you have:

  - Two views named `v1` in different databases (`d1` and `d2`) under the same schema name (`s1`), and
  - Both `v1` views are referenced by another view.

  You may rename either `v1` only if every dependent query that mentions both views **fully qualifies all such references**, e.g.:

  ```mzsql
  CREATE VIEW v2 AS
  SELECT *
  FROM d1.s1.v1
  JOIN d2.s1.v1
  ON d1.s1.v1.a = d2.s1.v1.a;
  ```

  If the two views were instead in schemas with distinct names, qualifying by schema alone would be sufficient (you would not need to include the database name).

- Renaming would cause any identifier collision with a dependent query.

  - An existing collision: a dependent query already uses the item’s current
    identifier for some database, schema, object, or column, so changing the
    item’s name would change how that identifier resolves.

    In the examples below, v1 cannot be renamed because dependent queries already use the identifier v1:

    - Any dependent query references a database, schema, or column that uses the same identifier.

        In the following examples, `v1` could _not_ be renamed:

        ```mzsql
        CREATE VIEW v3 AS
        SELECT *
        FROM v1
        JOIN v2
        ON v1.a = v2.v1
        ```

        ```mzsql
        CREATE VIEW v4 AS
        SELECT *
        FROM v1
        JOIN v1.v2
        ON v1.a = v2.a
        ```

  - A proposed-name collision: the new name matches any identifier referenced in
    a dependent query, whether that identifier is referenced explicitly or
    implicitly.

    Consider this example:

    ```mzsql
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

## Keyword collision

Materialize is very permissive with accepting SQL keywords as identifiers (e.g.
`offset`, `user`). If Materialize cannot use a keyword as an
identifier in a particular location, it throws a syntax error. You can wrap the
identifier in double quotes to force Materialize to interpret the word as an
identifier instead of a keyword.

For example, `SELECT offset` is invalid, because it looks like a mistyping of
`SELECT OFFSET <n>`. You can wrap the identifier in double quotes, as in
`SELECT "offset"`, to resolve the error.

We recommend that you avoid using keywords as identifiers whenever possible, as
the syntax errors that result are not always obvious.

The current keywords are listed below.

{{< kwlist >}}
