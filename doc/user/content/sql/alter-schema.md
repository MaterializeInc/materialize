---
title: "ALTER SCHEMA"
description: "`ALTER SCHEMA` change properties of a schema"
menu:
  main:
    parent: 'commands'
aliases:
  - /sql/alter-swap/
---

Use `ALTER SCHEMA` to:
- Swap the name of a schema with that of another schema.
- Rename a schema.
- Change owner of a schema.


## Syntax

{{< tabs >}}
{{< tab "Swap with" >}}

### Swap with

To swap the name of a schema with that of another schema:

```mzsql
ALTER SCHEMA <schema1> SWAP WITH <schema2>;
```

Syntax element       | Description
---------------------|------------
`<schema1>`         | The name of the schema you want to swap.
`<schema2>`         | The name of the other schema you want to swap with.

{{< /tab >}}
{{< tab "Rename schema" >}}

### Rename schema

To rename a schema:

```mzsql
ALTER SCHEMA <name> RENAME TO <new_name>;
```

Syntax element | Description
---------------|------------
`<name>`| The current name of the schema.
`<new_name>`| The new name of the schema.

See also [Renaming restrictions](/sql/identifiers/#renaming-restrictions).

{{< /tab >}}
{{< tab "Change owner to" >}}

### Change owner to

To change the owner of a schema:

```mzsql
ALTER SCHEMA <name> OWNER TO <new_owner_role>;
```

Syntax element | Description
---------------|------------
`<name>`| The name of the schema you want to change ownership of.
`<new_owner_role>`| The new owner of the schema.

To change the owner of a schema, you must be the owner of the schema and have
membership in the `<new_owner_role>`. See also [Privileges](#privileges).

{{< /tab >}}

{{< /tabs >}}


## Examples

### Swap schema names

Swapping two schemas is useful for a blue/green deployment. The following swaps
the names of the `blue` and `green` schemas.

```mzsql
CREATE SCHEMA blue;
CREATE TABLE blue.numbers (n int);

CREATE SCHEMA green;
CREATE TABLE green.tags (tag text);

ALTER SCHEMA blue SWAP WITH green;

-- The schema which was previously named 'green' is now named 'blue'.
SELECT * FROM blue.tags;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/alter-schema.md" >}}

## See also

- [`SHOW CREATE VIEW`](/sql/show-create-view)
- [`SHOW VIEWS`](/sql/show-views)
- [`SHOW SOURCES`](/sql/show-sources)
- [`SHOW INDEXES`](/sql/show-indexes)
- [`SHOW SECRETS`](/sql/show-secrets)
- [`SHOW SINKS`](/sql/show-sinks)
