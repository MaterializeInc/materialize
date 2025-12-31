---
audience: developer
canonical_url: https://materialize.com/docs/sql/drop-source/
complexity: intermediate
description: '`DROP SOURCE` removes a source from Materialize.'
doc_type: reference
keywords:
- DROP THEM
- IF EXISTS
- DROP SOURCE
- RESTRICT
- CASCADE
product_area: Sources
status: stable
title: DROP SOURCE
---

# DROP SOURCE

## Purpose
`DROP SOURCE` removes a source from Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


`DROP SOURCE` removes a source from Materialize.



`DROP SOURCE` removes a source from Materialize. If there are objects depending
on the source, you must explicitly drop them first, or use the `CASCADE`
option.

## Syntax

This section covers syntax.

```mzsql
DROP SOURCE [IF EXISTS] <source_name> [RESTRICT|CASCADE];
```text

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the named source does not exist.
`<source_name>` | The name of the source you want to remove.
**CASCADE** | Optional. If specified, remove the source and its dependent objects.
**RESTRICT** | Optional. Do not remove this source if it has dependent objects. _(Default.)_

## Examples

This section covers examples.

### Remove a source with no dependent objects

```mzsql
SHOW SOURCES;
```text
```nofmt
...
my_source
```text
```mzsql
DROP SOURCE my_source;
```bash

### Remove a source with dependent objects

```mzsql
SHOW SOURCES;
```text
```nofmt
...
my_source
```text
```mzsql
DROP SOURCE my_source CASCADE;
```bash

### Remove a source only if it has no dependent objects

You can use either of the following commands:

- ```mzsql
  DROP SOURCE my_source;
  ```text
- ```mzsql
  DROP SOURCE my_source RESTRICT;
  ```bash

### Do not issue an error if attempting to remove a nonexistent source

```mzsql
DROP SOURCE IF EXISTS my_source;
```

## Privileges

The privileges required to execute this statement are:

- Ownership of the dropped source.
- `USAGE` privileges on the containing schema.


## Related pages

- [`CREATE SOURCE`](../create-source)
- [`SHOW SOURCES`](../show-sources)
- [`SHOW CREATE SOURCE`](../show-create-source)
- [`DROP OWNED`](../drop-owned)

