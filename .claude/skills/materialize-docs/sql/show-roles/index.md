---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-roles/
complexity: intermediate
description: '`SHOW ROLES` lists the roles available in Materialize.'
doc_type: reference
keywords:
- WHERE
- SHOW ROLES
- LIKE
product_area: Indexes
status: stable
title: SHOW ROLES
---

# SHOW ROLES

## Purpose
`SHOW ROLES` lists the roles available in Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


`SHOW ROLES` lists the roles available in Materialize.



`SHOW ROLES` lists the roles available in Materialize.

## Syntax

This section covers syntax.

```mzsql
SHOW ROLES [ LIKE <pattern>  | WHERE <condition(s)> ];
```text

Syntax element             | Description
---------------------------|------------
**LIKE** \<pattern\>       | If specified, only show roles whose name matches the pattern.
**WHERE** <condition(s)>   | If specified, only show roles that meet the condition(s).

## Examples

This section covers examples.

```mzsql
SHOW ROLES;
```text
```nofmt
 name
----------------
 joe@ko.sh
 mike@ko.sh
```text

```mzsql
SHOW ROLES LIKE 'jo%';
```text
```nofmt
 name
----------------
 joe@ko.sh
```text

```mzsql
SHOW ROLES WHERE name = 'mike@ko.sh';
```text
```nofmt
 name
----------------
 mike@ko.sh
```


## Related pages

- [`CREATE ROLE`](../create-role)
- [`DROP ROLE`](../drop-role)

