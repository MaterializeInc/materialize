---
title: "SHOW ROLES"
description: "`SHOW ROLES` lists the roles available in Materialize."
menu:
  main:
    parent: 'commands'

---

`SHOW ROLES` lists the roles available in Materialize.

## Syntax

{{< diagram "show-roles.svg" >}}

## Examples

```mzsql
SHOW ROLES;
```
```nofmt
 name
----------------
 joe@ko.sh
 mike@ko.sh
```

```mzsql
SHOW ROLES LIKE 'jo%';
```
```nofmt
 name
----------------
 joe@ko.sh
```

```mzsql
SHOW ROLES WHERE name = 'mike@ko.sh';
```
```nofmt
 name
----------------
 mike@ko.sh
```


## Related pages

- [`CREATE ROLE`](../create-role)
- [`DROP ROLE`](../drop-role)
