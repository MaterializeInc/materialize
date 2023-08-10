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

```sql
SHOW ROLES;
```
```nofmt
 name
----------------
 joe@ko.sh
 mike@ko.sh
```

```sql
SHOW ROLES LIKE 'jo%';
```
```nofmt
 name
----------------
 joe@ko.sh
```

```sql
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
