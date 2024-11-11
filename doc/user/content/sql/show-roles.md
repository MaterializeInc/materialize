---
title: "SHOW ROLES"
description: "`SHOW ROLES` lists the roles available in Materialize."
menu:
  main:
    parent: 'commands'

---

`SHOW ROLES` lists the roles available in Materialize.

## Syntax

```mzsql
SHOW ROLES [ LIKE <pattern>  | WHERE <condition> ]
```

Option       | Description
-------------|------------
**LIKE**     | Specifies the pattern to filter the roles shown.
**WHERE**    | Specifies the condition(s) to filter the roles shown.

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
