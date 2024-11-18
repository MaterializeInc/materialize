---
title: "SHOW SECRETS"
description: "`SHOW SECRETS` lists the names of the secrets securely stored in Materialize's secret management system."
menu:
  main:
    parent: commands
---

`SHOW SECRETS` lists the names of the secrets securely stored in Materialize's
secret management system. There is no way to show the contents of an existing
secret, though you can override it using the [`ALTER SECRET`](../alter-secret)
statement.

## Syntax

```mzsql
SHOW SECRETS [ FROM <schema_name> ] [ LIKE <pattern>  | WHERE <condition(s)> ]
```

Option                        | Description
------------------------------|------------
**FROM** <schema_name>        | If specified, only show secrets from the specified schema.  Defaults to first resolvable schema in the search path. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
**LIKE** \<pattern\>          | If specified, only show secrets whose name matches the pattern.
**WHERE** <condition(s)>      | If specified, only show secrets that meet the condition(s).

## Examples

```mzsql
SHOW SECRETS;
```

```nofmt
         name
-----------------------
 kafka_ca_cert
 kafka_sasl_password
 kafka_sasl_username
```

```mzsql
SHOW SECRETS FROM public LIKE '%cert%';
```

```nofmt
         name
-----------------------
 kafka_ca_cert
```

## Related pages

- [`CREATE SECRET`](../create-secret)
- [`ALTER SECRET`](../alter-secret)
- [`DROP SECRET`](../drop-secret)
