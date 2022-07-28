---
title: "SHOW SECRETS"
description: "`SHOW SECRETS` lists the names of the secrets securely stored in Materialize."
menu:
  main:
    parent: commands
---

`SHOW SECRETS` lists the names of the secrets securely stored in Materialize. There is no way to show the contents of an existing secret, though you can override it using the [`ALTER SECRET`](../alter-secret) statement.

## Syntax

{{< diagram "show-secrets.svg" >}}

Field                | Use
---------------------|-----
_secret&lowbar;name_ | The schema to show secrets from. If omitted, secrets from the first schema in the search path are shown. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).

## Examples

```sql
SHOW SECRETS;
```

```nofmt
         name
-----------------------
 upstash_kafka_ca_cert
 upstash_sasl_password
 upstash_sasl_username
```

```sql
SHOW SECRETS FROM public LIKE '%cert%';
```

```nofmt
         name
-----------------------
 upstash_kafka_ca_cert
```

## Related pages

- [`CREATE SECRET`](../create-secret)
- [`ALTER SECRET`](../alter-secret)
- [`DROP SECRET`](../drop-secret)
