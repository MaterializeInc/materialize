---
title: "ALTER SECRET"
description: "`ALTER SECRET` changes the contents of a secret."
menu:
  main:
    parent: 'commands'
---

`ALTER SECRET` changes the contents of a secret. To rename a secret, see [`ALTER...RENAME`](/sql/alter-rename/).

## Syntax

{{< diagram "alter-secret.svg" >}}

Field | Use
------|-----
_name_ | The identifier of the secret you want to alter.
_value_ | The new value for the secret. The _value_ expression may not reference any relations, and must be implicitly castable to `bytea`.

## Examples

```sql
ALTER SECRET upstash_kafka_ca_cert AS decode('c2VjcmV0Cg==', 'base64');
```

## Related pages

- [`ALTER...RENAME`](/sql/alter-rename/)
- [`SHOW SECRETS`](/sql/show-secrets)
- [`DROP SECRET`](/sql/drop-secret)
