---
title: "CREATE SECRET"
description: "`CREATE SECRET` securely stores credentials in Materialize's secret management system."
menu:
  main:
    parent: 'commands'

---

A secret securely stores sensitive credentials (like passwords and SSL keys) in Materialize's secret management system. Optionally, a secret can also be used to store credentials that are generally not sensitive (like usernames and SSL certificates), so that all your credentials are managed uniformly.

## Syntax

{{< diagram "create-secret.svg" >}}

Field   | Use
--------|-----
_name_  | The identifier for the secret.
_value_ | The value for the secret. The _value_ expression may not reference any relations, and must be implicitly castable to `bytea`.

## Examples

```sql
CREATE SECRET upstash_kafka_ca_cert AS decode('c2VjcmV0Cg==', 'base64');
```

## Related pages

- [`CREATE CONNECTION`](../create-connection)
- [`ALTER SECRET`](../alter-secret)
- [`DROP SECRET`](../drop-secret)
- [`SHOW SECRETS`](../show-secrets)
