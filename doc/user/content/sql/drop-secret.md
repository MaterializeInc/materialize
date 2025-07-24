---
title: "DROP SECRET"
description: "`DROP SECRET` removes a secret from Materialize's secret management system."
menu:
  main:
    parent: 'commands'

---

`DROP SECRET` removes a secret from Materialize's secret management system. If
there are connections depending on the secret, you must explicitly drop them
first, or use the `CASCADE` option.

## Syntax

{{< diagram "drop-secret.svg" >}}

Field | Use
------|-----
**IF EXISTS** | Do not return an error if the specified secret does not exist.
_secret&lowbar;name_ | The secret you want to drop. For available secrets, see [`SHOW SECRETS`](../show-secrets).
**CASCADE** | Remove the secret and its dependent objects.
**RESTRICT** | Do not drop the secret if it has dependencies. _(Default)_

## Examples

### Dropping a secret with no dependencies

To drop an existing secret, run:

```mzsql
DROP SECRET kafka_sasl_password;
```

To avoid issuing an error if the specified secret does not exist, use the `IF EXISTS` option:

```mzsql
DROP SECRET IF EXISTS kafka_sasl_password;
```

### Dropping a secret with dependencies

If the secret has dependencies, Materialize will throw an error similar to:

```mzsql
DROP SECRET kafka_sasl_password;
```

```nofmt
ERROR:  cannot drop materialize.public.kafka_sasl_password: still depended upon by catalog
 item 'materialize.public.kafka_connection'
```

, and you'll have to explicitly ask to also remove any dependent objects using the `CASCADE` option:

```mzsql
DROP SECRET kafka_sasl_password CASCADE;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/drop-secret.md" >}}

## Related pages

- [`SHOW SECRETS`](../show-secrets)
- [`DROP OWNED`](../drop-owned)
