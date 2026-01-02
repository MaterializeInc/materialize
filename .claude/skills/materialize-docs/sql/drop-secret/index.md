---
audience: developer
canonical_url: https://materialize.com/docs/sql/drop-secret/
complexity: intermediate
description: '`DROP SECRET` removes a secret from Materialize''s secret management
  system.'
doc_type: reference
keywords:
- DROP THEM
- DROP SECRET
- IF EXISTS
- RESTRICT
- CASCADE
product_area: Indexes
status: stable
title: DROP SECRET
---

# DROP SECRET

## Purpose
`DROP SECRET` removes a secret from Materialize's secret management system.

If you need to understand the syntax and options for this command, you're in the right place.


`DROP SECRET` removes a secret from Materialize's secret management system.



`DROP SECRET` removes a secret from Materialize's secret management system. If
there are connections depending on the secret, you must explicitly drop them
first, or use the `CASCADE` option.

## Syntax

This section covers syntax.

```mzsql
DROP SECRET [IF EXISTS] <secret_name> [CASCADE|RESTRICT];
```text

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the specified secret does not exist.
_secret&lowbar;name_ | The secret you want to drop. For available secrets, see [`SHOW SECRETS`](../show-secrets).
**CASCADE** | Optional. If specified, remove the secret and its dependent objects.
**RESTRICT** | Optional. Do not drop the secret if it has dependencies. _(Default)_

## Examples

This section covers examples.

### Dropping a secret with no dependencies

To drop an existing secret, run:

```mzsql
DROP SECRET kafka_sasl_password;
```text

To avoid issuing an error if the specified secret does not exist, use the `IF EXISTS` option:

```mzsql
DROP SECRET IF EXISTS kafka_sasl_password;
```bash

### Dropping a secret with dependencies

If the secret has dependencies, Materialize will throw an error similar to:

```mzsql
DROP SECRET kafka_sasl_password;
```text

```nofmt
ERROR:  cannot drop materialize.public.kafka_sasl_password: still depended upon by catalog
 item 'materialize.public.kafka_connection'
```text

, and you'll have to explicitly ask to also remove any dependent objects using the `CASCADE` option:

```mzsql
DROP SECRET kafka_sasl_password CASCADE;
```

## Privileges

The privileges required to execute this statement are:

- Ownership of the dropped secret.
- `USAGE` privileges on the containing schema.


## Related pages

- [`SHOW SECRETS`](../show-secrets)
- [`DROP OWNED`](../drop-owned)

