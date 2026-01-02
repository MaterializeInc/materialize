---
audience: developer
canonical_url: https://materialize.com/docs/sql/create-secret/
complexity: intermediate
description: '`CREATE SECRET` securely stores credentials in Materialize''s secret
  management system.'
doc_type: reference
keywords:
- CREATE CONNECTION
- CREATE SECRET
- ALTER SECRET
product_area: Indexes
status: stable
title: CREATE SECRET
---

# CREATE SECRET

## Purpose
`CREATE SECRET` securely stores credentials in Materialize's secret management system.

If you need to understand the syntax and options for this command, you're in the right place.


`CREATE SECRET` securely stores credentials in Materialize's secret management system.



A secret securely stores sensitive credentials (like passwords and SSL keys) in Materialize's secret management system. Optionally, a secret can also be used to store credentials that are generally not sensitive (like usernames and SSL certificates), so that all your credentials are managed uniformly.

## Syntax

[See diagram: create-secret.svg]

Field   | Use
--------|-----
_name_  | The identifier for the secret.
_value_ | The value for the secret. The _value_ expression may not reference any relations, and must be implicitly castable to `bytea`.

## Examples

This section covers examples.

```mzsql
CREATE SECRET kafka_ca_cert AS decode('c2VjcmV0Cg==', 'base64');
```

## Privileges

The privileges required to execute this statement are:

- `CREATE` privileges on the containing schema.


## Related pages

- [`CREATE CONNECTION`](../create-connection)
- [`ALTER SECRET`](../alter-secret)
- [`DROP SECRET`](../drop-secret)
- [`SHOW SECRETS`](../show-secrets)

