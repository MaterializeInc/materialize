---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-secrets/
complexity: intermediate
description: '`SHOW SECRETS` lists the names of the secrets securely stored in Materialize''s
  secret management system.'
doc_type: reference
keywords:
- FROM
- WHERE
- ALTER SECRET
- SHOW SECRETS
- SHOW THE
- LIKE
product_area: Indexes
status: stable
title: SHOW SECRETS
---

# SHOW SECRETS

## Purpose
`SHOW SECRETS` lists the names of the secrets securely stored in Materialize's secret management system.

If you need to understand the syntax and options for this command, you're in the right place.


`SHOW SECRETS` lists the names of the secrets securely stored in Materialize's secret management system.



`SHOW SECRETS` lists the names of the secrets securely stored in Materialize's
secret management system. There is no way to show the contents of an existing
secret, though you can override it using the [`ALTER SECRET`](../alter-secret)
statement.

## Syntax

This section covers syntax.

```mzsql
SHOW SECRETS [ FROM <schema_name> ] [ LIKE <pattern>  | WHERE <condition(s)> ];
```text

Syntax element                | Description
------------------------------|------------
**FROM** <schema_name>        | If specified, only show secrets from the specified schema.  Defaults to first resolvable schema in the search path. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
**LIKE** \<pattern\>          | If specified, only show secrets whose name matches the pattern.
**WHERE** <condition(s)>      | If specified, only show secrets that meet the condition(s).

## Examples

This section covers examples.

```mzsql
SHOW SECRETS;
```text

```nofmt
         name
-----------------------
 kafka_ca_cert
 kafka_sasl_password
 kafka_sasl_username
```text

```mzsql
SHOW SECRETS FROM public LIKE '%cert%';
```text

```nofmt
         name
-----------------------
 kafka_ca_cert
```

## Related pages

- [`CREATE SECRET`](../create-secret)
- [`ALTER SECRET`](../alter-secret)
- [`DROP SECRET`](../drop-secret)

