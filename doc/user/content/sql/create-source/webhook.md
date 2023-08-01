---
title: "CREATE SOURCE: Webhook"
description: "Ingesting data into Materialize with HTTP requests"
pagerank: 50
menu:
  main:
    parent: 'create-source'
    identifier: webhook
    name: Webhook
    weight: 40
---

{{< private-preview />}}

{{% create-source/intro %}}
Webhook sources expose a public URL that allow your other applications to push data into Materialize.
{{% /create-source/intro %}}


## Syntax

{{< diagram "create-source-webhook.svg" >}}

## `webhook_body_format`

{{< diagram "webhook-body-format.svg" >}}

## `webhook_check_option`

{{< diagram "webhook-check-option.svg" >}}

Field                            | Use
---------------------------------|--------------------------
  _src_name_                     | The name for the source
 **IN CLUSTER** _cluster_name_   | The [cluster](/sql/create-cluster) to maintain this source.
 **INCLUDE HEADERS**             | Include a column named `'headers'` of type `map[text => text]` containing the headers of the request.
 **CHECK**                       | Specify a boolean expression that is used to validate each request received by the source.

### `BODY FORMAT` options

Field    | Value    | Description
---------|----------|-------------------------
`TEXT`   | `text`   | Parses the body of a request as UTF-8 text. If the body is not valid UTF-8, a response of `400` Bad Request will be returned.
`JSON`   | `jsonb`  | Parses the body of a request as JSON. If the body is not valid JSON, a respose of `400` Bad Request will be returned.
`BYTES`  | `bytea`  | Does no parsing of the request, stores the body as it was received.

### `CHECK WITH` options

Field                  | Value               | Description
-----------------------|---------------------|--------------
`BODY`                 | `text` or `bytea`   | Provide a column `'body'` to the check expression. The column can be renamed with the optional **AS** _alias_ statement, and the type can be changed to `bytea` with the optional **BYTES** keyword.
`HEADERS`              | `map[text=>text]` or `map[text=>bytea]` | Provide a column `'headers'` to the check expression. The column can be renamed with the optional **AS** _alias_ statement, and the type can be changed to `map[text => bytea]` with the optional **BYTES** keyword.
`SECRET` _secret_name_ | `text` or `bytea`    | Provide a column _secret_name_ to the check expression, with the value of the [`SECRET`](/sql/create-secret) _secret_name_. The column can be renamed with the optional **AS** _alias_ statement, and the type can be changed to `bytea` with the optional **BYTES** keyword.

## Webhook URL

After creating a webhook source, requests can be sent to `https://<HOST>/api/webhook/<database>/<schema>/<src_name>`.
Where `<HOST>` is the URL for your Materialize instance, which can be found on the [Materialize Web
Console](https://console.materialize.com/). Then `<database>` and `<schema>` are the database and
schema where you created your source, and `<src_name>` is the name you provided for your source at
the time of creation.

> **_Note_**: This is a public URL that is open to the internet and has no security. To validate that
requests are legitimate see [Validating Requests](/sql/create-source/webhook/validating_requests). For 
limits imposed on this endpoint see [Request Limits](/sql/create-source/webhook/request_limits).

## Validating Requests
[validating_requests]: #validating_requests

It's common for applications using webhooks to provide a method for validating a request is
legitimate. Using `CHECK` you can specify an expression to do this validation for your Webhook
Source.

For example, the following source HMACs the request body using the SHA256 hashing algorithm, and
asserts the result is equal to the value provided in the `x-signature` header, decoded with base64.

```
CREATE SOURCE my_webhook_source IN CLUSTER my_cluster FROM WEBHOOK
  BODY FORMAT JSON
  CHECK (
    WITH (
      HEADERS, BODY AS request_body,
      SECRET my_webhook_shared_secret,
    )
    decode(headers->'x-signature', 'base64') = hmac(request_body, my_webhook_shared_secret, 'sha256')
  )
```

The body and headers of the request are only provided for validation if `WITH ( BODY, HEADERS, ... )`
is specified as part of the `CHECK` statement. By default the type of `body` used for validation is
`text`, regardless of the `BODY FORMAT` you specified for the source. In the example above, the
`body` column for the `my_webhook_source` has a type of `jsonb`, but `request_body` as used in the
validation expression has type `text`. Futher, the request headers are not persisted as part of
`my_webhook_source` since `INCLUDE HEADERS` was not specified, but they are provided to the
validation expression.

> **_Note_**: Without a `CHECK` statement **all requests will be accepted**. To prevent bad actors from
inserting data it is **strongly encouraged** that you define a `CHECK` statement with your webhook
sources.

## Request Limits
[request_limits]: #request_limits

Webhook sources apply the following limits to received requests:

* Maximum size of the request body is `2MB`. Requests larger than this will fail with 413 Payload Too Large.
* Maximum number of concurrent connections is 100, across all webhook sources. Trying to connect
when the server is at the maximum will return 429 Too Many Requests.
* Requests that contain a header name specified more than one, will be rejected with 401 Unauthorized.
