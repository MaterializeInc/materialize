---
title: "Connect to Materialize via WebSocket"
description: "How to use Materialize via WebSocket"
menu:
  main:
    parent: "integrations"
    weight: 30
    name: "WebSocket API"
---

{{< alpha enabled-by-default="true" />}}

You can access Materialize through its interactive WebSocket API endpoint:

```bash
wss://<MZ host address>/api/experimental/sql
```

## Details

### General semantics

The API:

- Requires username/password authentication, just as connecting via `psql`.
  Materialize provides you the username and password upon setting up your
  account.
- Maintains an interactive session.
- Does not support some statements:
    - `CLOSE`
    - `COPY`
    - `DECLARE`
    - `FETCH`
- Supports specifying run-time configuration parameters ([session variables](https://www.postgresql.org/docs/current/sql-set.html))
  via the initial authentication message.

### Transactional semantics

The WebSocket API provides two modes with slightly different transactional semantics from one another:

- **Simple**, which mirrors PostgreSQL's [Simple Query][simple-query] protocol.
    - Supports a single query, but the single query string may contain multiple
      statements, e.g. `SELECT 1; SELECT 2;`
    - Treats all statements as in an implicit transaction unless other
      transaction control is invoked.
- **Extended**, which mirrors PostgreSQL's [Extended Query][extended-query] protocol.
    - Supports multiple queries, but only one statement per query string.
    - Supports parameters.
    - Eagerly commits DDL (e.g. `CREATE TABLE`) in implicit transactions, but
      not DML (e.g. `INSERT`).

## Usage

### Endpoint

```
wss://<MZ host address>/api/experimental/sql
```

To authenticate using a username and password, send an initial text or binary message containing a JSON object:

```
{
    "user": "<Your email to access Materialize>",
    "password": "<Your app password>",
    "options": { <Optional map of session variables> }
}
```

To authenticate using a token, send an initial text or binary message containing a JSON object:

```
{
    "token": "<Your access token>",
    "options": { <Optional map of session variables> }
}
```

Successful authentication will result in an initial `ReadyForQuery` response from the server.
Otherwise an error is indicated by a websocket Close message.

HTTP `Authorization` headers are ignored.

### Messages

WebSocket Text or Binary messages can be sent.
The payload is described below in the [Input format](#input-format) section.
Each request will respond with some number of response messages, followed by a `ReadyForQuery` message.
There is exactly one `ReadyForQuery` message for each request, regardless of how many queries the request contains.

### Input format

#### Simple

The message payload is a JSON object containing a key, `query`, which specifies the
SQL string to execute. `query` may contain multiple SQL statements separated by
semicolons.

```json
{
    "query": "select * from a; select * from b;"
}
```

#### Extended

The message payload is a JSON object containing a key `queries`, whose value is
array of objects, whose structure is:

Key | Value
----|------
`query` | A SQL string containing one statement to execute
`params` | An optional array of text values to be used as the parameters to `query`. _null_ values are converted to _null_ values in Materialize. Note that all parameter values' elements must be text or _null_; the API will not accept JSON numbers.

```json
{
    "queries": [
        { "query": "select * from a;" },
        { "query": "select a + $1 from a;", "params": ["100"] }
        { "query": "select a + $1 from a;", "params": [null] }
    ]
}
```

### Output format

The response messages are WebSocket Text messages containing a JSON object that contains keys `type` and `payload`.

`type` value | Description
---------------------|------------
`ReadyForQuery` | Sent at the end of each response batch
`Notice` | An informational notice.
`CommandStarting` | A command has executed and response data will be returned.
`CommandComplete` | Executing a statement succeeded.
`Error` | Executing a statement resulted in an error.
`Rows` | A rows-returning statement is executing, and some `Row` messages may follow.
`Row` | A single row result.
`ParameterStatus` | Executing a statement caused some session parameters to change.

#### `ReadyForQuery`

Exactly one of these is sent at the end of every request batch.
It can be used to synchronize with the server, and means the server is ready for another request.
(However, many requests can be made at any time; there is no need to wait for this message before issuing more requests.)
The payload is a `string` describing the current transaction state:

- `I` for idle: not in a transaction.
- `T` for in a transaction.
- `E` for a transaction in an error state. A request starting with `ROLLBACK` should be issued to exit it.

#### `Notice`

A notice can appear at any time and contains diagnostic messages that were generated during execution of the query.
The payload has the following structure:

```
{
    "message": <informational message>,
    "severity": <"warning"|"notice"|"debug"|"info"|"log">,
    "detail": <optional error detail>,
    "hint": <optional error hint>,
}
```

#### `Error`

Executing a statement resulted in an error.
The payload has the following structure:

```
{
    "message": <informational message>,
    "code": <error code>,
    "detail": <optional error detail>,
    "hint": <optional error hint>,
}
```

#### `CommandStarting`

A statement has executed and response data will be returned.
This message can be used to know if rows or streaming data will follow.
The payload has the following structure:

```
{
    "has_rows": <boolean>,
    "is_streaming": <boolean>,
}
```

The `has_rows` field is `true` if a `Rows` message will follow.
The `is_streaming` field is `true` if there is no expectation that a `CommandComplete` message will ever occur.
This is the case for `SUBSCRIBE` queries.

#### `CommandComplete`

Executing a statement succeeded.
The payload is a `string` containing the statement's tag.

#### `Rows`

A rows-returning statement is executing and some number (possibly 0) of `Row` messages will follow.
The payload is an array of `string` containing the column names of the row results.
Either a `CommandComplete` or `Error` message will always follow indicating there are no more rows and the final result of the statement.

#### `Row`

A single row result.
Will only occur after a `Rows` message.
The payload is an array of JSON values corresponding to the columns from the `Rows` message.

#### `ParameterStatus`

Executing a statement caused a session parameter to change. The payload has the following structure:

```
{
    "name": <name of parameter>,
    "value": <new value of parameter>,
}
```

#### TypeScript definition

You can model these with the following TypeScript definitions:

```typescript
type Auth =
    | { user: string; password: string; options?: { [name: string]: string } }
    | { token: string; options?: { [name: string]: string } }
    ;

interface Simple {
    query: string;
}

interface ExtendedRequest {
    query: string;
    params?: (string | null)[];
}

interface Extended {
    queries: ExtendedRequest[];
}

type SqlRequest = Simple | Extended;

interface Notice {
	message: string;
	severity: string;
	detail?: string;
	hint?: string;
}

interface Error {
	message: string;
	code: string;
	detail?: string;
	hint?: string;
}

interface ParameterStatus {
	name: string;
	value: string;
}

interface CommandStarting {
	has_rows: boolean;
	is_streaming: boolean;
}

type WebSocketResult =
    | { type: "ReadyForQuery"; payload: string }
    | { type: "Notice"; payload: Notice }
    | { type: "CommandComplete"; payload: string }
    | { type: "Error"; payload: Error }
    | { type: "Rows"; payload: string[] }
    | { type: "Row"; payload: any[] }
    | { type: "ParameterStatus"; payload: ParameterStatus }
    | { type: "CommandStarting"; payload: CommandStarting }
    ;
```

## Examples

### Run a query

```bash
$ echo '{"query": "select 1,2; values (4), (5)"}' | websocat wss://<MZ host address>/api/experimental/sql
{"type":"CommandStarting","payload":{"has_rows":true,"is_streaming":false}}
{"type":"Rows","payload":["?column?","?column?"]}
{"type":"Row","payload":["1","2"]}
{"type":"CommandComplete","payload":"SELECT 1"}
{"type":"CommandStarting","payload":{"has_rows":true,"is_streaming":false}}
{"type":"Rows","payload":["column1"]}
{"type":"Row","payload":["4"]}
{"type":"Row","payload":["5"]}
{"type":"CommandComplete","payload":"SELECT 2"}
{"type":"ReadyForQuery","payload":"I"}
```

## See also
- [SQL Clients](../sql-clients)

[simple-query]: https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.4
[extended-query]: https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
