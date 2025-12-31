# Tools and integrations

Get details about third-party tools and integrations supported by Materialize



Materialize is **wire-compatible** with PostgreSQL and can integrate with many
SQL clients and other tools that support PostgreSQL. To help you connect to
Materialize using various clients and tools, the following references are
available:

## Materialize Tools

- [mz - Materialize CLI](/integrations/cli/)
- [mz-debug (Debug tool)](/integrations/mz-debug/)

## SQL clients/client libraries

- [SQL clients](/integrations/sql-clients/)
- [Client Libraries](/integrations/client-libraries/)

## HTTP and WebSocket

- [Connect to Materialize via HTTP](/integrations/http-api/)
- [Connect to Materialize via WebSocket](/integrations/websocket-api/)

## LLM

- [LLM Integrations](/integrations/llm/)

## Foreign data wrapper

- [Foreign data wrapper](/integrations/fdw/)




---

## Client libraries


Applications can use various common language-specific PostgreSQL client
libraries to interact with Materialize and **create relations**, **execute
queries** and **stream out results**.

{{< note >}}
Client libraries tend to run complex introspection queries that may use configuration settings, system tables or features not yet implemented in Materialize. This means that even if PostgreSQL is supported, it's **not guaranteed** that the same integration will work out-of-the-box.
{{</ note >}}

| Language | Tested drivers                                                  | Notes                                                 |
| -------- | --------------------------------------------------------------- | ----------------------------------------------------- |
| Go       | [`pgx`](https://github.com/jackc/pgx)                           | See the [Go cheatsheet](/integrations/client-libraries/golang/).       |
| Java     | [PostgreSQL JDBC driver](https://jdbc.postgresql.org/)          | See the [Java cheatsheet](/integrations/client-libraries/java-jdbc/).  |
| Node.js  | [`node-postgres`](https://node-postgres.com/)                   | See the [Node.js cheatsheet](/integrations/client-libraries/node-js/). |
| PHP      | [`pdo_pgsql`](https://www.php.net/manual/en/ref.pgsql.php)      | See the [PHP cheatsheet](/integrations/client-libraries/php/).         |
| Python   | [`psycopg2`](https://pypi.org/project/psycopg2/)                | See the [Python cheatsheet](/integrations/client-libraries/python/).   |
| Ruby     | [`pg` gem](https://rubygems.org/gems/pg/)                       | See the [Ruby cheatsheet](/integrations/client-libraries/ruby/).       |
| Rust     | [`postgres-openssl`](https://crates.io/crates/postgres-openssl) | See the [Rust cheatsheet](/integrations/client-libraries/rust/).       |

👋 _Is there another client library you'd like to use with Materialize? Submit a
[feature
request](https://github.com/MaterializeInc/materialize/discussions/new?category=feature-requests&labels=A-integration)._




---

## Connect to Materialize via HTTP


You can access Materialize through its "session-less" HTTP API endpoint:

```bash
https://<MZ host address>/api/sql
```

## Details

### General semantics

The API:

- Requires username/password authentication, just as connecting via a SQL
  client (e.g. `psql`). Materialize provides you the username and password upon
  setting up your account.
- Requires that you provide the entirety of your request. The API does not
  provide session-like semantics, so there is no way to e.g. interactively use
  transactions.
- Ceases to process requests upon encountering the first error.
- Does not support statements whose semantics rely on sessions or whose state is
  indeterminate at the end of processing, including:
    - `CLOSE`
    - `COPY`
    - `DECLARE`
    - `FETCH`
    - `SUBSCRIBE`
- Supports specifying run-time [configuration parameters](/sql/set)
  via URL query parameters.

### Transactional semantics

The HTTP API provides two modes with slightly different transactional semantics from one another:

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

### OpenAPI spec

Download our [OpenAPI](https://swagger.io/specification/) v3 spec for this
interface: [environmentd-openapi.yml](/materialize-openapi.yml).

## Usage

### Endpoint

```
https://<MZ host address>/api/sql
```

Accessing the endpoint requires [basic authentication](https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication#basic_authentication_scheme). Reuse the same credentials as with a SQL client (e.g. `psql`):

* **User ID:** Your email to access Materialize.
* **Password:** Your app password.

#### Query parameters

You can optionally specify configuration parameters for each request, as a URL-encoded JSON object, with the `options` query parameter:

```
https://<MZ host address>/api/sql?options=<object>
```

For example, this is how you could specify the `application_name` configuration parameter with JavaScript:

```javascript
// Create and encode our parameters object.
const options = { application_name: "my_app" };
const encoded = encodeURIComponent(JSON.stringify(options));

// Add the object to our URL as the "options" query parameter.
const url = new URL(`https://${mzHostAddress}/api/sql`);
url.searchParams.append("options", encoded);
```

### Input format

#### Simple

The request body is a JSON object containing a key, `query`, which specifies the
SQL string to execute. `query` may contain multiple SQL statements separated by
semicolons.

```json
{
    "query": "select * from a; select * from b;"
}
```

#### Extended

The request body is a JSON object containing a key `queries`, whose value is
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

The output format is a JSON object with one key, `results`, whose value is
an array of the following:

Result | JSON value
---------------------|------------
Rows | `{"rows": <2D array of JSON-ified results>, "desc": <array of column descriptions>, "notices": <array of notices>}`
Error | `{"error": <Error object from execution>, "notices": <array of notices>}`
Ok | `{"ok": <tag>, "notices": <array of notices>}`

Each committed statement returns exactly one of these values; e.g. in the case
of "complex responses", such as `INSERT INTO...RETURNING`, the presence of a
`"rows"` object implies `"ok"`.

The `"notices"` array is present in all types of results and contains any
diagnostic messages that were generated during execution of the query. It has
the following structure:

```
{"severity": <"warning"|"notice"|"debug"|"info"|"log">, "message": <informational message>}
```

Note that the returned values include the results of statements which were
ultimately rolled back because of an error in a later part of the transaction.
You must parse the results to understand which statements ultimately reflect
the resultant state.

Numeric results are converted to strings to avoid possible JavaScript number inaccuracy.
Column descriptions contain the name, oid, data type size and type modifier of a returned column.

#### TypeScript definition

You can model these with the following TypeScript definitions:

```typescript
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

interface Column {
    name: string;
    type_oid: number; // u32
    type_len: number; // i16
    type_mod: number; // i32
}

interface Description {
	columns: Column[];
}

type SqlResult =
  | {
	tag: string;
	rows: any[][];
	desc: Description;
	notices: Notice[];
} | {
	ok: string;
	notices: Notice[];
} | {
	error: Error;
	notices: Notice[];
};
```

## Examples
### Run a transaction

Use the [extended input format](#extended) to run a transaction:
```bash
curl 'https://<MZ host address>/api/sql' \
    --header 'Content-Type: application/json' \
    --user '<username>:<passsword>' \
    --data '{
        "queries": [
            { "query": "CREATE TABLE IF NOT EXISTS t (a int);" },
            { "query": "CREATE TABLE IF NOT EXISTS s (a int);" },
            { "query": "BEGIN;" },
            { "query": "INSERT INTO t VALUES ($1), ($2)", "params": ["100", "200"] },
            { "query": "COMMIT;" },
            { "query": "BEGIN;" },
            { "query": "INSERT INTO s VALUES ($1), ($2)", "params": ["9", null] },
            { "query": "COMMIT;" }
        ]
    }'
```

Response:
```json
{
  "results": [
    {"ok": "CREATE TABLE", "notices": []},
    {"ok": "CREATE TABLE", "notices": []},
    {"ok": "BEGIN", "notices": []},
    {"ok": "INSERT 0 2", "notices": []},
    {"ok": "COMMIT", "notices": []},
    {"ok": "BEGIN", "notices": []},
    {"ok": "INSERT 0 2", "notices": []},
    {"ok": "COMMIT", "notices": []}
  ]
}
```

### Run a query

Use the [simple input format](#simple) to run a query:
```bash
curl 'https://<MZ host address>/api/sql' \
    --header 'Content-Type: application/json' \
    --user '<username>:<passsword>' \
    --data '{
        "query": "SELECT t.a + s.a AS cross_add FROM t CROSS JOIN s; SELECT a FROM t WHERE a IS NOT NULL;"
    }'
```

Response:
```json
{
  "results": [
    {
      "desc": {
        "columns": [
          {
            "name": "cross_add",
            "type_len": 4,
            "type_mod": -1,
            "type_oid": 23
          }
        ]
      },
      "notices": [],
      "rows": [],
      "tag": "SELECT 0"
    },
    {
      "desc": {
        "columns": [
          {
            "name": "a",
            "type_len": 4,
            "type_mod": -1,
            "type_oid": 23
          }
        ]
      },
      "notices": [],
      "rows": [],
      "tag": "SELECT 0"
    }
  ]
}
```

## See also
- [SQL Clients](../sql-clients)

[simple-query]: https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.4
[extended-query]: https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY




---

## Connect to Materialize via WebSocket


{{< private-preview enabled-by-default="true" />}}

You can access Materialize through its interactive WebSocket API endpoint:

```bash
wss://<MZ host address>/api/experimental/sql
```

## Details

### General semantics

The API:

- Requires username/password authentication, just as connecting via a SQL
  client (e.g. `psql`). Materialize provides you the username and password upon
  setting up your account.
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

Successful authentication will result in:

- Some `ParameterStatus` messages indicating the values of some initial session settings.
- One `BackendKeyData` message that can be used for cancellation.
- One `ReadyForQuery`message, at which point the server is ready to receive requests.

An error during authentication is indicated by a websocket Close message.
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
`ParameterStatus` | Announces the value of a session setting.
`BackendKeyData` | Information used to cancel queries.

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
    "code": <notice code>,
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
Either a `CommandComplete` or `Error` message will then follow indicating there are no more rows and the final result of the statement.
The payload has the following structure:

```
{
    "columns":
        [
            {
                "name": <column name>,
                "type_oid": <type oid>,
                "type_len": <type len>,
                "type_mod": <type mod>
            }
            ...
        ]
}
```

The inner object's various `type_X` fields are lower-level details that can be used to convert the row results from a string to a more specific data type.
`type_oid` is the OID of the data type.
`type_len` is the data size type (see `pg_type.typlen`).
`type_mod` is the type modifier (see `pg_attribute.atttypmod`).

#### `Row`

A single row result.
Will only occur after a `Rows` message.
The payload is an array of JSON values corresponding to the columns from the `Rows` message.
Numeric results are converted to strings to avoid possible JavaScript number inaccuracy.

#### `ParameterStatus`

Announces the value of a session setting.
These are sent during startup and when a statement caused a session parameter to change.
The payload has the following structure:

```
{
    "name": <name of parameter>,
    "value": <new value of parameter>,
}
```

#### `BackendKeyData`

Information used to cancel queries.
The payload has the following structure:

```
{
    "conn_id": <connection id>,
    "secret_key": <secret key>,
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

interface BackendKeyData {
	conn_id: number; // u32
	secret_key: number; // u32
}

interface Column {
    name: string;
    type_oid: number; // u32
    type_len: number; // i16
    type_mod: number; // i32
}

interface Description {
	columns: Column[];
}

type WebSocketResult =
    | { type: "ReadyForQuery"; payload: string }
    | { type: "Notice"; payload: Notice }
    | { type: "CommandComplete"; payload: string }
    | { type: "Error"; payload: Error }
    | { type: "Rows"; payload: Description }
    | { type: "Row"; payload: any[] }
    | { type: "ParameterStatus"; payload: ParameterStatus }
    | { type: "CommandStarting"; payload: CommandStarting }
    | { type: "BackendKeyData"; payload: BackendKeyData }
    ;
```

## Examples

### Run a query

```bash
$ echo '{"query": "select 1,2; values (4), (5)"}' | websocat wss://<MZ host address>/api/experimental/sql
{"type":"CommandStarting","payload":{"has_rows":true,"is_streaming":false}}
{"type":"Rows","payload":{"columns":[{"name":"?column?","type_oid":23,"type_len":4,"type_mod":-1},{"name":"?column?","type_oid":23,"type_len":4,"type_mod":-1}]}}
{"type":"Row","payload":["1","2"]}
{"type":"CommandComplete","payload":"SELECT 1"}
{"type":"CommandStarting","payload":{"has_rows":true,"is_streaming":false}}
{"type":"Rows","payload":{"columns":[{"name":"column1","type_oid":23,"type_len":4,"type_mod":-1}]}}
{"type":"Row","payload":["4"]}
{"type":"Row","payload":["5"]}
{"type":"CommandComplete","payload":"SELECT 2"}
{"type":"ReadyForQuery","payload":"I"}
```

## See also
- [SQL Clients](../sql-clients)

[simple-query]: https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.4
[extended-query]: https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY




---

## Connection Pooling


Because Materialize is wire-compatible with PostgreSQL, you can use any
PostgreSQL connection pooler with Materialize. In this guide, we’ll cover how to
use connection pooling with Materialize, alongside the common tool PgBouncer.

## PgBouncer
[PgBouncer](https://www.pgbouncer.org/) is a popular connection pooler for
PostgreSQL. It provides a lightweight and efficient way to manage database
connections, reducing the overhead of establishing new connections and
improving performance.

### Step 1: Install PgBouncer

You can [install PgBouncer](https://www.pgbouncer.org/downloads/) on your local machine or on a server.

### Step 2: Create an authentication userlist file

The userlist file contains the credentials for your Materialize user/app. The file has the format of:

```
"user@example.com" "mypassword-or-scram-secret"
```

#### If using `auth_type = plain` (Cloud and Self-Managed)

Specify the password in plaintext:

- **For Cloud**, use the password from an existing [Service Account](https://materialize.com/docs/security/cloud/users-service-accounts/create-service-accounts/) or generate a [new one](https://materialize.com/docs/security/cloud/users-service-accounts/create-service-accounts/).
- **For Self-Managed**, use the password associated with the role.

Example userlist file:
```
"foo@bar.com" "mypassword"
```

#### If using `auth_type = scram-sha-256` (Self-Managed only)

Specify the SCRAM secret. To find the SCRAM secret, run the following query as a superuser:

```sql
SELECT rolname, rolpassword FROM pg_authid WHERE rolname = 'your_role_name';
```

{{< note >}}
You must be a superuser to access the `pg_authid` table.
{{< /note >}}

Once you have the SCRAM secret, add it to the userlist file in the following format:
```
"your_role_name" "the-hash-you-got-from-pg_authid"
```

### Step 3: Configure PgBouncer to connect to your Materialize instance

1. Locate the configuration file. Refer to the [setup instructions](https://www.pgbouncer.org/downloads/) for your environment.

2. Update the file to:
   - Define a database named `materialize` with your Materialize connection details.
   - Configure PgBouncer as needed for your PgBouncer instance. Be sure to specify the `auth_type` and `auth_file` needed to connect to Materialize.

For example, the following is a basic configuration example to connect a local PgBouncer to a locally-running Materialize:

```ini
[databases]
;; For Cloud, use the connection details from the Console.
;; For Self-Managed, use your Materialize's connection details.
materialize = host=localhost port=6877

[pgbouncer]
logfile = /var/log/pgbouncer/pgbouncer.log
pidfile = /var/run/pgbouncer/pgbouncer.pid
;; Listen on localhost:6432 for incoming connections
listen_addr = localhost
listen_port = 6432

;; Set the authentication type
;; Materialize supports both plain and scram-sha-256.
;; Cloud and Self-Managed support plain.
;; auth_type = plain
;; Self-Managed also supports scram-sha-256:
auth_type = scram-sha-256

;; Set the authentication user list file
auth_file = /etc/pgbouncer/userlist.txt
```

For additional information on configuring PgBouncer, refer to the [PgBouncer documentation](https://www.pgbouncer.org/config.html).

### Step 4: Start the service and connect

After configuring PgBouncer, you can start the service. You can then connect to PgBouncer using the same connection parameters as you would for Materialize, but with the PgBouncer port (default is 6432). For example:

```bash
psql -h localhost -p 6432 -U your_role_name -d materialize
```




---

## Foreign data wrapper (FDW) 


{{< include-md file="shared-content/fdw-setup-intro.md" >}}

## Prerequisite

{{< include-md file="shared-content/fdw-setup-prereq.md" >}}

## Setup FDW in PostgreSQL

{{< include-md file="shared-content/fdw-setup-postgres.md" >}}




---

## MCP Server


The [Model Context Protocol (MCP) Server for Materialize](https://materialize.com/blog/materialize-turns-views-into-tools-for-agents/) lets large language models (LLMs) call your indexed views as real-time tools.
The MCP Server automatically turns any indexed view with a comment into a callable, typed interface that LLMs can use to fetch structured, up-to-date answers—directly from the database.

These tools behave like stable APIs.
They're governed by your SQL privileges, kept fresh by Materialize's incremental view maintenance, and ready to power applications that rely on live context instead of static embeddings or unpredictable prompt chains.

## Get Started

We recommend using [uv](https://docs.astral.sh/uv/) to install and run the server.
It provides fast, reliable Python environments with dependency resolution that matches pip.

If you don't have uv installed, you can install it first:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

To install and launch the MCP Server for Materialize:

```bash
uv venv
uv pip install mcp-materialize-agents
uv run mcp_materialize_agents
```

You can configure it using CLI flags or environment variables:

| Flag              | Env Var             | Default                                               | Description                                   |
| ----------------- | ------------------- | ----------------------------------------------------- | --------------------------------------------- |
| `--mz-dsn`        | `MZ_DSN`            | `postgres://materialize@localhost:6875/materialize`   | Materialize connection string                 |
| `--transport`     | `MCP_TRANSPORT`     | `stdio`                                               | Communication mode (`stdio`, `sse`, or `http`) |
| `--host`          | `MCP_HOST`          | `0.0.0.0`                                             | Host for `sse` and `http` modes               |
| `--port`          | `MCP_PORT`          | `3001` (sse), `8001` (http)                           | Port for `sse` and `http` modes               |
| `--pool-min-size` | `MCP_POOL_MIN_SIZE` | `1`                                                   | Minimum DB pool size                          |
| `--pool-max-size` | `MCP_POOL_MAX_SIZE` | `10`                                                  | Maximum DB pool size                          |
| `--log-level`     | `MCP_LOG_LEVEL`     | `INFO`                                                | Logging verbosity                             |


## Define Tools

Any view in Materialize can become a callable tool as long as it meets a few requirements to ensure that the tool is fast to query, safe to expose, and easy for language models to use correctly.

- [The view is indexed.](#1-define-and-index)
- [The view includes a top level comment.](#2-comment)
- [The role used to run the MCP Server must have required privileges.](#3-set-rbac-permissions)

### 1. Define and Index

You must create at least one [index](/concepts/indexes/) on the view. The columns in the index define the required input fields for the tool.

You can index a single column:

```mzsql
CREATE INDEX ON payment_status_summary (order_id);
```

Or multiple columns:

```mzsql
CREATE INDEX ON payment_status_summary (user_id, order_id);
```

Every indexed column becomes part of the tool's input schema.

### 2. Comment

The view must include a top-level comment that is used as the tool's description.
Comments should be descriptive as they help the model reason about what the tool does and when to use it.
You can optionally add a comment on any of the indexed columns to improve the tool's schema with descriptions for each field.

```mzsql
COMMENT ON VIEW payment_status_summary IS
  'Given a user ID and order ID, return the current payment status and last update time.
   Use this tool to drive user-facing payment tracking.';

COMMENT ON COLUMN payment_status_summary.user_id IS
  'The ID of the user who placed the order';

COMMENT ON COLUMN payment_status_summary.order_id IS
  'The unique identifier for the order';
```

### 3. Set RBAC Permissions

The database role used to run the MCP Server must:

* Have `USAGE` privileges on the database and schema the view is in.
* Have `SELECT` privileges on the view.
* Have `USAGE` privileges on the cluster where the index is installed.

```mzsql
GRANT USAGE on DATABASE materialize TO mcp_server_role;
GRANT USAGE on SCHEMA materialize.public TO mcp_server_role;
GRANT SELECT ON payment_status_summary TO mcp_server_role;
GRANT USAGE ON CLUSTER mcp_cluster TO mcp_server_role;
```

## Related Pages

* [CREATE VIEW](/sql/create-view)
* [CREATE INDEX](/sql/create-index)
* [COMMENT ON](/sql/comment-on)
* [CREATE ROLE](/sql/create-role)
* [GRANT PRIVILEGE](/sql/grant-privilege)




---

## mz - Materialize CLI


The Materialize command-line interface (CLI), lets you interact with
Materialize from your terminal.

You can use `mz` to:

  * Enable new regions
  * Run SQL commands against a region
  * Create app passwords
  * Securely manage secrets
  * Invite new users to your organization

## Getting started

1. Install `mz`:

   ```shell
   # On macOS:
   brew install materializeinc/materialize/mz
   # On Ubuntu/Debian:
   curl -fsSL https://dev.materialize.com/apt/materialize.sources | sudo tee /etc/apt/sources.list.d/materialize.sources
   sudo apt update
   sudo apt install materialize-cli
   ```

   See [Installation](installation) for additional installation options.

2. Log in to your Materialize account:

   ```shell
   mz profile init
   ```

   `mz` will attempt to launch your web browser and ask you to log in.

   See [Configuration](configuration) for alternative configuration methods.

3. Show enabled regions in your organization:

   ```shell
   $ mz region list
   ```
   ```
   aws/us-east-1  enabled
   aws/eu-west-1  disabled
   ```

4. Launch a SQL shell connected to one of the enabled regions in your
   organization:

   ```shell
   $ mz sql
   ```
   ```
   Authenticated using profile 'default'.
   Connected to the quickstart cluster.
   psql (14.2)
   Type "help" for help.

   materialize=>
   ```

   You can use the `--region=aws/us-east-1` flag with the name of an enabled region
   in your organization. If you don't yet have an enabled region, use
   [`mz region enable`](reference/region) to enable one.

## Command reference

Command          | Description
-----------------|------------
[`app-password`] | Manage app passwords for your user account.
[`config`]       | Manage configuration for `mz`.
[`profile`]      | Manage authentication profiles for `mz`.
[`region`]       | Manage regions in your organization.
[`secret`]       | Manage secrets in a region.
[`sql`]          | Execute SQL statements in a region.
[`user`]         | Manage users in your organization.

## Global flags

These flags can be used with any command and may be intermixed with any
command-specific flags.

{{% cli-global-args %}}

[Homebrew]: https://brew.sh
[homebrew-tap]: https://github.com/MaterializeInc/homebrew-materialize
[`app-password`]: reference/app-password
[`config`]: reference/config
[`profile`]: reference/profile
[`region`]: reference/region
[`secret`]: reference/secret
[`sql`]: reference/sql
[`user`]: reference/user




---

## mz-debug


`mz-debug` is a command-line interface tool that collects debug information for self-managed and emulator Materialize environments. By default, the tool creates a compressed file (`.zip`) containing logs and a dump of the system catalog. You can then share this file with support teams when investigating issues.

## Install `mz-debug`

{{< tabs >}}

{{< tab "macOS" >}}

```shell
ARCH=$(uname -m)
sudo echo "Preparing to extract mz-debug..."
curl -L "https://binaries.materialize.com/mz-debug-latest-$ARCH-apple-darwin.tar.gz" \
| sudo tar -xzC /usr/local --strip-components=1
```
{{</ tab >}}
{{< tab "Linux" >}}
```shell
ARCH=$(uname -m)
sudo echo "Preparing to extract mz-debug..."
curl -L "https://binaries.materialize.com/mz-debug-latest-$ARCH-unknown-linux-gnu.tar.gz" \
| sudo tar -xzC /usr/local --strip-components=1
{{</ tab >}}
{{</ tabs >}}

### Get version and help

To see the version of `mz-debug`, specify the `--version` flag:

```shell
mz-debug --version
```

To see the options for running `mz-debug`,

```shell
mz-debug --help
```

## Next steps

To run `mz-debug`, see
- [`mz-debug self-managed`](./self-managed)
- [`mz-debug emulator`](./emulator)




---

## SQL clients


Materialize is **wire-compatible** with PostgreSQL, which means it integrates
with many SQL clients that support PostgreSQL. In this guide, we’ll cover how to
connect to your Materialize region using some common SQL clients.

## Connection parameters

You can find the credentials for your Materialize region in the
[Materialize console](/console/), under **Connect
externally** in the navigation bar.

Field             | Value
----------------- | ----------------
Host              | Materialize host name.
Port              | **6875**
Database name     | Database to connect to (default: **materialize**).
Database username | Materialize user.
Database password | App password for the Materialize user.
SSL mode          | **Require**

Before connecting, double-check that you've created an **app-password** for your
user. This password is auto-generated, and prefixed with `mzp_`.

### Runtime connection parameters

{{< warning >}}
Parameters set in the connection string work for the **lifetime of the
session**, but do not affect other sessions. To permanently change the default
value of a configuration parameter for a specific user (i.e. role), use the
[`ALTER ROLE...SET`](/sql/alter-role) command.
{{< /warning >}}

You can pass runtime connection parameters (like `cluster`, `isolation_level`,
or `search_path`) to Materialize using the [`options` connection string
parameter](https://www.postgresql.org/docs/14/libpq-connect.html#LIBPQ-PARAMKEYWORDS),
or the [`PGOPTIONS` environment variable](https://www.postgresql.org/docs/16/config-setting.html#CONFIG-SETTING-SHELL).
As an example, to specify a different cluster than the default defined for the
user and set the transactional isolation to serializable on connection using
`psql`:

```bash
# Using the options connection string parameter
psql "postgres://<MZ_USER>@<MZ_HOST>:6875/materialize?sslmode=require&options=--cluster%3Dprod%20--transaction_isolation%3Dserializable"
```

```bash
# Using the PGOPTIONS environment variable
PGOPTIONS='--cluster=prod --transaction_isolation=serializable' \
psql \
    --username=<MZ_USER> \
    --host=<MZ_HOST> \
    --port=6875 \
    --dbname=materialize
```

## Tools

### DataGrip

{{< tip >}}

Integration with DataGrip/WebStorm is currently limited. Certain features --
such as the schema explorer, database introspection, and various metadata panels
-- may not work as expected with Materialize because they rely on
PostgreSQL-specific queries that use unsupported system functions (e.g.,
`age()`) and system columns (e.g., `xmin`).

As an alternative, you can [use the JDBC metadata
introspector](https://www.jetbrains.com/help/datagrip/cannot-find-a-database-object-in-the-database-tree-view.html#temporarily-enable-introspection-with-jdbc-metadata).
To use the JDBC metadata instrospector, from your data source properties, in the
**Advanced** tab, select **Introspect using JDBC Metadata** from the **Expert
options** list. For more information, see the [DataGrip
documentation](https://www.jetbrains.com/help/datagrip/cannot-find-a-database-object-in-the-database-tree-view.html#temporarily-enable-introspection-with-jdbc-metadata).

{{< /tip >}}

To connect to Materialize using [DataGrip](https://www.jetbrains.com/help/datagrip/connecting-to-a-database.html),
follow the documentation to [create a connection](https://www.jetbrains.com/help/datagrip/connecting-to-a-database.html)
and use the **PostgreSQL database driver** with the credentials provided in the
Materialize console.

<img width="1131" alt="DataGrip Materialize Connection Details" src="https://user-images.githubusercontent.com/21223421/218108169-302c8597-35a9-4dce-b16d-050f49538b9e.png">

[use the JDBC metadata
introspector]:

### DBeaver

**Minimum requirements:** DBeaver 23.1.3

To connect to Materialize using [DBeaver](https://dbeaver.com/docs/wiki/),
follow the documentation to [create a connection](https://dbeaver.com/docs/wiki/Create-Connection/)
and use the **Materialize database driver** with the credentials provided in the
Materialize console.

<img width="1314" alt="Connect using the credentials provided in the Materialize console" src="https://github.com/MaterializeInc/materialize/assets/23521087/ae98dc45-2e1a-4e78-8ca0-e8d53beed30f">

The Materialize database driver depends on the [PostgreSQL JDBC driver](https://jdbc.postgresql.org/download.html).
If you don't have the driver installed locally, DBeaver will prompt you to
automatically download and install the most recent version.

#### Connect to a specific cluster

By default, Materialize connects to the [pre-installed `quickstart` cluster](/sql/show-clusters/#pre-installed-clusters).
To connect to a specific [cluster](/concepts/clusters), you must
define a bootstrap query in the connection initialization settings.

<br>

1. Click on **Connection details**.

1. Click on **Connection initialization settings**.

1. Under **Bootstrap queries**, click **Configure** and add a new SQL query that
sets the active cluster for the connection:

    ```mzsql
    SET cluster = other_cluster;
    ```

Alternatively, you can change the default value of the `cluster` configuration
parameter for a specific user (i.e. role) using the [`ALTER
ROLE...SET`](/sql/alter-role) command.

#### Show system objects

By default, DBeaver hides system catalog objects in the database explorer. This
includes tables, views, and other objects in the `mz_catalog` and `mz_internal`
schemas.

To show system objects in the database explorer:

1. Right-click on the database connection in the **Database Navigator**.
1. Click on **Edit Connection**.
1. In the **Connection settings** tab, select **General**.
1. Next to the **Navigator view**, click **Customize**.
1. In the **Navigator settings** dialog, check the **Show system objects** checkbox.
1. Click **OK**.

### TablePlus

{{< note >}}
As we work on extending the coverage of `pg_catalog` in Materialize,
some TablePlus features might not work as expected.
{{< /note >}}

To connect to Materialize using [TablePlus](https://tableplus.com/),
follow the documentation to [create a connection](https://docs.tableplus.com/gui-tools/manage-connections#create-a-connection)
and use the **PostgreSQL database driver** with the credentials provided in the
Materialize console.

<img width="1440" alt="Screenshot 2023-12-28 at 18 45 11" src="https://github.com/MaterializeInc/materialize/assets/23521087/c530769e-3445-49c8-8f36-9f7166352ac4">

### `psql`

{{< warning >}}
Not all features of `psql` are supported by Materialize yet, including some backslash meta-commands.
{{< /warning >}}

{{< tabs >}}
{{< tab "macOS">}}

Start by double-checking whether you already have `psql` installed:

```shell
psql --version
```

Assuming you’ve installed [Homebrew](https://brew.sh/):

```shell
brew install libpq
```

Then symlink the `psql` binary to your `/usr/local/bin` directory:

```shell
brew link --force libpq
```

{{< /tab >}}

{{< tab "Linux">}}

Start by double-checking whether you already have `psql` installed:

```shell
psql --version
```

```bash
sudo apt-get update
sudo apt-get install postgresql-client
```

The `postgresql-client` package includes only the client binaries, not the PostgreSQL server.

For other Linux distributions, check out the [PostgreSQL documentation](https://www.postgresql.org/download/linux/).

{{< /tab >}}

{{< tab "Windows">}}

Start by double-checking whether you already have `psql` installed:

```shell
psql --version
```

Download and install the [PostgreSQL installer](https://www.postgresql.org/download/windows/) certified by EDB.
{{< /tab >}}
{{< /tabs >}}



