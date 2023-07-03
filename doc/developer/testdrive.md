# Introduction

Testdrive is a Materialize invention that feels a lot like sqllogictest, except it supports plugable commands for interacting with external systems, like Kafka. Multiple test suites use `testdrive` to drive the test, but the bulk of the functional tests live in `test/testdrive`.

`testdrive` is an "expected vs. actual output" type of framework, with both the SQL queries and other actions and their expected results specified in a single file with a `.td` extension. As such, it does not allow for code-like unit tests or assertion-based testing.

# Example

Here's a representative snippet of a testdrive file:

```
$ kafka-ingest format=avro topic=data schema=${schema} timestamp=42
{"before": null, "after": {"a": 1, "b": 1}}

> SELECT * FROM data
a b
---
1 1
```

The first command, the `$ kafka-ingest ...` action, ingests some data into
a topic named `data`. The next command, `> SELECT * FROM data` operates like a
sqllogictest file. It runs the specified SQL command against Materialize and
verifies the result.

Commands in testdrive are introduced with a `$`, `>`, or `!` character. These
were designed to be reminiscent of how your shell would look if you were running
these actions commands. `$` actions take the name of a testdrive action
(like `kafka-ingest` or `set`), followed by any number of `key=value` pairs. `>`
and `!` introduce a SQL command that is expected to succeed or fail,
respectively.

# When to use

Testdrive is the preferred system test framework when testing:

* sources, sinks, and interactions with external systems in general,
* interactions between objects in the catalog,
* serialization of data types over the PostgreSQL wire protocol.

# When not to use

`testdrive` is currently sub-optimal for the situations listed below and `sqllogictest` is the better driver in those cases:
- test cases containing many `EXPLAIN` statements and a lot of recorded query plans;
- test cases where substantial changes of the ouput over time are expected, which is often the case if many query plans have been recorded in the test.

Unlike the `sqllogictest` driver, `testdrive` will fail the test at the first difference, and there is no ability to automatically produce a new version of the test file that includes all the required updates to make the test pass.

# Running

`testdrive` can be run in the following ways:

## As a fully containerized, self-sufficient test via mzcompose

The easiest way to run testdrive tests is via [mzcompose](mzcompose.md).

The mzcompose configuration will automatically set up all of testdrive's
dependencies, including Zookeeper, Kafka, and the Confluent Schema Registry.

**WARNING:** On ARM-based machines (like M1 Macs), running the Confluent
Platform in Docker is nearly unusably slow because it runs under QEMU emulation.
(Confluent [refuses to announce a timeline][confluent-arm] for when they will
provide ARM images.) As a workaround, run tests using Redpanda instead of the
Confluent Platform, or run tests locally without mzcompose.

For full reference documentation on the available mzcompose options, consult the
help text:

```
./mzcompose run default --help
```

### Common invocations

Run testdrive against all files in `test/testdrive` using Confluent Platform and
Localstack:

```
./mzcompose --dev run default
```

Run using Redpanda instead of the Confluent Platform:

```
./mzcompose --dev run default --redpanda
```

Run testdrive against a single file:

```
./mzcompose --dev run default FILE.td
```

## Running tests locally without mzcompose

Most testdrive scripts live in [test/testdrive](/test/testdrive). To run a
testdrive script, you'll need two terminal windows open. In the first terminal,
run Materialize:

```shell
$ ./bin/environmentd --release
```

In the second terminal, run testdrive:

```shell
$ cargo run --bin testdrive --release -- test/testdrive/TESTFILE.td
```

Many testdrive tests require Zookeeper, Kafka, and
the Confluent Schema Registry. See the [unit tests docs for starting them](guide-testing.md#unitintegration-tests).

For testdrive tests that interacts with Amazon AWS, you will need to be
running [LocalStack][]. To install and run LocalStack:

```shell
$ pip install localstack
$ START_WEB=false SERVICES=iam,sts localstack start
```

If you've previously installed LocalStack, be sure it is v0.11 or later.


If you are running services (Kafka, LocalStack, etc.) on nonstandard ports,
you can configure the correct URLs with command-line options. Ask testdrive
to tell you about these options with the `--help` flag:

```
cargo run --bin testdrive -- --help
```

[LocalStack]: https://github.com/localstack/localstack

# Creating tests

## Test format

A `testdrive` `.td` test starts with a copyright header and must not contain trailing spaces and trailing newlines. Those properties are checked by the `bin/lint` linter and also enforced by the CI.

## Test naming

Tests related to Kafka are named `kafka-*.td`. This allows various CI jobs that deal with Kafka to pick them up, e.g. to run them against different Kafka versions, against Redpanda, etc.

## Test location

If the test uses local files or kafka sources and sinks, it belongs in `test/testdrive`. Tests that relate to direct Postgres replication belong in `test/pg-cdc`. Tests that use Debezium belong in `test/debezium`.

A test placed in a particular directory will be automatically picked for execution, there is no need to declare the existence of the test in another file.

# Command-line options

The `testdrive` binary accepts the following command-line options. They are usually set appropriately by the `mzcompose` workflow that calls `testdrive`.

## Default timeout

#### `--default-timeout <default-timeout>`

Default timeout as a Duration string [default: "10s"]. A timeout at least that long will be applied to all operations.

#### `--initial-backoff <initial-backoff>`

Specifies the initial backoff interval that will be applied as testdrive retries queries. Default is "50ms".

#### `--backoff-factor <backoff-factor>`

Specifies the backoff factor that will be applied to increase the backoff interval between retries. Default is 1.5.

## Interfacing with services

#### `--materialize-url <materialize-url>`

materialized connection string [default: postgres://materialize@localhost:6875]

#### `--cert <PATH>`

Path to TLS certificate keystore. The keystore must be in the PKCS#12 format.

#### `--cert-password <PASSWORD>`

Password for the TLS certificate keystore

#### `--kafka-addr <[ENCRYPTION://]HOST:PORT>`

Kafka bootstrap address [default: localhost:9092]

#### `--kafka-option <KEY=VAL>...`

Kafka configuration option use when connecting or performing operations. For example:

```
--kafka-option=security.protocol=SASL_PLAINTEXT --kafka-option acks=all
```

#### `--schema-registry-url <URL>`

Schema registry URL [default: http://localhost:8081]

## Resource sharing and reuse across tests

By default, `testdrive` will clean up the environment and Mz prior to running each test. In certain situations, two or more testdrive invocations need to run consequtively while being able to operate on the same database objects. The options below help to ensure successful cooperation.

#### `--no-reset`

By default, `testdrive` will clean up its environment as much as it can before the start of the test, which includes Mz databases, etc. If two consequtive invocations of `testdrive` need to be able to operate on the same objects, e.g. database tables, the second invocation needs to run with `--no-reset`

#### `--seed <seed>`

Unless specified, each `testdrive` invocation will use random names for certain objects, such as kafka topics. Using `--seed` allows for multiple `testdrive` instances to see and operate on the same objects. Note that each testdrive invocation will still receive its own temporary directory unless the `--temp-dir` option is specified.

#### `--temp-dir <temp-dir>`

Force the use of the specfied temporary directory rather than creating one with a random name. This allows multiple `testdrive` instances to be able to find the same files at the same location.

## Controlling test suite execution

Those options are useful in narrow testing scenarios where a full, deterministic, end-to-end testdrive run is not desirable.

#### `--max-errors <max-errors>`

Maximum number of errors before aborting [default: 10]. Note that currently this allows more than one test to fail, but it does not allow tolerating multiple failures within a test. Individual tests will still fail and exit at the first error or actual/expected difference.

#### `--max-tests <max-tests>`

Max number of tests to run before terminating. This is useful in conjunction with `--seed` and `--shuffle-tests` to produce random shorter workloads against the Mz server.

#### `--shuffle-tests`

Shuffle the list of tests before running them (using the value from --seed, if any). This is useful when attempting to use `testdrive` in randomized scenarios or when applying background workload to the database.

## Other options

#### `--validate-postgres-stash=postgres://root@materialized:26257?options=--search_path=adapter`

After executing a DDL statement, validate that representation of the catalog in the stash is identical to the in-memory one.

# Executing statements

## Executing a SQL query

```
> SELECT* from t1;
row1_val1 row1_val2
row2_val1 row2_val2
```

This will execute the query after the `>` sign and will check that rows return match the expected rows provided after the statement. If the rows do not match, the statement will be retried until a timeout occurs.

Rows are pre-sorted before comparing, so the order in which the expected rows are listed in the `.td` file does not matter.

Within each row, individual columns are separated by spaces.

### Escaping and quoting

If a particular expected column value includes a space itself, quote it with double quotes:

```
> SELECT 'this row';
"this row"
```

If the expected value needs to include double quotes, quote them:

```
> SELECT '"this"';
\"this\"
```

### Specifying expected column names


The "expected results" block after the `>` statement can include column names as well, if they are separated from the data rows by `---`:

```
> SELECT 1 AS ts;
ts
---
1
```

### Multi-line statements

To execute a multi-line statement, indent all lines after the first by two spaces:

```
> SELECT f1
  FROM t1;
```

## Executing a DDL statement

The syntax is identical, however the statement will not be retried on error:

```
> DROP TABLE IF EXISTS t1
```

## Executing a statement that is expected to return an error

Specify a statement that is expected to return an error with the `!` sigil. For
example:

```
! SELECT * FROM no_such_table
contains:unknown catalog item
```

The expected error is specified after the statement. `testdrive` will retry the
statement until the expected error is returned or a timeout occurs.

In the example above, the full error returned by Materialize is:

```
ERROR:  unknown catalog item 'no_such_table'
```

The `contains:` prefix in the expected error means that the presence of the
substring `unknown catalog item` in the error message is considered a pass.

There are four types of error expectations:

  * `contains:STRING` expects any error message containing `STRING`.
  * `exact:STRING` expects an error message that is exactly `STRING`.
  * `regex:REGEX` expects an error message that matches the regex `REGEX`.
  * `timeout` expects the query to time out.


You can include include variables in `contains`, `exact`, and `regex`
expectations:

```
$ set table_name=no_table

$ SELECT * FROM ${table_name}
exact:ERROR:  unknown catalog item '${table_name}'
```

With regex matches, the values of any interpolated variables are matched
literally, i.e.,  their values are *not* treated as regular expression patterns.
For example, a variable with value `my.+name` will match the string `my.+name`,
not `my friend's name`.

Additionally, you can add one or both of the following, after the error
expectation field:
```
detail:expected detail
hint:expected hint
```

These assert that the `DETAIL` or `HINT` fields _contains_ the expected message.
These fields in the postgres error response are otherwise ignored. If both
are specified, `detail` must come before `hint`.

## Executing statements in multiple sessions

By default, `testdrive` will run all `>` and `!` statements in a single connection. A limited form of multiple connections is provided by the `$ postgres-connect` action. See the "Connecting to databases over the pgwire protocol" section below.

## Executing an `EXPLAIN` statement

Since the output of `EXPLAIN` is a multi-line string, a separate `? EXPLAIN` action is provided:

```
$ set-regex match=u\d+ replacement=UID

? EXPLAIN SELECT * FROM t1 AS a1, t1 AS a2 WHERE a1.f1 IS NOT NULL;
%0 =
| Get materialize.public.t1 (UID)
| Filter !(isnull(#0))
| ArrangeBy ()
...
```

As the `EXPLAIN` output contains parts that change between invocations, a `$ set-regex` is required to produce a stable test.

# Using Variables

> :warning: **all testdrive variables are initialized to their ultimate value at the start of the test**
>
> The place where the variable is declared within the script does not matter.
>
> :warning: **testdrive variables are read-only**

## Defining variables

#### `$ set name=value`

Sets the variable to a value

```
$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"string"}
        ]
    }
```

#### `$ set-from-sql var=NAME`

Sets the variable to the result of a SQL query that returns one row containing
one string column:

```
$ set-from-sql var=environment-id
SELECT mz_environment_id()
```

## Referencing variables

#### `${variable_name}`

A variable can be referenced in both the arguments and the body of a command.

```
$ kafka-ingest format=avro topic=kafka-ingest-repeat schema=${schema} repeat=2
{"f1": "${schema}"}
```

For example, this test will pass:

```
> SELECT ${testdrive.seed};
"${testdrive.seed}"
```

#### `$ set name`

Sets the variable to the possibly multi-line value that follows.

```
$ set schema
syntax = "proto3";

message SimpleId {
  string id = 1;
}
```

## Variable reference

The following variables are defined for every test. Many correspond to the command-line options passed to `testdrive`:

#### `testdrive.kafka-addr`
#### `testdrive.kafka-addr-resolved`

#### `testdrive.schema-registry-url`

#### `testdrive.seed`

The random seed currently in effect. If no ```--seed``` option was specified, it will be a random seed generated by `testdrive`.

#### `testdrive.temp-dir`

The temporary directory to use. If no ```--temp-dir``` option is specified, it will be a random directory inside the OS temp directory.

#### `testdrive.aws-region`

#### `testdrive.aws-account`

#### `testdrive.aws-access-key-id`

#### `testdrive.aws-secret-access-key`

#### `testdrive.aws-token`

#### `"stdrive.aws-endpoint`

#### `testdrive.materialize-internal-http-addr`

#### `testdrive.materialize-http-addr`

#### `testdrive.materialize-sql-addr`

#### `testdrive.materialize-internal-sql-addr`

#### `testdrive.materialize-user`

## Accessing the environment variables

The environment variables are available uppercase, with an the `env.` prefix:

```
> SELECT ${env.HOME}
"{$env.HOME}"
```

# Dealing with variable output

Certain tests produce variable output and testdrive provides a way to mask that via regular expression substitution.

#### `$ set-regex match=regular_expression replacement=replacement_str`

All matches of the regular expression in the rest of the test will be converted to ```replacement_str```.

For example, this will convert all UNIX-like timestamps to the fixed string `<TIMESTAMP>`and this test will pass:

```
$ set-regex match=\d{10} replacement=<TIMESTAMP>
> SELECT round(extract(epoch FROM now()));
<TIMESTAMP>
```

Note that only one regex can be active at a time: additional calls to `set-regex` will unset the previous regex. If you want to unset the regex without setting a new one, `unset-regex` will do that for you.

## Useful regular expressions

#### `$ set-regex match=(\s\(u\d+\)|\n|materialize\.public\.) replacement=`

This does the following three things that are useful for tests containing ```EXPLAIN``` or ```SHOW``` commands:
- removes all u#### identifiers from the output
- removes all mentions of `materialize.public`, to make the output more concise
- removes all newlines, to flatten the EXPLAIN output on a single line

# Script Actions

## Connecting to databases over the pgwire protocol

In addition to the default connection that is used for the `>` and `!` statements, one can establish additional connections to pgwire-compatible servers, that is, to Postgres and to Materialize itself. This allows for some simple concurrency testing where a transaction can be left open across multiple statements.

#### `$ postgres-execute connection=...`

Executes a list of statements over the specified connection. The `connection` can be specified as either an URL or as a named connection created by `$ postgres-connect`:

```
$ postgres-execute connection=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
CREATE TABLE postgres_execute (f1 INTEGER);
```

If any of the statements fail, the entire test will fail. Any result sets returned from the server are ignored and not checked for correctness.

#### `$ postgres-connect name=.... url=...`

Creates a named psql connection that can be used by multiple `$ postgres-execute` statements

```
$ postgres-connect name=conn1 url=postgres://materialize:materialize@${testdrive.materialize-sql-addr}

$ postgres-execute connection=conn1
BEGIN;
INSERT INTO postgres_connect VALUES (1);

...

$ postgres-execute connection=conn1
INSERT INTO postgres_connect VALUES (2);
COMMIT;
```

Note that when using multiple connections, commands are already executed in a
single-threaded context, so will be serialized in the order they appear in the
`.td` file.

#### `$ postgres-verify-slot connection=... slot=... active=(true|false)`

Pauses the test until the desired Postgres replication slot (identified by
`LIKE` pattern) has become active or inactive on the Postgres side of the direct
Postgres replication. This is used to prevent flakiness in Postgres-related
tests.


## Connecting to MySQL

#### `$ mysql-connect name=... url=... password=...`

Creates a named connection to MySQL. For example:

```
$ mysql-connect name=mysql_connection url=mysql://mysql_user@mysql_host password=1234

```

##### `$ mysql-execute name=...`

Executes SQL queries over the specified named connection to MySQL. The ouput of the queries is not validated, but an error will cause the test to fail.

## Connecting to Microsoft SQL Server

#### `$ sql-server-connect name=...`

Creates a named connection to SQL Server. The parameters of the connection are specified in ADO syntax. As the ADO string can contain spaces, it needs to be provided in the body of the action:

```
$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=sa;Password=${arg.sa-password}
```

#### `$ sql-server-execute name=...`

Executes SQL queries over the specified named connection to SQL server. If transactions are used, each transaction must be fully self-contained within a single line:

```
$ sql-server-execute name=sql-server
USE test;
BEGIN TRANSACTION INSERT INTO t1 VALUES (1); INSERT INTO t2 VALUES (2); COMMIT;
```

The output of the queries is not validated in any way. An error during execution will cause the test to fail.

## Sleeping in the test

#### `$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=N`

Sleeps for `N` seconds

#### `$ random-sleep duration=N`

Sleeps a random interval between 0 and N seconds

#### `> select mz_internal.mz_sleep(N)`

Instructs Mz to sleep for the specified number of seconds. `mz_sleep()` returns `<null>`, so the test needs to be coded accordingly:

```
> SELECT mz_internal.mz_sleep(1)
<null>
```

## Controlling timeouts and retries

#### `$ set-sql-timeout duration=N(ms|s|m) [force=true]`

Adjusts the SQL timeout for tests that contain queries that may take a long time to run. Alternatively, the `--default-timeout` setting can be passed to `testdrive`.

The command will be ignored if it sets a timeout that is smaller than the
default timeout, unless you specify `force=true`. Use this override with
caution! It may cause the test to fail in test environments that introduce
overhead and need a larger `--default-timeout` to succeed.

#### `$ set-max-tries max-tries=N`

Adjust the number of tries testdrive will perform while waiting for a SQL statement to arrive to the desired result.

Set `max-tries` to `1` in order to ensure that statements are executed only once. If the desired result is not achieved on the first try, the test will fail. This is
useful when testing operations that should return the right result immediately rather than eventually.

## `TEST SCRIPT` sources
`TEST SCRIPT` sources can be a useful to have a source that emits data in specific pattern,
without setting up data in a local source. They are created as follows:

```
> CREATE SOURCE unit
  FROM TEST SCRIPT
  '[
    {"command": "emit", "key": "fish", "value": "value", "offset": 0},
    {"command": "emit", "key": "fish2", "value": "hmm", "offset": 1},
    {"command": "emit" ,"key": "fish", "value": "value2", "offset": 2}
  ]'
  KEY FORMAT BYTES
  VALUE FORMAT BYTES
  ENVELOPE UPSERT
```

Each "command" can be:
- `"emit"`: emit data at a specific offset. The `"key"` is optional, but required
for some envelopes, like `UPSERT`
- `"terminate"`: terminate the source, ignoring all later commands. This closes the
source's `upper`
  - The default behavior if there is no `"terminate"` command is for the source
  to pend forever, after it processes all other commands.

Note that this soure has some limitations:
- It does not work with formats like `avro`
- It requires the key and value format to specified individually, as
it does not support CSR formats.

These limitations may be lifted in the future; additionally, more features may
be added to this source.

## Actions on local files

#### `$ file-append path=file.name [compression=gzip] [repeat=N]`

Writes the data provided to a file on disk. The file will be created inside the temporary directory of the test.

If the `repeat` parameter is provided, the data provided will be appended to the
file the specified number of times, rather than just once.

#### `$ file-delete path=file.name`

Deletes the specified file from within the temporary directory.

## Actions on Kafka topics

#### `$ kafka-add-partitions topic=... total-partitions=N`

Add partitions to an existing topic

#### `$ kafka-create-topic`

Create a Kafka topic

#### `$ kafka-delete-topic-flaky`

Delete a Kafka topic

Even though `kafka-delete-topic-flaky` ensures that the topic no longer exists
in the broker metadata there is still work to be done asychnronously before
it's truly gone that must complete before we attempt to recreate it. There is
no way to observe this work completing so the only option left is sleeping for
a while after executing this command.

For this reason this command must be used with great care or not at all,
otherwise there is a risk of introducing flakiness in CI.

#### `$ kafka-ingest topic=... schema=... ...`

Sends the data provided to a kafka topic. This action has many arguments:

##### `topic`

The topic to publish the data to.

##### `format=(avro|bytes)`

The format in which the data is provided

##### `key-format=(avro|bytes)`

For data that also includes a key, the format of the key

##### `schema`

The schema to use

##### `key-schema`

For data that contains a key, the schema of the key

##### `key-terminator=str`

For data provided as `format=bytes key-format=bytes`, the separator between the key and the data in the test

##### `repeat=N`

Send the same data `N` times to Kafka. This is used to create longer Kafka topics without bloating the test.

The variable `${kafka-ingest.iteration}` will hold the current iteration and can be used in the body of `$ kafka-ingest`.

##### `start-iteration=N`

Set the starting value of the `${kafka-ingest.iteration}` variable.

#### `partition=N`

Send the data to the specified partition.

### set-schema-id-var=VAR

Sets the variable named VAR to the ID of the schema with which data was written.
The variable is only set if the format of the key was Confluent Avro or
Protobuf.

### set-key-schema-id-var=VAR

Sets the variable named VAR to the ID of the key schema with which data was
written. The variable is only set if the format of the key was Confluent Avro
or Protobuf.

#### `kafka-verify-data format=avro [sink=... | topic=...] [sort-messages=true] [partial-search=usize]`

Obtains the data from the specified `sink` or `topic` and compares it to the expected data recorded in the test.

The comparison algorithm is sensitive to the order in which data arrives, so `sort-messages=true` can be used
along with manually pre-sorting the expected data in the test. The rows in the expected data section need to
be ordered according to the following rules:
 - earlier timestamps sort first
 - deletes sort before inserts
 - as all items are sorted as strings, "10" sorts before "2"

It is possible to call `$ kafka-verify` multiple times on the same topic in case the test needs to check
that the data arrives in some partial order. For example, to make sure that all of timestamp `1` arrived
before all of timestamp `2`:

```
$ kafka-verify ... sort-messages=true
1 {"a":"1"}
1 {"b":"1"}

$ kafka-verify ... sort-messages=true
2 {"c":"1"}
2 {"d":"1"}
```

If `partial-search=usize` is specified, up to `partial-search` records will be read from the given topic and
compared to the provided records. The records do not have to match starting at the beginning of the sink but
once one record matches, the following must all match.  There are permitted to be records remaining in the
topic after the matching is complete.  Note that if the topic is not required to have `partial-search`
elements in it but there will be an attempt to read up to this number with a blocking read.

#### `kafka-verify-commit consumer-group-id=... topic=... partition=...

Verifies that the provided offset (the input data) matches the committed offset
for the specified consumer group, topic, and partition.

#### `headers=<list or object>`

`headers` is a parameter that takes a json map (or list of maps) with string key-value pairs
sent as headers for every message for the given action.

## Actions on Confluent Schema Registry

#### `$ schema-registry-publish subject=... schema-type=<avro|json|protobuf> [references=subject[,subject...]]`

Publish a schema to the schema registry.

The command's input is used as the body of the schema.

The required `subject` argument specifies the name under which to register the
schema. If registering a schema for a Kafka topic using the topic name strategy
(the usual case), use the subject `$TOPIC-key` or `$TOPIC-value`, depending on
whether the schema is for the key type or the value type.

The required `schema-type` argument indicates the type of the schema.

The optional `references` argument specifies a comma-separated list of existing
schema subjects upon which the schema depends. The action always declares the
reference on the *latest* version of the referenced subject. If this is
limiting, feel free to adjust the action to permit specifying the desired
version, instead of assuming the latest version.

#### `$ schema-registry-verify subject=... schema-type=avro`

Verify the contents of the latest version of a schema in the schema registry.

The command's input is used as the expected schema.

The required `subject` argument specifies the name of the schema in the registry
to validate. The latest version of the subject is always used. If this is
limiting, feel free to adjust the action to specify the desired version. If
verifying a schema for a Kafka topic using the topic name strategy (the usual
case), use the subject `$TOPIC-key` or `$TOPIC-value`, depending on whether
you wish to verify the key or the value schema.

The required `schema-type` argument indicates the type of the schema. At
present, only Avro schemas are supported. Feel free to adjust the action to
support additional schema types.

#### `$ schema-registry-wait subject=...`

Block the test until schema with the specified subject has been defined at the
schema registry.

This action is useful to fortify tests that expect an external party, e.g.
Debezium, to upload a particular schema.

## Actions on REST services

#### `$ http-request method=(GET|POST|PUT) url=... content-type=...`

Issue a HTTP request against a third-party server. The body of the command is used as a body of the request. This is generally used when communicating with REST services such as Debezium and Toxiproxy. See `test/debezium-avro/debezium-postgres.td.initialize` and `test/pg-cdc-resumption/configure-toxiproxy.td`

```
$ http-request method=POST url=http://example/com content-type=application/json
{
  "f1": "f2"
}
```

The test will fail unless the HTTP status code of the response is in the 200 range.

## Actions with `psql`

#### `$ psql-execute command=...`

Executes a command against Materialize via `psql`. This is intended for testing
`psql`-specific commands like `\dn`.

## Conditionally skipping the rest of a `.td` file

```
$ skip-if
SELECT true
```

`skip-if` followed by a SQL statement will skip the rest of the `.td` file if
the statement returns one row containing one column with the value `true`. If
the statement returns `false`, then execution of the script proceeds normally.
If the query returns anything but a single row with a single column containing a
`boolean` value, testdrive will mark the script as failed and proceed with
execution of the next script.

This action can be useful to conditionally test if a script is running against
a version of Materialize that is able to execute the SQL constructs contained therein.

```
$ skip-if
SELECT mz_version_num() < 2601;
```

## Run an action/query conditionally on version

```
>[version>=5500] SELECT 1;
1
```

The `[version>=5500]` property allows running the action or query only when we are connected to a Materialize instance with a compatible version. The supported comparison operators are `>`, `>=`, `=`, `<=` and `<`. The version number is the same as returned from [`mz_version_num()`](https://materialize.com/docs/sql/functions/#system-information-func) and has the same format `XXYYYZZ`, where `XX` is the major version, `YYY` is the minor version and `ZZ` is the patch version. So in the example we are only running the `SELECT 1` query if the Materialize instance is of version `v0.55.0` or higher. For lower versions no query is run and no comparison of results is performed subsequently.

[confluent-arm]: https://github.com/confluentinc/common-docker/issues/117#issuecomment-948789717
[aws-creds]: https://github.com/MaterializeInc/i2/blob/main/doc/aws-access.md
