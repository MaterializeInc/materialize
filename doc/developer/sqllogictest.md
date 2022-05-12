# sqllogictest Extended Guide

This page assumes that you have already read and understood [the basics of
testing with Materialize][testing guide] and [SQLite sqllogictest
documentation][upstream docs].

[testing guide]: /doc/developer/guide-testing.md
[upstream docs]: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

## Usage guide

### Test writing guidelines

If you have made a new function, be sure to test both `select func(NULL)` and
`select func(col) from atable` where `col` is a column that contains `NULL`. The
null in `select func(NULL)` is treated as type `` whereas the null in `col` is
treated as the same type as `col`. If the function has more than one argument,
be sure to test with `NULL` and a column that contains `NULL` for each argument.

If you have made a new function with numeric arguments, make sure that you have
tested what happens if each argument is 0. Other important numbers to remember
testing are negative numbers, the number 1 (for certain tests), and +/-
infinity.

### Type coercion

Do not use expected data types to verify the data type of resulting columns
because sqllogictest will try its best to coerce the results to the type
expected. Instead, use `SHOW COLUMNS...`.

While we support coercing boolean results to T(ext), B(ool), and I(nt), we
recommend using T for new tests involving selecting on booleans.

### Negative testing

You can test that a statement or query will fail (with optionally a particular
error message) like this:

```
statement error <optional expected error message here>
<your statement here>
```

or this:

```
query error <optional expected error message here>
<your query here>
```

`query error` will only run the statement if it is `SELECT`, `CREATE VIEW`, and
`SHOW INDEXES` statements. Otherwise, it will just skip running the statement
and declare the test as passing.

`statement error` will run any type of statement.

### Primary keys

While you can't create tables with primary keys in Materialize yet, you
can create tables with primary keys in sqllogictest, and the optimizer will take
the primary keys into account.

The syntax is either:
`CREATE TABLE foo (a INT PRIMARY KEY, b INT)`
or
```
CREATE TABLE bar (
    a smallint,
    b integer,
    c char(10),
    d char(20),
    PRIMARY KEY (a, b)
)
```

### Useful tips

* If you use Visual Studio Code, @benesch has made a syntax highlighter for
  sqllogictest, which you can find
  [here](https://marketplace.visualstudio.com/items?itemName=benesch.sqllogictest).
* If you run `cargo run --bin sqllogictest -- <path_to_test_file>
  --rewrite-results`, sqllogictest will automatically replace the expected
  results with the actual results. The main time you use this argument is if you
  have made an improvement to the query planner and need to update the expected
  plan for every `EXPLAIN PLAN` query.
* If you want to debug a sqllogictest run via debugger, use `rust-lldb --
  target/debug/sqllogictest <path_to_test_file_to_run>`. Do not use `rust-gdb`
  to debug sqllogictest on MacOS because gdb will print a bunch of error
  messages and then freeze.

## Materialize-specific behavior in sqllogictest

### `simple` extension

In addition to `statement` and `query`, we have added our own directive
`simple`. It is followed by multiple lines until `----` and executes
these lines as a simple query over pgwire. This is needed because the
other directives use the extended protocol, and we needed a way to test
multi-statement queries and implicit transactions.

An optional `conn=<name>` can be added to execute the message on a
named connection. It will be created on first use. This can be used to
test transaction isolation or otherwise do things that require multiple
connections.

The output is one line per row, one "COMPLETE X" (where X is the
number of affected rows) per statement, or an error message.

### `copy` extension

The `copy` directive executes a [`COPY FROM`](https://materialize.com/docs/sql/copy-from/)
statement. For example, the following directive

```
copy table path/to/file.csv
```

pipes the contents of file.csv to the following SQL statement:

```
COPY table FROM STDIN
```

### modes

We have extended sqllogictest to have the concept of the "mode." There are two
extant modes:
* `mode standard` (default): Supports running tests written by SQLite.
* `mode cockroach`: Supports running tests written by CockroachDB.

In our Materialize-specific sqllogictests, we use both modes, sometimes in the
same file. Most of the time, we use `mode cockroach`. The times when we use
`mode standard` are:
* `EXPLAIN PLAN` queries.
* When we are querying a single row with many columns.

#### Differences between modes

1. In `mode standard`, spaces in the query result are treated as column
   delimiters only when it comes to column names. In `mode cockroach`, spaces in
   the query result are treated as column delimiters except when the expected
   result has only one column. Thus, if you are querying multiple columns of a
   table,
    * the result looks like this in `mode standard`:
        ```
        query TT colnames
        select col1 col2 from atable
        ----
        col1 col2
        row1col1
        row1col2
        row2col1
        row2col2
        ```
    * and in `mode cockroach`:
        ```
        query TT colnames
        select col1 col2 from atable
        ----
        col1 col2
        row1col1 row1col2
        row2col1 row2col2
        ```

    In `mode cockroach`, if your expected column name or row entry contains a
    space, to have the space not treated as a column delimiter, replace the
    space with the symbol ␠, which is U+2420, the Unicode "SYMBOL FOR SPACE".
    Example:
    ```
    query TT colnames
    select "col 1" "col 2" from atable
    ----
    col␠1 col␠2
    row1␠col1 row1␠col2
    row2␠col1 row2␠col2
    ```

    As mentioned earlier, in `mode cockroach`, replacing the space with the
    symbol ␠ is optional when there is only one column of expected output. This
    will work fine:
    ```
    query T colnames
    select "col 1" from atable
    ----
    col 1
    row1 col1
    row2 col1
    ```

    In `mode standard`, it does not seem to possible to indicate that the
    expected column name contains a space even if only one column of output is
    expected.

2. In `mode standard`, a Decimal in the query result is treated as having a
   scale of 4 if its actual scale is 0 and as having a scale of 3 otherwise. A
   floating point number is always treated as having a scale of 3.
    ```
    mode standard

    query R
    SELECT 384::decimal(38, 5)
    ----
    384.000

    query R
    SELECT 1::decimal(38, 0)
    ----
    1.0000

    query R
    SELECT 2::float
    ----
    2.000

    query R
    SELECT 3.143892789739843::float
    ----
    3.144

    query R
    SELECT 314.3820897445435::float
    ----
    314.382
    ```

    `mode cockroach`, on the other hand, respects the scale of a Decimal type
    and the precision of a floating point number.

    ```
    mode cockroach

    query R
    SELECT 384::decimal(38, 5)
    ----
    384.00000

    query R
    SELECT 1::decimal(38, 0)
    ----
    1

    query R
    SELECT 2::float
    ----
    2

    query R
    SELECT 3.143892789739843::float
    ----
    3.143892789739843

    query R
    SELECT 314.3820897445435::float
    ----
    314.3820897445435
    ```

### multiline

As you can see [above](#differences-between-modes), generally sqllogictest
treats newlines as column or row delimiters. We have extended sqllogictest to
have some primitive support for text output that contains newlines with the
`multiline` keyword. If you add the `multiline` keyword, all text between `----` and `EOF` is treated as one entry with newlines. Here's an example.

```
query T multiline
select 'hello
 world'
----
hello
 world
EOF
```

The sqllogictest binary will panic if you specify any other options together
with `multiline`.

### Commands we do not support

Our sqllogictest will skip running the following commands and return an Ok:
* `CREATE UNIQUE INDEX ...`
* `REINDEX ...`

These commands are present in the SQLite tests, but we do not support them yet,
and we don't want failures associated with these commands to show up on our
dashboard yet.

Our sqllogictest will interpret the CockroachDB extension sort-mode
`partialsort(...)` as rowsort instead. Do not add new tests involving
`partialsort` until `partialsort` is fixed.

### Internals

It may be necessary to make modifications to our sqllogictest-related code in
order to support and test changes to our API, such as:
* new SQL commands
* new supported data types

You can look for sqllogictest-related code in the following directories:
* [src/sqllogictest](/src/sqllogictest): Contains code for the sqllogictest
  binary. Look here if you want to change things like how sqllogictest passes
  queries to Materialize and how it handles results. Note that generally, you
  are not allowed to:
    * Change how results are formatted because it would break our support for
      tests not written by us.
    * Create new expected result types. When new data types are added to
      Materialize, we prefer sqllogictest to coerce the new data types to one of
      the existing expected result types. Observe how in
      [test/sqllogictest/dates-times.slt](/test/sqllogictest/dates-times.slt),
      we are selecting date and time types, but the expected result type is T
      (text).
* [src/coord](/src/coord): This is the starting point for the code that handles
  processing the changelog received from PostgreSQL. Search the directory for
  `Plan::CreateTable` and `Plan::SendDiffs`.
