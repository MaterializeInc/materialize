# Coordinator Interface

This document describes the external interface that the coordinator
presents to the protocol layers (pgwire and HTTP). In general, the
concepts exposed by the coordinator API are close to native
pgwire concepts (prepare, bind, execute, and so on), but there is not
a 1-to-1 correspondence, and so this document is not simply a
restatement of the [Postgres protocol documentation][pgwire].

## The Adapter Client

During system startup, all users of the coordinator are instantiated
with an _[adapter client]_, in some documentation also referred to as
a _coordinator client_. This is a reference-counted handle to the
coordinator, with the capability to send messages to the coordinator
instructing it to perform various tasks.

## Sessions

Approximately the only useful actions that an adapter client allows
one to take (via, behind the scenes, sending messages to the
coordinator) are [creating][create session] and [starting][start
session] sessions. A _session_ is an abstraction of a network
connection that is authenticated (that is, always associated with a
valid user) and further associated with various metadata such as the
values of various session variables and the set of prepared statements
and executing cursors.

The state associated with a session is partly stored in the
[`Session`][session] struct, and partly in a map maintained by the
coordinator and keyed on the session's connection ID.

## Basic Concepts

A _statement_ is the most general term for what is sometimes called a
query or a command. In SQL source text, statements are typically
delimited by semicolons. That said, the coordinator does not directly
accept SQL source text, but rather deals with the
[`Statement`][statement] AST. Statements may have _parameters_. These
are unspecified values that appear in places where an actual value is
expected.

Typically, before a statement may be executed, it must be _prepared_;
preparation does some planning and type-checking and binds the
statement to a name.

The next step after preparation is binding the statement to a
_portal_, also called a _cursor_. It is at this stage that the values
for all parameters must be specified. The lifecycles and names of
portals and the prepared statements that are bound to them are not
strongly connected: one prepared statement may be bound to many
different portals, and may be deallocated or reassigned while the
portals still exist, without affecting them.

As a convenience, preparation and binding may be combined in one step,
using an interface function called `declare`. This prepares a
statement (whose parameter values must all be speciified up front) and
binds it to a portal in one step, without naming the prepared
statement.

Once a prepared statement has been bound to a portal, it may be
_executed_. Execution may cause the catalog to be mutated in various
ways, and may return a result set. Describe the full set of effects
and return values of statements is outside the scope of this document;
the interested reader should consult the general Materialize SQL
documentation.

Note that these concepts are not only exposed by the controller
interface, but also at a higher level, to the end-user, in SQL. For
example, the user may bind the statement `SELECT * FROM t` to the
portal `c` by executing the SQL `DECLARE c CURSOR for SELECT * FROM
t`, and may later execute this portal using the SQL `FETCH FORWARD ALL
FROM c`.  Alternatively, the user may make that statement into a
prepared statement named `ps` by executing `PREPARE ps AS SELECT *
FROM t`, and later execute it with `EXECUTE ps`.  It's important to
note that these statements are distinct from the ones they reference;
for example, the statement `DECLARE c CURSOR for SELECT * FROM t` must
itself be prepared, bound to a portal (distinct from the portal `c`),
and executed.

## Communication Flow

Once a session has been established, the owner of the adapter client
gains access to the much richer interface of the [session
client]. Most functions of this interface are implemented according to
the same pattern: they send a command to the coordinator and wait for
a response.

We avoid the need to design intricate locking protocols by simply
requiring that the coordinator have exclusive access to the session
object during the entire execution of most commands. Therefore, as
part of the common communication pattern for most commands mentioned
above, the entire session object is sent to the coordinator, and then
sent back. The session client then simply crashes if any of these
functions are called while it does not have possession of the session
object.

In the upcoming sections, we will describe the various interface
functions in more detail.

## `prepare`

The [`SessionClient::prepare`][prepare] function corresponds to the
`Command::Prepare` coordinator command. Its purpose is to bind a
statement (possibly with parameters) to a name. It is called with a
description of a parsed parameterized SQL statement, a name to bind it
to, and a description of the types of its parameters (for those that
are known). The coordinator plans the statement in order to determine
the types of all parameters and the output relation (if any), and
associates this information with the given name in its per-session
state.

Using the empty string for the name may be used to specify the default
prepared statement, matching postgres protocol semantics.

When successful, this function returns no value.

## `set_portal`

The [`Session::set_portal`][setportal] function does not correspond to
any coordinator command and, in fact, does not require communication
with the coordinator at all -- note that it is a function on the
`Session` object itself, rather than on either of the coordinator
clients. Its most important parameters are a portal name, a statement
AST and its description (discovered during preparation), and a list of
values for the parameters of the statement. The session assigns the
given statement to a portal of the given name.

When successful, this function returns no value.

## `declare`

The [`SessionClient::declare`][declare] function corresponds to the
`Command::Declare` coordinator command. It behaves essentially like a
combination of `SessionClient::prepare` and `Session::set_portal`, preparing a statement and
binding it to a portal for execution, except that it does not cause
the statement to be bound to a prepared statement name (only to a
portal name).

When successful, this function returns no value.

## `execute`

The [`SessionClient::execute`][execute] function corresponds to the
`Command::Execute` coordinator command. Its purpose is to begin
execution of a portal to which a statement and set of parameters has
previously been bound. It is called with the name of the portal and a
wire called `cancel_future` on which the coordinator can listen for
requests to cancel the execution.

When successful, this function returns an `ExecuteResponse`, whose
meaning varies depending on the kind of statement being executed. In
some cases it means that the statement's execution has finished; in
others, it only means that it has _begun_, and the client must either
wait for results, or take further actions to drive it to
completion. The specific protocols for various types of statements are
described in more detail below.

### `SELECT` queries

The coordinator responds with an instance of
[`ExecuteResponse::SendingRows`][sendingrows], which contains a future
that will ultimately resolve to a batch of rows, an error, or a
cancelation signal. If the query is successful, all rows it returns
will be available as the output of the future as soon as it resolves
(i.e., this is not a streaming API).

In general, queries may take arbitrarily long to finish, including
possibly never terminating. If the client is no longer interested in
the results of a query, for example because the end user disconnected
or requested a cancellation, then the client is expected to
communicate that fact to the coordinator by causing the
`cancel_future` wire discussed above to activate.

### Subscriptions

The coordinator responds to a bare `SUBSCRIBE` statement (that is, one
that is not part of a `COPY ... TO STDOUT` statement) with the an instance of
[`ExecuteResponse::Subscribing`][subscribing]. This response contains
a channel on which batches of rows are received. The client may
continue drawing from this channel until either it closes gracefully
(indicating that the subscribe has finished, as can happen for
constant relations), or an error or cancellation is indicated.

As is the case for [`SELECT` queries](#select-queries), if the client is no longer
interested in further responses, it should indicate that fact by
actuating the corresponding `cancel_future`.

### `COPY ... TO STDOUT`

The coordinator responds to such a statement with an instance of
[`ExecuteResponse::CopyTo`][copyto], which simply wraps an instance of
the `ExecuteResponse` for the inner [subscription](#subscriptions) or
[`SELECT` query](#select-queries).

### `COPY ... FROM STDIN`

The coordinator responds with an instance of
[`ExecuteResponse::CopyFrom`][copyfrom] containing some metadata about the
expected input format. The execution of the statement has by no means
finished at this point; this `ExecuteResponse` is merely an
instruction to the client to collect some data from the end user,
decode it into rows, and pass them to the coordinator in the
`SessionClient::insert_rows` method (which corresponds to a
[`Command::CopyRows`][commandcopyrows] coordinator command). After the
coordinator receives this second command, the statement execution is
considered finished.

### `FETCH`

The coordinator responds with an instance of
[`ExecuteResponse::Fetch`][fetch], which instructs the session to
execute a cursor with a given name (i.e., the cursor specified
textually in the `FETCH` statement being executed). Thus, a total of
two calls to `execute` should happen as part of this flow (an "outer"
one to `execute` the `FETCH` statement, and an "inner" one `execute`
the cursor referenced in the `FETCH` statement). The entire statement
execution is considered to have finished once this "inner" execute
finishes.

### All others

Unless described differently elsewhere, the typical statement
terminates in the coordinator, and no further action by the client is
necessary. To give one arbitrary example among many, if a `CREATE
SOURCE` statement is successful, the coordinator responds with an
instace of [`ExecuteResponse::CreatedSource`][createdsource], which
carries no additional information and requires no response.

[adapter client]: https://github.com/MaterializeInc/materialize/blob/0495d6272f/src/adapter/src/client.rs#L85-L92
[create session]: https://github.com/MaterializeInc/materialize/blob/0495d6272f/src/adapter/src/client.rs#L115-L123
[start session]: https://github.com/MaterializeInc/materialize/blob/0495d6272f/src/adapter/src/client.rs#L125-L170
[session]: https://github.com/MaterializeInc/materialize/blob/0495d6272f/src/adapter/src/session.rs#L57-L87
[session client]: https://github.com/MaterializeInc/materialize/blob/0495d6272f/src/adapter/src/client.rs#L225-L239
[prepare]: https://github.com/MaterializeInc/materialize/blob/0495d6272f/src/adapter/src/client.rs#L281-L299
[declare]: https://github.com/MaterializeInc/materialize/blob/0495d6272f/src/adapter/src/client.rs#L301-L317
[execute]: https://github.com/MaterializeInc/materialize/blob/0495d6272f/src/adapter/src/client.rs#L319-L336
[sendingrows]: https://github.com/MaterializeInc/materialize/blob/0495d6272f/src/adapter/src/command.rs#L370-L375
[subscribing]: https://github.com/MaterializeInc/materialize/blob/0495d6272f/src/adapter/src/command.rs#L386-L386
[copyto]: https://github.com/MaterializeInc/materialize/blob/0495d6272f/src/adapter/src/command.rs#L286-L289
[copyfrom]: https://github.com/MaterializeInc/materialize/blob/0495d6272f/src/adapter/src/command.rs#L290-L295
[commandcopyrows]: https://github.com/MaterializeInc/materialize/blob/0495d6272f/src/adapter/src/command.rs#L95-L102
[fetch]: https://github.com/MaterializeInc/materialize/blob/0495d6272f/src/adapter/src/command.rs#L344-L352
[createdsource]: https://github.com/MaterializeInc/materialize/blob/0495d6272f/src/adapter/src/command.rs#L315-L315
[statement]: https://github.com/MaterializeInc/materialize/blob/0495d6272f/src/sql-parser/src/ast/defs/statement.rs#L42-L100
[setportal]: https://github.com/MaterializeInc/materialize/blob/0495d6272f/src/adapter/src/session.rs#L526-L563
[pgwire]: https://www.postgresql.org/docs/15/protocol-flow.html
