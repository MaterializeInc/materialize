# Life of a Query

_Inspired by [CRDB: Life of a SQL
Query](https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/life_of_a_query.md)._

## Introduction

This document aims to provide an overview of the components involved in
processing a SQL query, following the code paths through the various layers:
network protocol, session management, the adapter, query parsing, planning, and
optimization, interaction of the adapter layer and clusters, the storage layer,
and delivery of results from clusters, back through the adapter layer to the
client.

We don't discuss design decisions and historical evolution but focus on the
current code. We also only cover the common and most significant parts and omit
discussing details and special cases.

## PostgreSQL Client Protocol

Queries arrive at Materialize through the [PostgreSQL wire
protocol](https://www.postgresql.org/docs/current/protocol.html).

The protocol is implemented in the [pgwire
crate](https://github.com/MaterializeInc/materialize/tree/bea88fb10b02792bad73afd79a6350b381a7dcc3/src/pgwire).
Incoming connections are handled in
[handle_connection](https://github.com/MaterializeInc/materialize/blob/bea88fb10b02792bad73afd79a6350b381a7dcc3/src/pgwire/src/server.rs#L105),
which passes on to
[run](https://github.com/MaterializeInc/materialize/blob/b3475aaaf96f9fae4a0d68cbb5202c224d9ce15b/src/pgwire/src/protocol.rs#L127),
and ultimately we create a
[StateMachine](https://github.com/MaterializeInc/materialize/blob/b3475aaaf96f9fae4a0d68cbb5202c224d9ce15b/src/pgwire/src/protocol.rs#L478)
whose
[run](https://github.com/MaterializeInc/materialize/blob/b3475aaaf96f9fae4a0d68cbb5202c224d9ce15b/src/pgwire/src/protocol.rs#L512)
handles the connection until completion.

The state machine has an adapter client
([SessionClient](https://github.com/MaterializeInc/materialize/blob/b3475aaaf96f9fae4a0d68cbb5202c224d9ce15b/src/adapter/src/client.rs#L481))
that it needs to use to accomplish most "interesting" things. It's a client to
the adapter layer, with its main component, the Coordinator. The work of
processing queries and maintaining session state is split between the "front
end" state machine that runs the pgwire protocol and the "back end" adapter
layer that handles talking to the other components.

> [!NOTE]
> We will keep using the terms adapter front end and adapter back end below to
> mean, respectively, the state machine that is responsible for a single
> connection and can do work concurrently with other connections, and the
> adapter/Coordinator which has to sequentialize work and is therefore more
> limited in the amount of work it can do concurrently.
>
> If it is clear from context, we will drop the adapter prefix.

## Adapter / Coordinator

The
[adapter](https://github.com/MaterializeInc/materialize/tree/f641a29d4aad9fcfeb2de535ff54706a1f1d38c4/src/adapter)
is named such because it translates user commands into commands that the other
internal systems understand. It currently understands SQL but is intended as a
generic component that isolates the other components from needing to know
details about the front-end commands (SQL).

A core component is the
[Coordinator](https://github.com/MaterializeInc/materialize/blob/9682eb11f31e2eb8005cb3eae687f81a1bce21bb/src/adapter/src/coord.rs#L1633).
It mediates access to durable environment state, kept in a durable catalog and
represented by the
[Catalog](https://github.com/MaterializeInc/materialize/blob/9682eb11f31e2eb8005cb3eae687f81a1bce21bb/src/adapter/src/catalog.rs#L137)
in memory, and to the controllers for the storage and compute components (more
on that later).

It holds on to mutable state and clients to other components. Access and
changes to these are mediated by a "single-threaded" event loop that listens to
internal and external command channels. Commands are worked of sequentially.
Other parts can put in commands, for example, the front end calling into the
adapter is implemented as sending a command which then eventually causes a
response to be sent back, but the Coordinator will also periodically put in
commands for itself.

> [!NOTE]
> There is active work in progress on changing this design because it doesn't
> lend itself well to scaling the amount of work that the adapter can do. We
> want to move more work "out to the edges", that is towards the front end
> (which can do work for different connections/sessions concurrently) and the
> controllers for the other components. See [Design Doc: A Small
> Coordinator](/doc/developer/design/20250717_a_small_coordinator_more_scalable_isolated_materialize.md)
> for details.

## Query Processing

Query processing follows these steps:

parsing & describing → logical planning → timestamp selection → optimization & physical planning → execution

### Parsing & Describing

The Materialize parser is a hand-written recursive-descent parser that we
forked from [sqlparser-rs](https://github.com/andygrove/sqlparser-rs). The main
entry point is
[parser.rs](https://github.com/MaterializeInc/materialize/blob/aba45afb39455b84cd64d63c3af50ffcae46fd83/src/sql-parser/src/parser.rs#L129).

Parsing happens completely in the front end, it produces an AST of
[Statement](https://github.com/MaterializeInc/materialize/blob/7fab0d0e12a15799334854dcb3990997bc72e037/src/sql-parser/src/ast/defs/statement.rs#L43)
nodes. The AST only represents the syntax of the query and says nothing about
how or if it can be executed.

Describing is the process of figuring out the result type of a statement. For
this, the front end needs access to the Catalog, for which it needs to call into
the adapter.

Both parsing and describing happen in the front end, with calls into the adapter
as needed. All further steps are orchestrated by the adapter/Coordinator: the
front end passes the AST as a command and will become involved again when
sending results back to the client.

### Logical Planning

Logical planning generates a
[Plan](https://github.com/MaterializeInc/materialize/blob/b3475aaaf96f9fae4a0d68cbb5202c224d9ce15b/src/sql/src/plan.rs#L133)
from the AST. Glossing over some details, this binds referenced names based on
the Catalog as of planning time and determines a logical execution plan. The
entrypoint is
[plan](https://github.com/MaterializeInc/materialize/blob/b3475aaaf96f9fae4a0d68cbb5202c224d9ce15b/src/sql/src/plan/statement.rs#L274).

One of the differences between the user (SQL) commands that are input to the
adapter and commands for the rest of the system is the use of user-defined and
reusable names in SQL statements rather than the use of immutable, non-reusable
IDs. The binding mentioned above turns those user-defined names into IDs.

As mentioned in the previous section, logical planning happens inside the
Coordinator.

### Timestamp Selection

Another difference between user commands and internal commands is the absence of explicit timestamps in user (SQL) commands, quoting from [formalism.md](/doc/developer/platform/formalism.md#adapter):

> A `SELECT` statement does not indicate *when* it should be run, or against
> which version of its input data. The Adapter layer introduces timestamps to
> these commands, in a way that provides the appearance of sequential
> execution.

The internal interface that we use for determining timestamps is
[TimestampOracle](https://github.com/MaterializeInc/materialize/blob/cbaed5e677317feabd97048595e529bf3770547e/src/timestamp-oracle/src/lib.rs#L41).
The production implementation is an oracle that uses CRDB as the source of
truth. See [Design Doc: Distributed Timestamp
Oracle](/doc/developer/design/20230921_distributed_ts_oracle.md) for more
context.

### Optimization & Physical Planning

There are multiple stages to optimization and different internal
representations, and different types of queries or created objects will
instantiate different optimizer pipelines. The optimization pipeline for SELECT
is [this
snippet](https://github.com/MaterializeInc/materialize/blob/bdf9573960cece48e7811db7ef777b973d355fe6/src/adapter/src/coord/sequencer/inner/peek.rs#L568).

The final result of these stages will depend on the type of query we're
optimizing, but for certain types of SELECT and permanent objects it will
include a
[DataflowDescription](https://github.com/MaterializeInc/materialize/blob/53acc93eee1bbbf418fde681389aec0419db8954/src/compute-types/src/dataflows.rs#L40).
Which is a physical execution plan that can be given to the compute layer, to
execute on a cluster.

TODO: Expand this section on optimization, if/when needed.

### Execution

For SELECT, which is internally called PEEK, there are three different execution scenarios:

- Fast-Path Peek: there is an existing arrangement (think index) in the cluster
  that we're targeting for the query. We can read the result from memory with
  minimal massaging and return it to the client (through the adapter).
- Slow-Path Peek: there is no existing arrangement that we can query. We have to create a dataflow in the targeted cluster that will ultimately fill an arrangement that we can read the result out of.
- Persist Fast-Path Peek: there is no existing arrangement but the query has a shape that allows us to read a result right out of a storage collection (in persist)

The result of optimization will indicate which of these scenarios we're in, and
the adapter will now have to talk to the compute controller to implement
execution of the query.

Ultimately, for all of these scenarios the adapter will make a call into the
compute controller to read out a peek result. For slow-path peeks it will first
create the dataflow, but the functionality for reading our the result is the
same for fast path and slow path after that. The entrypoint for that is
[peek](https://github.com/MaterializeInc/materialize/blob/4a84902b5bed3bbd605d7d165fa6e0823c88c102/src/compute-client/src/controller.rs#L862).

The adapter will pass the sending endpoint of a channel to the compute
controller, for sending back the results, and then setup up an async task that
reads from that channel, massages results, and sends them out as another
stream. The receiving end of that second stream is what the adapter returns in
a
[ExecutionResponse::SendingRowsStreaming](https://github.com/MaterializeInc/materialize/blob/b3475aaaf96f9fae4a0d68cbb5202c224d9ce15b/src/adapter/src/command.rs#L362),
to the adapter front end, which handles sending results back out to the pgwire
client.

### Query Processing: A Flow Chart

TODO: Give a graphical representation of the stages that a query goes through,
likely as a chart that shows what components invoke what other components, in
the style of a lamport diagram.

### Query Processing: Network hops

TODO: Describe which of the steps involved in processing a query involve
network hops, talking to "external" systems, talking to the cluster. etc. Then
maybe also give a description of which steps are CPU heavy and which are not.

## Details

The above description of query processing has mentioned some names and concepts
that are involved in query processing that we didn't explain further. We now
explain those.

## Compute & Storage Controllers

Should maybe explain the compute and storage protocol, to really describe how
the commands flow to the cluster and how the responses come back.

## Arrangements

TODO: Write up something about arrangements, how it's the basis for sharing and ultimately the thing that can be queries from a cluster.

## Storage

TODO: Both storage and persist are mentioned above, so we should at least give
an overview.

## Persist

TODO: Both storage and persist are mentioned above, so we should at least give
an overview.

