# Serving layer

- Associated: (Insert list of associated epics, issues, or PRs)

<!--
The goal of a design document is to thoroughly discover problems and
examine potential solutions before moving into the delivery phase of
a project. In order to be ready to share, a design document must address
the questions in each of the following sections. Any additional content
is at the discretion of the author.

Note: Feel free to add or remove sections as needed. However, most design
docs should at least keep the suggested sections.
-->

## The Problem

Selects are slow.
In the best case, we can support around 5'000 queries per second.
The reason is that Materialize hasn't been optimized for fast selects, but rather for efficient incremental view maintenance.
We optimize each query, select its time stamp, send it to a replica, wait for compute to gather results, merge the results and send it back to the client.
Several steps along this path incur latency, and this design shows alternatives that have the potential to avoid the latency.

Specifically, the design outlines a system that should be able to handle 100s of 1000s requests per second, but it remains to be seen if this will turn out true.

<!--
What is the user problem we want to solve?

The answer to this question should link to at least one open GitHub
issue describing the problem.
-->

## Success Criteria

Materialize can serve queries at a rate of 100k per second.
More is better, but not required.

<!--
What does a solution to this problem need to accomplish in order to
be successful?

The criteria should help us verify that a proposed solution would solve
our problem without naming a specific solution. Instead, focus on the
outcomes we hope result from this work. Feel free to list both qualitative
and quantitative measurements.
-->

## Out of Scope

<!--
What does a solution to this problem not need to address in order to be
successful?

It's important to be clear about what parts of a problem we won't be solving
and why. This leads to crisper designs, and it aids in focusing the reviewer.
-->

## Solution Proposal

To increase the queries-per-seconds, we need to change the architecture of Materialize, as incremental improvements will not allow us to reach orders-of-magnitude better performance.
The main bottle necks are per-query work (optimization), serializing points in code (communication with clusters), and inefficient data structures for key look ups (arrangements).
We tackle all individually, although for some we have a menu of options at our disposal.

At the moment, the environmentd process optimizes queries, handles communication with the client and cluster replicas.
Cluster replicas maintain indexes as arrangements, and handle peeks to read information from arrangements,.
Clusters scale horizontally by adding more and bigger replicas, at the expense of causing higher tail latency.
Environmentd currently does not scale horizontally.

We could scale environmentd as a whole (which is a separate project), or extract behavior from environmentd to handle it elsewhere, which is the main idea we follow in this design.

### Serving layer considerations

A serving layer has to handle requests for data, at a system-determined time, and provide responses to clients at low latency and high throughput.
It must scale horizontally and be isolated from other serving layer replicas.

Choices:
 * Fast data structure to lookup values by key.
   We do not support scans or inequality comparisons.
 * Reading from persist as the only way to get data into a serving layer.
   We can allow map/filter/projects (without temporal filters).
 * Isolation levels must be respected, i.e., we need to maintain multiple timestamped values per key, and their diff.
 * We can offer different interfaces.
   To handle complexity, we might not want to rely on SQL, but rather offer a REST API.
   All interfaces need to pre-define the queries the serving layer can handle.
   We can offer an implicit API (observe frequent queries), or an explicit API (declare queries).

TODO:
 * What about a query like `SELECT :param`, which is not a keyed lookup (besides the fact it wouldn't make much sense!)
 * Is it sufficient to say that each parameter must appear in a equality constrain with other data, but can't be free.

### Proposal 1: Serving layer replicas

We introduce the notion of a serving cluster with serving cluster replicas.
Its task is to handle specific queries, and return results at low latency.

Supporting SQL prepared statements (or an equivalent)
 * Investigate lateral joins
 * If the placeholders are equality lookups, we can convert the query to a materialized view that arranges the data such that the placeholder columns are keys to the prepared statement.
 * An API endpoint is simpler, as it would define equality lookup on materialized views based on specific columns.

As we're not targeting arbitrary SQL queries, we do not need to touch the optimizer in surprising ways---the biggest change would be to optimize queries with placeholders into a predictable shape.

What we need:
 * Access to the timestamp oracle to determine appropriate timestamps for queries.
 * Do we need to handle transactions?
 * A parallel data structure mapping `(key, time) -> (value, diff)?`.
   The diff would always be positive.

### Considerations for parallel data structures

Whatever data structure we select, we must generate it from a stream of updates, and must be able to read it in constant or logarithmic time.
Hot keys should be faster to access than cold ones.

* Hash maps are hard to make concurrent and would require atomic operations per key.
  Depending on the workload, it could suffer from contention, for example when recently modified keys are hot.
* A LSM we maintain asynchronously in the background.
  Each layer maps keys to their full history within the time bounds of a read and write frontier.
* We can support caches scoped to a query or connection to maintain frequent hot keys.

### Sharding of the serving layer

For key point lookups, we can piggy-back on cluster replicas.
Each replica handles part of the key space, and a routing layer needs to route queries depending on the key's hash.



<!--
What is your preferred solution, and why have you chosen it over the
alternatives? Start this section with a brief, high-level summary.

This is your opportunity to clearly communicate your chosen design. For any
design document, the appropriate level of technical details depends both on
the target reviewers and the nature of the design that is being proposed.
A good rule of thumb is that you should strive for the minimum level of
detail that fully communicates the proposal to your reviewers. If you're
unsure, reach out to your manager for help.

Remember to document any dependencies that may need to break or change as a
result of this work.
-->

## Minimal Viable Prototype

<!--
Build and share the minimal viable version of your project to validate the
design, value, and user experience. Depending on the project, your prototype
might look like:

- A Figma wireframe, or fuller prototype
- SQL syntax that isn't actually attached to anything on the backend
- A hacky but working live demo of a solution running on your laptop or in a
  staging environment

The best prototypes will be validated by Materialize team members as well
as prospects and customers. If you want help getting your prototype in front
of external folks, reach out to the Product team in #product.

This step is crucial for de-risking the design as early as possible and a
prototype is required in most cases. In _some_ cases it can be beneficial to
get eyes on the initial proposal without a prototype. If you think that
there is a good reason for skipping or delaying the prototype, please
explicitly mention it in this section and provide details on why you'd
like to skip or delay it.
-->

## Alternatives

### Scale environmentd horizontally

We have plans for unrelated reasons to scale environmentd horizontally, and this could get us to a point where we can support faster queries.
We'll treat this as orthogonal for the purpose of this design as we're designing a separate serving layer under the control of environmentd.

<!--
What other solutions were considered, and why weren't they chosen?

This is your chance to demonstrate that you've fully discovered the problem.
Alternative solutions can come from many places, like: you or your Materialize
team members, our customers, our prospects, academic research, prior art, or
competitive research. One of our company values is to "do the reading" and
to "write things down." This is your opportunity to demonstrate both!
-->

## Open questions

* What's the right data structure?
* How do clients talk to the serving replica?
  Is environmentd a bump on the wire, is there a separate service, or do we allow direct connections?

<!--
What is left unaddressed by this design document that needs to be
closed out?

When a design document is authored and shared, there might still be
open questions that need to be explored. Through the design document
process, you are responsible for getting answers to these open
questions. All open questions should be answered by the time a design
document is merged.
-->
