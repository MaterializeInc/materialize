# Guard rails for cluster reliability

- Associated: MaterializeInc/materialize#31246 (MVP)

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

<!--
What is the user problem we want to solve?

The answer to this question should link to at least one open GitHub
issue describing the problem.
-->

Materialize offers little mechanisms to ensure a cluster stays up once it is running, which is at odds with the requirements for operational systems.
Users expect an operational system to be always available, and giving the mechanisms to prevent users from compromising the system.
The underlying issue is that Materialize does not offer performance isolation within a replica.
Any user with the ability to create dataflows, or read from external storage, can create objects that consume the remaining resources and cause unavailability.

Materialize offers coarse-grained mechanisms to prevent some of the issues.
RBAC can express a policy that prevents users from issuing queries, or creating expensive objects.
It does not, however, allow fine-grained permissions: A query that reads from an index causes much less work than a query that requires its own dataflow.
In this design, we look at a spectrum of solutions to address this issue.


## Success Criteria

<!--
What does a solution to this problem need to accomplish in order to
be successful?

The criteria should help us verify that a proposed solution would solve
our problem without naming a specific solution. Instead, focus on the
outcomes we hope result from this work. Feel free to list both qualitative
and quantitative measurements.
-->

Materialize offers a range of mechanisms that allow users to prevent undesired operations on a cluster.

## Out of Scope

<!--
What does a solution to this problem not need to address in order to be
successful?

It's important to be clear about what parts of a problem we won't be solving
and why. This leads to crisper designs, and it aids in focusing the reviewer.
-->

We distinguish transient from permanent dataflows for the purposes of this design.
While we could apply the same mechanism to permanent dataflows, it is less clear how we would operate the system in the presence of a limit violation.
We cannot simply terminate a permanent dataflow, as it would affect all queries that depend on it, and our users expect dataflows to be always-on.
For this reason, we limit the scope of this design to transient dataflows, which are selects and subscribes.

We also do not address the problem how the user can express the limit.
Users do not necessarily know the resources of a replica, and how much a query would consume.
While we can implement absolute or relative limits, it is unclear to the author which one would feel most natural to the user.

## Solution Proposal

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

User's needs aren't uniform; a solution working for one customer might be too limiting for another.
We propose a set of mechanisms that offer a spectrum of solutions:
* We add a RBAC permission `CREATE DATAFLOW`, which is required to create dataflows.
  If the permission is absent, select and subscribe queries can only read from existing objects.
* We add a RBAC permission `READ EXTERNAL`, which is required to read data from external storage (persist).
  The permission controls both dataflows reading from external sources, and peeks that directly read from external storage.
* We implement a mechanism to limit the resources of a dataflow serving a query with a probability.

### `CREATE DATAFLOW` permission

The `CREATE DATAFLOW` permission controls whether a query can render its own dataflow.
Without the permission, Materialize will reject a query that it cannot otherwise fulfill.
The permission applies to a cluster.

Implementing this requires additional plumbing.
At the moment, we evaluate the RBAC permissions before sequencing a plan.
However, we only learn during sequencing whether an object needs to render a dataflow.

### `READ EXTERNAL` permission

The `READ EXTERNAL` permission controls whether a query can read from external storage.
If a query can only be fulfilled by reading data from external storage, Materialize checks that the issuer has the permission on the target cluster.

The permission would apply to all objects, i.e., indexes, materialized views, and queries.
Similar considerations as with `CREATE DATAFLOW` regarding the implementation apply here, too.

### Limiting a query's heap size

Measuring heap size usage is inherently difficult as we cannot attribute all memory usage to a single query.
Any operator can allocate and deallocate memory, and we do not want to (or cannot) instrument all operators.
We can only measure instrumented dataflow operators, and with a time delay, which renders all solutions approximate.

A solution needs to limit the heap size usage of a query to a configurable value, and enforce it with high probability.

To measure the heap size of a query, we hook into the arrangement heap size infrastructure.
It already provides the system with a view of the heap size of each arrangement and its merge batcher.
We can reduce the data for each dataflow, and join it with a per-dataflow limit to obtain the current heap size of a query.
Reducing the value yields the absolute heap size, which we can apply a threshold to detect limits exceeding their allocation.

We then forward the information to the controller, which can decide on how to handle dataflows with excessive resource utilization.
It can decide to terminate and fail a query, or let its execution continue.

We have several choices on how to implement this approach:
* A bespoke dataflow to detect when the heap size of a dataflow exceeds a limit,
* A subscribe-based approach that offloads the detection to the controller.

The fundamental problem of any solution is that we do not have isolation between queries on the same cluster.
Thus, established mechanisms that work on a per-process level do not apply.
Instead, we need to rely on custom instrumentation to report memory utilization.

Timely is a cooperatively-scheduled system, which means that we can only measure and enforce limits at specific points in time.
It depends on the cooperation of all operators, and we cannot enforce limits on operators that do not cooperate.
For any solution, we need operators to be well-behaved: They need to yield regularly and report accurate heap size usage.

We can only enforce a limit with high probability, as an operator can allocate more resources while we're waiting to compute the current utilization, or depend on a decision by the controller.
A user can side-step this problem by chosing a limit that leaves enough room for the system to react.

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


### Limiting a query's heap size

On a high level, we add a session variable `max_query_heap_size`, encoding an optional byte size value.
If set, we instruct the replica to notify the controller if a dataflow would exceed its limits.
The controller can then decide to terminate the query, or let it continue.

For the MVP, we can implement a simple dataflow that reports the heap size of a dataflow to the controller.

## Alternatives

<!--
What other solutions were considered, and why weren't they chosen?

This is your chance to demonstrate that you've fully discovered the problem.
Alternative solutions can come from many places, like: you or your Materialize
team members, our customers, our prospects, academic research, prior art, or
competitive research. One of our company values is to "do the reading" and
to "write things down." This is your opportunity to demonstrate both!
-->

### Heap size: Instrument the memory allocator

As an alternative to measure through arrangement size reporting, we could instrument the memory allocator and Timely scheduler and track (de)allocations on a per-dataflow basis.
The current utilization would then be the sum of all allocations minus deallocations.

While this approach might be more accurate, it is also more invasive and can incur substantial overhead.
The arrangement size logging has little overhead, and only logs the size of the arrangement and its merge batcher at
specific moments in time.
A solution on the allocator level would need to track all allocations and deallocations, even if they do not survive the operator scheduling quantum.

This solution would not be able to enforce limits faster than the MVP, as it would still need to report the current utilization to the controller.

### Heap size: Drop dataflow from within

The design proposal requires the controller to terminate a query if it exceeds its limits, which means another network round-trip.
We could give the replica permission to drop dataflows they detect as exceeding their limits.
This violates current principles that the controller is in charge of what a replica is doing.
Also, it can be difficult to implement as we need to broadcast a worker-local decision to all workers, and the `drop_dataflow` API is still experimental.

## Open questions

<!--
What is left unaddressed by this design document that needs to be
closed out?

When a design document is authored and shared, there might still be
open questions that need to be explored. Through the design document
process, you are responsible for getting answers to these open
questions. All open questions should be answered by the time a design
document is merged.
-->

### Heap size: Delayed response

Any solution that depends on instrospection data has a delay between the problem occurring and its detection.
We can work towards reducing the delay, but we cannot eliminate it.
Is a probabilistic enforcement of limits acceptable to our users?
