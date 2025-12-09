# Reading from replacement materialized views

Associated:
* https://github.com/MaterializeInc/materialize/pull/34234 (MVP: `CREATE MATERIALIZED VIEW ... REPLACING`)
* [Replacement materialized views](./20251111_replacement_materialized_views.md) (design doc for the broader feature)

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

We recently introduced the feature of replacement materialized views, which allow users to create materialized views that can replace existing ones.
The feature relies on the ability of materialized views to self-correct, meaning that only once the user applies the replacement, its results will be visible to queries.
However, users may want to read from the replacement materialized view even before applying it.
For example, users may want to validate the correctness of the replacement materialized view before applying it by querying it and comparing its results to the original materialized view.
To address this issue, we need to provide a mechanism for users to read from replacement materialized views.

## Success Criteria

<!--
What does a solution to this problem need to accomplish in order to
be successful?

The criteria should help us verify that a proposed solution would solve
our problem without naming a specific solution. Instead, focus on the
outcomes we hope result from this work. Feel free to list both qualitative
and quantitative measurements.
-->

Users can inspect the contents of replacement materialized views before applying them.
Ideally, they can use familiar SQL constructs (e.g., `SELECT` statements) to read from replacement materialized views.
Secondarily, users can inspect meta-data about replacement materialized views, such as the size and volume of the correction data they would write once applied.

## Out of Scope

<!--
What does a solution to this problem not need to address in order to be
successful?

It's important to be clear about what parts of a problem we won't be solving
and why. This leads to crisper designs, and it aids in focusing the reviewer.
-->

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

The implementation of this feature is not immediately clear, as we're facing several constraints related to the current architecture of replacement materialized views and how Materialize handles queries.
Let's first define axioms that we'd like to uphold with our design:
1. Homogeneity: Reading from a replacement materialized view behaves similarly to reading from a regular view and does not require special syntax.
2. Structural equivalence: After applying a replacement materialized view, its shape and behavior should be identical to that of a regular materialized view.
3. Reading from a replacement materialized view does not interfere with the ongoing replacement process of the original materialized view.
4. The solution should be efficient and not introduce significant overhead to the system.
5. Replacements, and replaced materialized views survive environmentd restarts, and can be reconciled after a restart.

### Constraint: Materialized views are readable through persist

Users can only query the contents of materialized views through the persist source.
This means that we cannot use the canonical shard of a materialized view to read its replacement's contents, as it is not directly queryable.
Instead, we need to find a way to expose the replacement materialized view's contents through a different mechanism.

* Proposal: Create a temporary persist shard for the replacement materialized view.
    When a user creates a replacement materialized view, we can create a temporary persist shard.
    The replacement materialized view writes to the canonical shard (but is in read-only mode), and also writes to the temporary persist shard.
    Users can then query the temporary persist shard to read the contents of the replacement materialized view.

* Proposal: Expose an index of the replacement materialized view's contents.
    Instead of creating a temporary persist shard, we can expose an index of the replacement materialized view.
    This index would be queryable, allowing users to read the contents of the replacement materialized view directly.

Both proposals would allow users to read from replacement materialized views without interfering with the ongoing replacement process or the original materialized view.
Writing to a temporary persist shard may introduce some overhead, but it would provide a straightforward way for users to query the replacement materialized view and would allow querying the replacement from a different cluster.
Providing an index would introduce memory overhead, but it would allow for more efficient querying of the replacement materialized view from the same cluster.

Both proposals would need to ensure that the replacement materialized view's shape and behavior are identical to that of a regular materialized view after applying the replacement.
We render a materialized view (and replacements) as single dataflows.
Timely constrains us from modifying dataflows after they are created, so we cannot add or remove operators after a dataflow is created.
To avoid violating the structural equivalence axiom, we would need to ensure that the dataflow for the replacement materialized view is identical in shape to that of a regular materialized view.

It follows that we need finer-grained control over the structure of dataflows serving materialized views.

> [!NOTE]
> Why we cannot easily shut down parts of running dataflows.
> In Timely dataflow, an operator lives as long as it can produce data.
> Timely determines this based on its upstream operators, and the capabilities that the operator holds.
> Only once all upstream operators have terminated, and the operator has no capabilities left, can it terminate.
> This means that even if we were to "shut down" an operator in a dataflow, it would still be alive as long as its upstream operators are alive.
> This makes it challenging to shut down parts of a dataflow without terminating the entire dataflow.
>
> We could introduce a shunt operator that can be toggled to either pass through data or drop it.
> This operator would allow us to effectively "shut down" parts of a dataflow by dropping capabilities and data.
> However, we would need to clone data for each output of the shunt operator, which could introduce memory overhead.
>
> TODO: Could we use an operator that works on reference-counted data?
> We'd need to clone the data, but that's cheap for `Rc`.
> Downstream dataflows aren't set up to accept reference-counted data today, though.

One solution to this problem is to split the dataflow into multiple dataflows and connect them with a capture/replay mechanism.
For example, we could have one dataflow that computes the view's results and captures the output, and another dataflow that writes the data to persist.
A replacement materialized view could then have a similar structure, with an additional dataflow that writes to the temporary persist shard or maintains the index.
One the user applies the replacement, we could shut down the dataflow that writes the temporary persist shard or maintains the index, and keep the rest running.
This would allow us to have finer-grained control over the structure of the dataflows and ensure structural equivalence.

A downside of this approach is that it introduces additional complexity to the system.
We would need to name the dataflows and manage their lifecycles, which would complicate the meaning of (global) IDs.

### Constraint: Reconciliation and restarts

In Materialize, all parts should behave deterministically and be restorable after a restart.
This means that the planner and coordinator need to be able to reconstruct the state of replacement materialized views after a restart.
For this to work, we need to ensure that any temporary persist shards or indexes are also restorable after a restart, and that the dataflow fragments have unique names/IDs that can be used to identify them.

This implies that we'd break the 1:1 association of catalog items to global IDs, as a replacement materialized view would now be associated with multiple dataflows (one for the original materialized view, one for the replacement, and potentially one for the temporary persist shard or index).

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

### Coordinator changes

In the coordinator, we adjust handling materialized views as follows:
* We render a materialized view as two dataflows:
    1. A dataflow that computes the view's results and captures the output.
    2. A dataflow that replays the captured output to persist.
* For replacement materialized views, we render an additional dataflow that writes to a temporary persist shard or maintains an index.
* We assign unique global IDs to each dataflow fragment associated with a materialized view.
  We store the global IDs, and their purpose in the catalog item, as part of the version information.

### Catalog changes

Once we complete above steps, catalog items can have multiple global IDs per version associated with them.
We need to change how we store this information.
Previously, a version associates a single global ID:
```protobuf
syntax = "proto3";

message ItemVersion {
  GlobalId global_id = 1;
  Version version = 2;
}
```

We need to adjust this to store multiple global IDs with their purpose:
```protobuf
syntax = "proto3";

message ItemVersion {
  repeated GlobalIdMapping global_ids = 1;
  Version version = 2;
}

message GlobalIdMapping {
  GlobalId global_id = 1;
  string purpose = 2; // e.g., "view", "persist", "temp_persist", "index"
}
```

An alternative to consider is to have strongly-typed purposes, e.g., an enum instead of a string.

### Optimizer changes

Splitting dataflows into multiple fragments may affect optimization.
It can give us the opportunity to unify optimization pipelines for views, indexes, materialized views, and replacement materialized views.

## Alternatives

<!--
What other solutions were considered, and why weren't they chosen?

This is your chance to demonstrate that you've fully discovered the problem.
Alternative solutions can come from many places, like: you or your Materialize
team members, our customers, our prospects, academic research, prior art, or
competitive research. One of our company values is to "do the reading" and
to "write things down." This is your opportunity to demonstrate both!
-->

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
