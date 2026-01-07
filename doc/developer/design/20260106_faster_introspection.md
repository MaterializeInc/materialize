# Faster introspection

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

## Problem

<!--
What is the user problem we want to solve?

The answer to this question should link to at least one open GitHub
issue describing the problem.
-->

In Materialize, cluster replicas host the user's workload.
Users observe the status of their workloads via introspection queries that gater information from various parts of the system.
Currently, the queries can be slow or unresponsive when the system is under load.
In the limit, this makes it impossible to observe progress for workloads that never hydrate, and slow for others.

In this design, we want to outline alternatives for improving the performance of introspection queries.
Specifically, we want to improve the performance of queries against _compute introspection_, which present as indexes maintained by the compute layer itself.

## Success criteria

<!--
What does a solution to this problem need to accomplish in order to
be successful?

The criteria should help us verify that a proposed solution would solve
our problem without naming a specific solution. Instead, focus on the
outcomes we hope result from this work. Feel free to list both qualitative
and quantitative measurements.
-->

Users can query introspection data with minimal delay, even when the system is under load.

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

In compute, we collect introspection data through Timely's logging mechanism, and replay the data on the same worker that generated it.
This means that when a worker is busy, it cannot process new introspection data or respond to queries.
We cannot preempt operators as Timely follows a cooperative multitasking model.

This implies that we need to separate the introspection data processing from the main query processing path, and move it to a different timely runtime.
This way, even if the main runtime is busy, the introspection data can still be processed and queries can be answered.

* Reserve workers: We can reserve a subset of workers in the cluster for processing introspection data.
  These workers would run a separate Timely runtime that only processes introspection data.
* Sidecar cluster replica: We can run a separate cluster replica that only processes introspection data.
  This replica would run a separate Timely runtime that only processes introspection data.
  The main cluster replica would send introspection data to the sidecar replica.

The separate cluster could maintain the introspection data as indexes, or write it to persist if desired.
This part of the design is orthogonal to the main proposal, and can be decided based on the desired trade-offs.

Separating the introspection data processing into a separate timely runtime has implications on how we can query the data.
Currently, introspection queries are executed in the same cluster replica as the user's workload.
With the proposed design, we need to route introspection queries to a separate cluster replica.

* When a user creates a replica, we spawn a sidecar replica for introspection.
  The user can then query the sidecar replica for introspection data.
* We could share a sidecar replica for multiple user replicas.

All solutions sending data over the network introduce a new failure mode as the network can be unreliable.


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
