# When is a cluster in production?

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

<!--
What is the user problem we want to solve?

The answer to this question should link to at least one open GitHub
issue describing the problem.
-->

Users host their workload on clusters, but have low confidence that a setup that works now still works in the future.
Materialize, at the same time, has no way to let users mark a cluster as production-relevant, or signal to users that a cluster is not production-ready.
This leads to frustration when restarting Materialize: Users observe downtime, need to scale, and Materialize engineers need to make ad-hoc decisions on how to react.
We try to solve this problem by introducing active and passive signals characterizing the production-readiness of a setup.

## Success Criteria

<!--
What does a solution to this problem need to accomplish in order to
be successful?

The criteria should help us verify that a proposed solution would solve
our problem without naming a specific solution. Instead, focus on the
outcomes we hope result from this work. Feel free to list both qualitative
and quantitative measurements.
-->

This effort is successful if all workloads in production can restart with confidence, without requiring manual intervention or observing additional downtime over a baseline.

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

We introduce active and passive signals that allow users to express their workload status, and for Materialize to signal events with possibly catastrophic outcomes.

### User's expressing their workload's status

We should treat any user workload as important, but need to distinguish development and production use-cases.
Once a user determines that their use-case reached a certain maturity, they can seal a cluster, from which point on Materialize will actively refuse modifications that could compromise the cluster's stability.

A sealed cluster would not allow creating and dropping objects, or creating dataflows in support of select or subscribe queries.
Users can unseal a cluster to enable modifications.

Some configurations are not stable, as in a restart will cause a change in behavior.
Consider an index shared by multiple objects, which is then dropped.
Sealing this cluster would prevent further modifications, but there is no guarantee the cluster could successfully restart.

Idea: Refuse to seal dirty clusters/clusters with only dirty replicas.

Idea: Seal causes a graceful restart of the cluster.

### Observing the workload status

A replica receives a stream of commands, which includes punctuation to distinguish a prefix of re-applied operations from updates in steady-state.
We could lean into this to determine whether a replica has observed DDL since it entered steady-state.
A replica that has not observed DDL is considered clean, versus one that has observed DDL, which is considered dirty.

In this context, all instructions to render dataflows, or advance frontiers to the empty frontier, are considered replica-level DDL.

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
