# On-disk regions

- Associated: [#22678](https://github.com/MaterializeInc/materialize/issues/22678)

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

Materialize cannot support larger-than-memory workloads, both during temporary memory spikes and in steady-state.
Instead of degrading gracefully, it terminates the process and starts all-over.
This can cause situations where replicas cannot restart without intervention of support.

This document analyzed the problem, and outlines what we need to change to avoid the issue.

## Success Criteria

<!--
What does a solution to this problem need to accomplish in order to
be successful?

The criteria should help us verify that a proposed solution would solve
our problem without naming a specific solution. Instead, focus on the
outcomes we hope result from this work. Feel free to list both qualitative
and quantitative measurements.
-->

* Materialize tolerates temporary memory spikes and gives users time to react.
* Materialize degrades gracefully when workload characteristics change, giving users time to react.

## Out of Scope

<!--
What does a solution to this problem not need to address in order to be
successful?

It's important to be clear about what parts of a problem we won't be solving
and why. This leads to crisper designs, and it aids in focusing the reviewer.
-->

* Materialize externalizes its arrangement state to disk.

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

We identified the following sources of significant memory allocations:
* Columnation on disk: Arrangements contain data organized into batches. We already support
  region-allocating data that makes up parts of the batches, namely the pointers from keys and values. Moving this to
  disk-based regions is little effort and enables us to degrade gracefully during steady-state. Prootype in [#22723].
* Columnated staging area: Arrangements collect data for not-yet-finished times before promoting them to a batch.
  The data is not region-allocated, and we only convert it once it enters the arrangement. Moving the staging area to
  disk-based regions enables us to mitigate running out of memory during hydration. [#348] enhances Differential to
  support building region-allocated data in the staging area.
* List-of-list keys and values: The first level of allocation of keys and values (and associated offsets) is not
  region-allocated and consumes heap space proportional to the number of distinct keys and values. (No issue yet.)
* Columnation for time/delta `(T, R)`: The data is heap allocated, and we can not region-allocate it because Differential requires
  mutable access to the data. Follow issue [#397].
* Persist sources and sinks: Consume space during startup, but out of scope for this effort.

The following table lists the sources of memory allocations and shows for each of them their impact on hydration,
steady-state, and data access cost. A 0 indicates that we expect no change, a +/++ indicates that we expect a (large)
improvement, and a -/-- indicates that we expect a regression.

| &darr; Change            | Memory during hydration | Steady-state memory | Data access cost |
|--------------------------|-------------------------|---------------------|------------------|
| Columnation on disk      | 0*                      | ++                  | -                |
| Columnated staging area  | ++                      | +                   | 0                |
| Columnation for (T, R)   | 0*                      | +                   | -                |
| List-of-list keys/values | 0*                      | ++                  | --               |


[#348]: https://github.com/TimelyDataflow/differential-dataflow/pull/348
[#397]: https://github.com/TimelyDataflow/differential-dataflow/issues/397

[#22723]: https://github.com/MaterializeInc/materialize/pull/22723

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
there is a good reason for skpiping or delaying the prototype, please
explicitly mention it in this section and provide details on why you you'd
like to skip or delay it.
-->

We add support for disk-based regions as follows:
* Integrate [`lgalloc`] with Materialize.
* Add a feature flag `enable_columnation_lgalloc` to control whether we use `lgalloc` or the default allocator.
* Use the `DISK` flag on clusters/replicas to request local disk.

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


[`lgalloc`]: https://github.com/antiguru/rust-lgalloc