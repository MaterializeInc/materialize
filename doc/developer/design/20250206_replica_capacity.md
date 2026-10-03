# Replica capacity

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

A core problem of using Materialize today is that users cannot rely on a configuration that works today to continue working in the future.
We give indications about a workload's status, but it is easy to ignore them, or drift into an unsupported configuration by workload changes or unintended DDL operations.
Here, we discuss the problem of how Materialize can restart a workload successfully.

At the moment, we present detailed metrics, such as memory, CPU and disk utilization, for replicas, with the hope that the metrics successfully characterize the health of a replica, and allow users to make scaling decisions.
While the metrics are useful from an operational perspective, they are not suitable to predict the future behavior of a replica.
Specifically, they are a bad predictor for whether a replica can successfully restart, since the resource utilization during restart can be different from steady-state.
In this design we introduce the concept of _capacity_, which refers to the resource requirements during the whole lifetime of a replica and the object it is hosting, and ideally allows users to operate Materialize with confidence.

## Success Criteria

<!--
What does a solution to this problem need to accomplish in order to
be successful?

The criteria should help us verify that a proposed solution would solve
our problem without naming a specific solution. Instead, focus on the
outcomes we hope result from this work. Feel free to list both qualitative
and quantitative measurements.
-->

The ideal outcome of this design is a capacity metric that characterizes the resource requirements of a replica and its objects during the whole life cycle, from startup and hydration, to steady-state.
We guarantee configuration with an acceptable capacity can successfully restart, without intervention by the user or a Materialize engineer.

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

The _capacity_ of a replica is percentage value between 0 and unbounded.
It does not directly correspond to any specific metric, but is based on several observable signals.

An _acceptable capacity_ is one that is less or equal to 95%.
Any capacity above is considered unstable and there are not guarantees that a configuration can start successfully.
(The actual number here is less important than us specify any number with confidence.)

We need to relate the resource requirements of an object to the resources a replica can offer.
This is a nontrivial task since we do not know precisely how many resources an object requires over time, during hydration and in steady-state.
A replica can host several objects, and some hydrate in parallel, which means that the sum of their requirements must fit the offered resources.
Assuming the worst-case scenario is certainly helpful to guarantee successful restarts, but comes at the cost of potentially over-provisioning the replica.

Sequential hydration allows us to limit how many objects hydrate in parallel, where a hydration parallelism of 1 is a valid value.
(We currently use 4 as the default.)
We do not include this in the first iteration of the design as it complicates the problem.

The resource requirements of an object depend on the input data, their transformation, and the output type.
Indexes maintain data for immediate access, materialized views write to storage.
We maintain some data in memory, and some on spillable memory.

For the purpose of this document, we should not distinguish between spillable and non-spillable memory.
It would require us to reason about the resource requirements with greater accuracy, and disk serves as a mechanism to survive temporary workload variations without crashing.

Let me discuss the resource requirements for indexes and materialized views, which are the only permanent objects compute supports.
(Sources and sinks suffer from the same problem, but the author lacks knowledge about their behavior.)

### Indexes

Indexes require at most resources proportional to the product of their inputs.
For most indexes, this is a vast overestimation and is only true for cross-joins.
Most other joins are cardinality-preserving or shrink the output size.
In the absence of advance analysis of query plans, it's better to look at observed metrics.

We know the steady-state memory utilization and ignore other signals like the size of its inputs.
The required resources are within a factor of two from the steady-state utilization.

### Materialized views

Materialized views suffer from the same problem as indexes, but they do not necessarily maintain their output in memory.
The resource requirements can be approximated as within a factor of two of its steady-state plus the output size.

### Sources and sinks

@antiguru cannot say much about non-compute objects. :/

### Combining resource requirements

Once we know the resource requirements of each object we can estimate the resources required to successfully restart a replica.
We sum the requirements for each object and divide it by the resources offered by the replica.
As we assume the estimation to be off by a factor of two, a ratio of $\frac{1}{2}$ would correspond to a capacity of 100%.
Thus, with some slack, a target capacity of 95%, which would correspond to a maximal utilization of 90% seems adequate.

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

Before implementing any of this idea in code, we can validate the hypotheses by observing how Materialize behaves in production.

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

* Should we have a finer-grained model that can take sequential hydration into account?
  While possible, it would require us to exercise more control over what gets hydrated in which order, which I think is a separate problem.
