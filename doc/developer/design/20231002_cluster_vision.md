# Cluster UX Long Term Vision

- Associated: [Epic](https://github.com/MaterializeInc/materialize/issues/22120)

## The Problem
We need a documented vision for the cluster UX in the long term which covers both
the "end state" goal as well as the short and medium states in order to:
* Ensure alignment in the future that we are working toward
* Make product prioritization decisions around cluster work
* Make folks more comfortable accepting intermediate states that aren't ideal in service of a greater goal
* Come up with a narrative for customers on what to expect around cluster management

Epic: https://github.com/MaterializeInc/materialize/issues/22120

## Success Criteria
Primarily, a merged design doc that is reviewed and approved by EPD leadership,
and is socialized to GTM.

Secondarily, a roadmap for cluster work for the next quarter.

Qualitatively, positive feedback from EPD leadership and GTM folks that they
have clarity on the vision and roadmap, and the reasoning behind those decisions.

## Out of Scope
Designing the actual cluster API changes themselves, or proposing implementation details.

## Solution Proposal
The objectives we are striving for with the cluster UX:
* Easy to use and manage
* Maximize resource efficiency/minimize unused resource cost
* Enable fault tolerance/use-case isolation

### Declarative vs Imperative
We should move toward a declarative API for managing clusters, where:

Declarative is like `CREATE CLUSTER` with managed replicas and \
Imperative is like `CREATE/DROP CLUSTER REPLICA`.

This means deprecating manual cluster replica management. \
We believe this is easier to use and manage.

The primary work item for this is **graceful reconfiguration**. At the moment, a change in size causes downtime until the new replicas are hydrated. As such, customers still want the flexibility to create their own replicas for graceful resizing. We can avoid this by leaving a subset of the original replicas around until the new replicas are hydrated. \
This requires us to 1) detect when hydration is complete and 2) trigger database object changes based on this event (without/based on an earlier DDL statement).

Another consideration is internal use-cases, such as unbilled replicas. We may want to keep around an imperitive API for internal (support) use only.

To be determined: whether replica sets fits into this model, either externally exposed or internal-only. Perhaps they are a way we could recover clusters with heterogeneous replicas while retaining a declarative API.

### Resource usage
The very long-term goal is clusterless Materialize, where Materialize does automatic workload scheduling for the customer.

An intermediary solution, which is also far off is autoscaling of clusters, where Materialize automatically resizes clusters based on the observed workload.

A more achievable offering in the short-term is automatic shutdown of clusters, where Materialize can spin down a cluster to 0 replicas based on certain criteria, such as a scheduled time or amount of idle time. \
This would reduce resource waste for development clusters. The triggering mechanism from graceful rehydration is also a requirement here.

### Data model
We should move toward prescriptive guidance on how users should configure their clusters with respect to databases and schemas, \
e.g. should clusters typically be scoped to a single schema.

We should also be more prescriptive about what data should be colocated, \
e.g. when should the user create a new cluster for their new sources/MVs/indexes versus increase the size of their existing cluster.

We believe this will make it clearer how to achieve appropriate fault tolerance and maxmimize resource efficiency.

### Support & testing
Support is able to create create unbilled or partially billed cluster resources for resolving customer issues. This is soon to be possible via unbilled replicas [#20317](https://github.com/MaterializeInc/materialize/issues/20317).

### Roadmap
**Now**
* @antiguru to complete `ALTER...SET CLUSTER` [#20841](https://github.com/MaterializeInc/materialize/issues/20841), without graceful rehydration.
* @antiguru to continue in-flight work on multipurpose clusters [#17413](https://github.com/MaterializeInc/materialize/issues/17413) - TODO(@antiguru): fill in details.
* @ggnall to do discovery on the prescriptive data model as part of Blue/Green deployments project [#19748](https://github.com/MaterializeInc/materialize/issues/19748)

**Next**
* Graceful rehydration, to support graceful manual execution of `ALTER...SET CLUSTER` and `ALTER...SET SIZE`.
* Deprecate `CREATE/DROP CLUSTER REPLICA` for users.

**Later**
* Auto-shutdown of clusters.
* Shadow replicas.

**Much Later**
* Autoscaling clusters / clusterless.

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
