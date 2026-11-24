# Cluster UX Long Term Vision

Associated: [Epic](https://github.com/MaterializeInc/materialize/issues/22120)

Authors: @chaas, @antiguru, @benesch

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

We can classify actions that users take in managing clusters into two categories:
_development workflows_ and _production workflow_.

#### Development workflows
In a development workflow, the underlying set of objects being configured are not being used yet
in a production system, and the user is rapidly changing things.\
In this workflow, downtime is acceptable.\
A command like `ALTER <object> ... SET CLUSTER` (moving an object between clusters) would fall
under this category.

For development workflows, since downtime is acceptable, the primary work items is to
**expose rehydration status**.\
Users need an easy way to detect that rehydration is complete and they can resume querying against
the object.

#### Production workflows
In a production workflow, the underlying set of objects are actively depended on by a production
system.\
In this workflow, downtime is not acceptable.\
A command like `ALTER CLUSTER ... SET (SIZE = <>)` (resizing a cluster) would fall under this
category.

If a user wants to do a development workflow on a production system, they must use **blue/green
deployments**. For example, if the user wants to move an object between clusters, they must use
blue/green to set up another version of the object/cluster and cutover the production system
to it once the object is rehydrated and ready.\
Again, for this workflow, exposing hydration status is the primary work item.

For production workflows, like resizing an active cluster, blue/green is an acceptable intermediate
solution, but is an overkill amount of work for such a simple action.

In an ideal state, we could provide a simple declarative interface for seamlessly resizing.\
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

Engineering may also want the ability to create unbilled shadow replicas for testing new features and
query plan changes, which do not serve customers' production workflows, if they can be made safe.

### Roadmap
**Now**
* @antiguru to work on `ALTER...SET CLUSTER` [#17417](https://github.com/MaterializeInc/materialize/issues/17417), without graceful rehydration.
* @antiguru to continue in-flight work on multipurpose clusters [#17413](https://github.com/MaterializeInc/materialize/issues/17413), which is co-locating compute and storage objects [PR #21846](https://github.com/MaterializeInc/materialize/pull/21846).
* @ggnall to do discovery on the prescriptive data model as part of Blue/Green deployments project [#19748](https://github.com/MaterializeInc/materialize/issues/19748)
* Expose rehydration status [#22166](https://github.com/MaterializeInc/materialize/issues/22166)

**Next**
* Graceful reconfiguration, to support graceful manual execution of `ALTER...SET CLUSTER` and
`ALTER...SET SIZE`.
* Deprecate `CREATE/DROP CLUSTER REPLICA` for users.

**Later**
* Auto-shutdown of clusters.
* Shadow replicas.
* Autoscaling clusters.

**Much Later**
* Clusterless.
