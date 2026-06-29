# Configurable Cluster Replica Sizes

## The Problem

We want users to be able to safely configure cluster replica sizes on runtime for self managed.

> At the moment, we pass the cluster replica size map as a command line argument to environmentd, and this is the only way to specify cluster replica sizes and their metadata. A change to the cluster replica size map requires a restart of environmentd, and patching the Kubernetes stateful set, or whatever deployment mechanism the user has in place. This is a tedious, dangerous, and inefficient process.
>

Currently, cluster sizes are created on bootstrap via CLI arguments, saved in the catalog as `cluster_replica_sizes`, then saved to our builtin tables. On creation of a cluster replica, we save it as a `ReplicaAllocation` object [[code pointer](https://github.com/MaterializeInc/materialize/blob/d37c5be00a29b8b4d5e341f3efac15f9f01a8942/src/adapter/src/catalog/state.rs#L2290)]

## Success Criteria

- Allow a self managed user to configure cluster replica sizes with a schema similar to this code snippet: [https://materialize.com/docs/self-managed/v25.2/sql/appendix-cluster-sizes/#custom-cluster-sizes](https://materialize.com/docs/self-managed/v25.2/sql/appendix-cluster-sizes/#custom-cluster-sizes)
- Allow a user to validate the custom sizes by interacting with the database itself
- Fix potential breaking changes in all clients (i.e. Console)
- Update Terraform providers to use this new functionality

## Out of Scope

- Allow configurable sizes in Cloud
- Propagate modifications of a cluster replica size to all existing replicas using that size
- Documentation of other system variables

## Solution Proposal: Manage cluster replica sizes via a dyncfg

We want to separate system replica sizes from custom user cluster replica sizes where custom user cluster replica sizes come from a dyncfg. The workflow of someone editing a replica size would look something like:

1. Materialize deployments will come with a k8s configmap. This configmap will contain a JSON object called something similar to `custom_user_cluster_replica_sizes` with a shape similar to this [[code snippet](https://materialize.com/docs/self-managed/v25.2/sql/appendix-cluster-sizes/#custom-cluster-sizes)]
1. Edits to the configmap should automatically sync and no restart of environmentd should be required. You can make edits to the configmap either by editing it in Kubenertes directly or syncing it locally with a command like:
    ```
    kubectl get configmap my-configmap -o yaml > my-configmap.yaml
    kubectl apply -f my-configmap.yaml
    ```

1. To verify the cluster replica sizes in the database itself, one can run `SHOW custom_user_cluster_replica_sizes`
1. If the configmap fails to sync, we’ll print out a warning in the logs of the environmentd pod on which field is causing the issue.
- If a cluster size is modified, any existing clusters with that size shouldn’t be affected. Only newly created cluster replicas with the modified cluster size will.
- We can also create a field during the helm chart install to pre-populate this configmap with initial custom cluster replica sizes.
- By default, this configmap will only merge into our dyncfgs and won’t be required.

To achieve this, we’d need to do the following:

### Sync dyncfg values from a file

We currently have functionality to sync dyncfg values from a file: https://github.com/MaterializeInc/materialize/pull/32317

### Orchestrate creation of the file

Similar to the listeners configmap for password auth [[code pointer](https://github.com/MaterializeInc/materialize/blob/v0.151.0/src/orchestratord/src/controller/materialize/environmentd.rs#L1152-L1172)], we can either:

- Allow orchestratord to create a configmap in a volume in environmentd. Then glue the path to the dyncfg synchronization.
- Create a custom cluster size CRD. This will allow orchestratord to handle statefulset creation in the future.

This provides the following positive properties:

- No risk of removal of built in objects that rely on a specific built in cluster size
- Not accessible to cloud by default. We also don’t have to worry about conflicts with our license key check if we automatically set `credits_per_hour` to 0
- Runtime updates via writes to the configmap

The biggest con is it’s unclear how to sync the dyncfg value to our built in catalog tables. I’ve written more about this in the Open Questions section.

## Alternatives

### Utilize `extraArgs` in our Materialize CR by treating it as the source of truth and force a restart of environmentd every time

**Pros:**

- Easiest / quickest to implement. It’d simply be just adding a dyncfg
- Makes it easier to sync the dyncfg value to our built in catalog tables
- Some system variables actually require a restart of environmentd, so this creates a unified interface for all system variables.

**Cons:**

- Can’t edit cluster replica sizes on runtime

### Manage as catalog objects

**Pros:**

- Builtin table updates become easier

**Cons:**

- More time consuming to implement and we lock ourselves into backwards compatibility with this new syntax
- Central source of truth of cluster sizes lives in the database. Can’t easily just update a file
- Not clear that the user actually wants a DML interface

# Rollout

## Testing

- environmentd test to see if the dyncfg reflects in the system catalog, similar to `src/environmentd/tests/bootstrap_builtin_clusters.rs`
- Cloudtest that asserts live changes to the synced file reflect in the database

## Lifecycle

- Customers will roll this out using a new version of the operator as well as Materialize. This would involve a `helm upgrade` on the operator and a `kubectl apply` on the Materialize CR.

# Open questions

- What should the interface be in the Materialize CR? We have the following options:
    - A boolean that signals if we want to create the configmap
    - Allow defining cluster replica sizes in the Materialize CR
        - Risk here is we don’t want the Materialize CR definition file to be the source of truth
- We currently sync the builtin table `mz_cluster_replica_sizes` with CLI arguments during bootstrap by updating the catalog in-memory with our new sizes then writing to our built in tables. This is an issue however since there’s no mechanism to sync the dyncfg to our builtin tables and it’s questionable if we really want to write to our builtin tables upon changes on a json string dyncfg. There are a few options:
    - Watch for changes to the dyncfg and write to builtin tables
    - De-normalize `mz_cluster_replica_sizes` into `mz_clusters`.
    - Document restarting envd when they update the dyncfg variable in order for changes to appear in the Console
