# Introduction

`cloudtest` is a test framework that allows testing Materialize inside Kubernetes.

Using a K8s environment for testing has the advantage that the same orchestrator that
is used in the Cloud is used to spawn computeds and storageds. K8s will also be responsible
for restarting any containers that have exited.

The framework is pased on pytest and KinD and uses, for the most part, the official `kubernetes`
python library to control the K8s cluster.

# Installation and setup

1. Install kubectl

```
curl -LO "https://dl.k8s.io/release/v1.24.3/bin/linux/amd64/kubectl"
chmod +x kubectl
sudo mv kubectl /usr/local/bin
```

2. Install KinD

```
curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.14.0/kind-linux-amd64
chmod +x kind
sudo mv kind /usr/local/bin
```

More installation options are available here [https://kind.sigs.k8s.io/docs/user/quick-start/#installing-with-a-package-manager]

3. Create a Kubernetes KinD cluster

```
kind create cluster --config=misc/kind/cluster.yaml
```

# Running tests

```
cd test/cloudtest
./pytest
```

To run a single test:

```
./pytest -k test_name_goes_here
```

## Checking K8s cluster status

```
kubectl --context kind-kind get all
```

## Resetting the K8s cluster

This command removes almost all resources from the K8s cluster, so that a test can be rerun.

```
kubectl --context kind-kind delete all --all
```

# Writing tests

See the examples in `test/clustertest/test_smoke.py`

The tests folow pytest conventions:

```
from materialize.cloudtest.application import MaterializeApplication

def test_something(mz: MaterializeApplication) -> None:
    assert ...
```

The `MaterializeApplication` object is what creates the K8s cluster. It is instantiated once per `pytest` invocation

## Waiting for a resource to reach a particular state


```
from materialize.cloudtest.wait import wait

wait(condition="condition=Ready", resource="pod/compute-cluster-1-replica-1-0")
```

`wait` uses `kubectl wait` behind the scenes. Here is what the `kubectl wait` documentation has to say about the possible conditions:

```
  # Wait for the pod "busybox1" to contain the status condition of type "Ready"
  kubectl wait --for=condition=Ready pod/busybox1

  # The default value of status condition is true; you can wait for other targets after an equal delimiter (compared
after Unicode simple case folding, which is a more general form of case-insensitivity):
  kubectl wait --for=condition=Ready=false pod/busybox1

  # Wait for the pod "busybox1" to contain the status phase to be "Running".
  kubectl wait --for=jsonpath='{.status.phase}'=Running pod/busybox1

  # Wait for the pod "busybox1" to be deleted, with a timeout of 60s, after having issued the "delete" command
  kubectl delete pod/busybox1
  kubectl wait --for=delete pod/busybox1 --timeout=60s
```

In particular, to wait unti a resource has been deleted:

```
wait(condition="delete", resource="secret/some_secret")
```

## Running testdrive

```
    mz.testdrive.run_string(
        dedent(
            """
            > SELECT 1;
            1
            """
        )
    )
```

Note that each invocation of `testdrive` will drop the current database and recreate it. If you want
to run multiple `testdrive` fragments within the same test, use `no_reset=True` to prevent cleanup
and `seed=N` to make sure they all share the same random seed:

```
    mz.testdrive.run_string(..., no_reset=True, seed = N)
```

## Running one-off SQL statements

If no result set is expected:

```
    mz.environmentd.sql("DROP TABLE t1;")
```

To fetch the result set:

```
    id = mz.environmentd.sql_query("SELECT id FROM mz_secrets WHERE name = 'username'")[
        0
    ][0]
```

## Interacting with the cluster via kubectl

You can call `kubectl` and collect its output as follows:

```
    secret_description = mz.kubectl("describe", "secret", "some_secret")
```

## Interacting with the K8s cluster via API

The following methods

```
   mz.environmentd.api()
   mz.environmentd.apps_api()
   mz.environmentd.rbac_api()
```

return API handles that can then be used with the official `kubernetes` python module.
