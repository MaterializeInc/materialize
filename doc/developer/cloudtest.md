# Introduction

cloudtest is a test framework that allows testing Materialize inside Kubernetes.

Using a Kubernetes environment for testing has the advantage of exercising the
same code paths used in production used to orchestrate cloud resources (e.g.,
clusters and secrets). Kubernetes will also be responsible for restarting any
containers that have exited.

Notable deviations from production include:

  * Using [MinIO] instead of S3 for `persist` blob storage.
  * Using a single-node CockroachDB installation instead of Cockroach Cloud.
  * No access to AWS resources like VPC endpoints.

The framework is based on [pytest] and [kind] and uses, for the most part, the
official [`kubernetes`] Python library to control the Kubernetes cluster.

# Setup

1. Install [kubectl], the official Kubernetes command-line tool:

    On macOS, use [Homebrew] to install it:

    ```
    brew install kubectl
    ```

    On Linux, use:

    ```
    curl -fL https://dl.k8s.io/release/v1.26.6/bin/linux/amd64/kubectl > kubectl
    chmod +x kubectl
    sudo mv kubectl /usr/local/bin
    ```

    See the [official kubectl installation instructions][kubectl-installation]
    for additional installation options.

2. Install [kind], which manages local Kubernetes clusters:

    On macOS, use:

    ```
    brew install kind
    ```

    On Linux, use:

    ```
    curl -fL https://kind.sigs.k8s.io/dl/v0.14.0/kind-linux-amd64 > kind
    chmod +x kind
    sudo mv kind /usr/local/bin
    ```

    See the [official kind installation instructions][kind-installation]
    for additional installation options.

3. Create and configure a dedicated kind cluster for cloudtest:

    ```
    cd test/cloudtest
    ./setup
    ```

4. On macOS, configure Docker to use "gRPC FUSE" as file sharing implementation for the containers
   (Docker settings, tab "General"). This will speed up the execution of cloudtests.

# Running tests

To run all short tests:

```
./pytest
```

To run a single test:

```
./pytest -k test_name_goes_here
```

⚠️ By default, cloudtest builds Materialize in release mode. You can instead
build in debug mode by passing the `--dev` flag:

```
./pytest --dev [-k TEST]
```

⚠️ By default, cloudtest only runs short tests. To include long tests you can include the `-m=long`
flag:

```
./pytest -m=long
```

To check the cluster status:

```
kubectl --context=kind-mzcloud get all
```

Consider also using the [k9s] terminal user interface:

```
k9s --context=kind-mzcloud
```

To remove all resources from the Kubernetes cluster, so that a test can be rerun
without needing to reset the cluster:

```
./reset
```

To remove the Kubernetes cluster entirely:

```
./teardown
```

# Interactive development

cloudtest is also the recommended tool for deploying a local build of
Materialize to Kubernetes, where you can connect to the cluster and
interactively run tests by hand.

Use the `test_wait` workflow, which does nothing but wait for the default
cluster to become ready:

```
./pytest --dev -k test_wait
```

# Writing tests

See the examples in `test/clustertest/test_smoke.py`.

The tests follow pytest conventions:

```python
from materialize.cloudtest.app.materialize_application import MaterializeApplication

def test_something(mz: MaterializeApplication) -> None:
    assert ...
```

The `MaterializeApplication` object is what creates the Kubernetes cluster. It
is instantiated once per `pytest` invocation

## Waiting for a resource to reach a particular state

```
from materialize.cloudtest.util.wait import wait

wait(condition="condition=Ready", resource="pod/compute-cluster-u1-replica-u1-0")
```

`wait` uses `kubectl wait` behind the scenes. Here is what the `kubectl wait`
documentation has to say about the possible conditions:

```shell
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

In particular, to wait until a resource has been deleted:

```python
wait(condition="delete", resource="secret/some_secret")
```

## Running testdrive

```python
mz.testdrive.run(
    input=dedent(
        """
        > SELECT 1;
        1
        """
    )
)
```

Note that each invocation of `testdrive` will drop the current database and
recreate it. If you want to run multiple `testdrive` fragments within the same
test, use `no_reset=True` to prevent cleanup and `seed=N` to make sure they all
share the same random seed:

```python
mz.testdrive.run(..., no_reset=True, seed = N)
```

## Running one-off SQL statements

If no result set is expected:

```python
mz.environmentd.sql("DROP TABLE t1;")
```

To fetch a result set:

```python
id = mz.environmentd.sql_query("SELECT id FROM mz_secrets WHERE name = 'username'")[0][0]
```

## Interacting with the Kubernetes cluster via kubectl

You can call `kubectl` and collect its output as follows:

```
secret_description = mz.kubectl("describe", "secret", "some_secret")
```

## Interacting with the Kubernetes cluster via API

The following methods

```python
mz.environmentd.api()
mz.environmentd.apps_api()
mz.environmentd.rbac_api()
```

return API handles that can then be used with the official [`kubernetes`] Python
module.

[Homebrew]: https://brew.sh
[`kubernetes`]: https://github.com/kubernetes-client/python
[k9s]: https://k9scli.io
[kind-installation]: https://kind.sigs.k8s.io/docs/user/quick-start/#installing-with-a-package-manager
[kind]: https://kind.sigs.k8s.io
[kubectl-installation]: https://kubernetes.io/docs/tasks/tools/
[kubectl]: https://kubernetes.io/docs/reference/kubectl/
[MinIO]: https://min.io
[pytest]: https://pytest.org

# Troubleshooting

## DNS issues

If pods are failing with what seems like DNS issues (can't resolve redpanda, or
cannot connect to postgres) you can try and have a look at the [relevant
Kubernetes
documentation](https://kubernetes.io/docs/tasks/administer-cluster/dns-debugging-resolution/).
At least the list of [known
issues](https://kubernetes.io/docs/tasks/administer-cluster/dns-debugging-resolution/#known-issues)
can be very relevant for your linux distribution, if it is running
`systemd-resolved`.

In at least one case, a VPN (mullvad) was interfering with DNS resolution. Try
de-activating your VPN and then tear down and restart your testing cluster to
see if that helps.

## botocore.exceptions.ClientError: An error occurred (AccessDenied) when calling the CreateMultipartUpload operation: Access Denied

If tests are failing almost immediately while trying to upload a file to S3, it may be a bug in our debuginfo upload logic. You can _**unset**_ all your AWS credentials to work around this.

## Failure joining worker nodes

If `./setup` fails during the `Joining worker nodes` step and spams 404 error messages, the kubelet has likely died on at least one node. You can troubleshoot this by adding `--retain` to the `kind create cluster` command in `setup`, and then `docker exec -it "$node" bash` to access the node. From there you can access the kubelet logs with `journalctl -xeu kubelet`.

Some common issues are listed at https://kind.sigs.k8s.io/docs/user/known-issues . We launch many nodes, so it is likely to be the inotify limits.
