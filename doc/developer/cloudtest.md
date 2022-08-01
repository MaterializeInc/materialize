# Introduction

`cloudtest` is a test framework that allows testing Materialize inside Kubernetes.

Using a K8s environment for testing has the advantage that the same orchestrator that
is used in the Cloud is used to spawn computeds and storageds. K8s will also be responsible
for restarting any containers that have exited.

The framework is pased on pytest and KinD and uses, for the most part, the official `kubernetes`
python library to control the K8s cluster.

# Setup

1. Install kubectl

```
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
chmod+x kubectl
sudo mv kubectl /usr/local/bin
```

2. Install KinD

```
go get sigs.k8s.io/kind
export PATH=$PATH:$(go env GOPATH)/bin
```

More installation options are available here [https://kind.sigs.k8s.io/docs/user/quick-start/#installing-with-a-package-manager]

3. Create a Kubernetes KinD cluster

```
~/go/bin/kind create cluster --config=misc/kind/cluster.yaml
```

# Running

```
cd test/cloudtest
./pytest
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
