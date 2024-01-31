Please note that the cloudtests in this folder require `misc/kind/cluster-node-recovery-test.yaml` instead of
`misc/kind/cluster.yaml`.

Consequently, the environment variable `CLUSTER_DEFINITION_FILE` needs to reference this file:
```
export CLUSTER_DEFINITION_FILE="misc/kind/cluster-node-recovery-test.yaml"
```
