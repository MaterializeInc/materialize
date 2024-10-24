# Local Manual Testing

This directory contains simple examples for deploying MinIO, PostgreSQL, and Redpanda instances in a Kubernetes cluster. These configurations are intended for testing purposes only.

## Contents

- **minio.yaml**: Deploys a MinIO object storage service.
- **postgres.yaml**: Deploys a PostgreSQL database.
- **redpanda.yaml**: Deploys a Redpanda Kafka-compatible streaming platform.

> **Note**: These deployments are for testing purposes. Do not use these configurations in a production environment without further adjustments.

## Deployment Steps

0. Create a namespace:

    ```bash
    kubectl create namespace materialize # or use an existing namespace
    ```

1. Deploy the services to your Kubernetes cluster:

    ```bash
    kubectl apply -f minio.yaml
    kubectl apply -f postgres.yaml
    kubectl apply -f redpanda.yaml
    ```

2. Monitor the deployments to ensure the pods are running:

    ```bash
    kubectl get pods -w
    ```

3. (Optional) Create a bucket in MinIO:

    ```bash
    kubectl exec -it minio-123456-abcdef -n materialize -- /bin/sh
    mc alias set local http://localhost:9000 minio minio123
    mc mb local/bucket
    ```

## Optional: Node labels for ephemeral storage

When running Materialize locally on Kubernetes (e.g., Docker Desktop, Minikube, Kind), specific node labels need to be added to ensure that pods are scheduled correctly. These labels are required for the pod to satisfy node affinity rules defined in the deployment.

```sh
kubectl get nodes --show-labels
```

If the required labels are missing, add them to the node by running:

```sh
kubectl label node <node-name> materialize.cloud/disk=true
kubectl label node <node-name> workload=materialize-instance
```

After adding the labels, verify that they were successfully applied by running the following command again:

```sh
kubectl get nodes --show-labels
```

## Metrics service

The metrics service is required for the `environmentd` pod to function correctly.

```
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
```

If you get TLS errors, you can disable TLS by editing the `metrics-server` deployment:

```sh
kubectl edit deployment metrics-server -n kube-system
```

Look for the args section in the deployment and add the following:

```yml
    args:
    - --kubelet-insecure-tls
    - --kubelet-preferred-address-types=InternalIP,Hostname,ExternalIP
```

Get the metrics server pod status:

```sh
kubectl get pods -n kube-system
```

## Notes

- Adjust resource requests and limits based on your cluster's capacity.
- Ensure your Kubernetes cluster has enough resources to run these services.
- The `minio.yaml`, `postgres.yaml`, and `redpanda.yaml` configurations are minimal examples suitable for local testing environments.
