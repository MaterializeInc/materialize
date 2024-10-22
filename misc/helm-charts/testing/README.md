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
    kubectl create namespace materialize-environment # or use an existing namespace
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

## Notes

- Adjust resource requests and limits based on your cluster's capacity.
- Ensure your Kubernetes cluster has enough resources to run these services.
- The `minio.yaml`, `postgres.yaml`, and `redpanda.yaml` configurations are minimal examples suitable for local testing environments.
