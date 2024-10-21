# Materialize Helm Charts

This directory contains Helm charts for Materialize components in Kubernetes.

## Available Charts

1. **Materialize Operator**: Deploys the Materialize Kubernetes Operator for managing Materialize environments.

2. **Materialize Environmentd**: Deploys Materialize environments as defined by the Materialize Operator.

## Prerequisites

- Kubernetes 1.19+
- Helm 3.2.0+

## Usage

```bash
helm install [RELEASE_NAME] ./[CHART_NAME]
```

Example:
```bash
helm install mz-operator ./operator
```

Once the operator is installed, you can create Materialize environments using the `environmentd` chart.

Example:
```bash
helm install mz-environmentd ./environmentd
```
