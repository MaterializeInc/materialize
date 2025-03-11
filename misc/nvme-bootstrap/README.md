# OpenEBS NVMe Bootstrap for Materialize

This guide helps you set up and configure NVMe instance store volumes for optimal Materialize performance on Kubernetes. The solution provides automatic detection and configuration of NVMe devices, making them available to Materialize through OpenEBS LVM storage classes.

> **WARNING:** This setup **automatically partitions and formats NVMe instance store volumes**. Make sure your nodes have NVMe storage (`r6gd.2xlarge`, `r7gd.2xlarge`), and verify backups before proceeding with the setup.

## Overview

Materialize requires fast, locally-attached NVMe storage for optimal performance. This solution:

1. Automatically detects NVMe instance store devices on your nodes
2. Creates an LVM volume group from these devices
3. Configures OpenEBS LVM Local-PV to provision persistent volumes from this storage
4. Makes high-performance storage available to Materialize

## Prerequisites

- AWS account with permissions to create EC2 instances with NVMe storage
- Kubernetes cluster with nodes that have NVMe instance store volumes
  - **Important**: You must use instance types with NVMe storage (those with the "d" suffix)
  - Recommended instance types: `r6gd.2xlarge`, `r7gd.2xlarge` (not `r8g.2xlarge` which lacks NVMe storage)
  - When using Bottlerocket OS, additional configuration is handled automatically
- Tools required:
  - [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
  - [Helm](https://helm.sh/docs/intro/install/) (v3.2.0+)
  - [Docker](https://docs.docker.com/get-docker/) (for building the container)

## Automated Setup with Terraform

If you're using the [Materialize AWS Terraform module](https://github.com/MaterializeInc/terraform-aws-materialize), you can enable NVMe bootstrap by configuring:

```hcl
module "materialize" {
  source = "git::https://github.com/MaterializeInc/terraform-aws-materialize.git"

  # Use an instance type with NVMe storage
  node_group_instance_types = ["r6gd.2xlarge"]
  node_group_ami_type       = "BOTTLEROCKET_ARM_64"
  enable_nvme_storage       = true

  install_materialize_operator = true

  # Other module parameters...
}
```

The module handles creating the appropriate storage class and configuring Materialize to use it.

## Manual Setup

**TODO:** The following steps will be eventually automated in the Terraform modules for Materialize but can be done manually for now.

If you're setting up manually or need to customize the configuration, follow these steps:

### Step 1: Build and Push the Container Image

> Note: This is temporary and will be replaced with a pre-built image which can be pulled from a public registry.

```bash
# Clone the Materialize repository
git clone https://github.com/MaterializeInc/materialize.git
cd materialize

# Navigate to the container directory
cd misc/nvme-bootstrap/container

# Build the image
docker build -t your-registry/nvme-bootstrap:latest .

# Push to your registry
docker push your-registry/nvme-bootstrap:latest
```

### Step 2: Deploy the NVMe Bootstrap Components

```bash
# Navigate to the Kubernetes manifests directory
cd misc/nvme-bootstrap/kubernetes

# Apply RBAC resources for the bootstrap component
kubectl apply -f rbac.yaml

# Deploy the DaemonSet (update the image reference if needed)
kubectl apply -f daemonset.yaml

# Get the pod logs to monitor the setup
kubectl logs -n kube-system -l app=nvme-disk-setup

# Wait for the pods to be ready
kubectl -n kube-system wait --for=condition=Ready pods -l app=nvme-disk-setup --timeout=120s
```

The DaemonSet will:

1. Run on all nodes in your cluster
2. Detect available NVMe devices
3. Create the "instance-store-vg" volume group
4. Make the storage available for OpenEBS

### Step 3: Install OpenEBS

OpenEBS provides the CSI driver that interfaces with LVM to provide persistent storage:

```bash
# Add the OpenEBS Helm repository
helm repo add openebs https://openebs.github.io/charts
helm repo update

# Create namespace for OpenEBS
kubectl create namespace openebs

# Install OpenEBS with only the necessary components
helm install openebs openebs/openebs \
  --namespace openebs \
  --set engines.replicated.mayastor.enabled=false
```

Verify the installation:

```bash
# Check if the LVM controller is running
kubectl get pods -n openebs -l role=openebs-lvm
```

### Step 4: Create and Test the Storage Class

```bash
# TODO: remove this step once the Terraform module handles this
# Create the StorageClass
kubectl apply -f storageclass.yaml

# Deploy a test PVC and Pod to verify functionality
kubectl apply -f test-pvc.yaml

# Check if the PVC is bound
kubectl get pvc test-lvm-pvc
```

A successful test shows your storage class is working correctly.

To clean up the test resources:

```bash
# Delete the test PVC
kubectl delete -f test-pvc.yaml
```

### Step 5: Configure Materialize to Use the Storage Class

When installing Materialize, provide the storage class configuration:

```bash
# Create Helm values file
cat > materialize-values.yaml << EOF
storage:
  storageClass:
    create: true
    name: "openebs-lvm-instance-store-ext4"
EOF

# Install Materialize with the storage configuration
helm install my-materialize-operator materialize/materialize-operator \
  --namespace materialize \
  --create-namespace \
  --set observability.podMetrics.enabled=true \
  --values materialize-values.yaml
```

This configures Materialize to use the NVMe-backed storage class for its persistent storage needs.

If you are doing this using the [Materialize Helm Terraform module](https://github.com/materializeInc/terraform-helm-materialize), you can set the `storageClass` field in the `materialize` module to `openebs-lvm-instance-store-ext4`.

```
...
    storage = {
      storageClass = {
        create = true
        name   = "openebs-lvm-instance-store-ext4"
        provisioner = "local.csi.openebs.io"
        parameters = {
          storage  = "lvm"
          fsType   = "ext4"
          volgroup = "instance-store-vg"
        }
      }
    }
...
```

## Verifying the Setup

To verify your NVMe bootstrap setup is working correctly:

```bash
# Check the NVMe setup logs
kubectl logs -n kube-system -l app=nvme-disk-setup

# Check that PVCs can be created with the storage class
kubectl get pvc -A | grep openebs-lvm-instance-store-ext4
```

## Troubleshooting

### Common Issues and Solutions

#### No NVMe Devices Found

**Symptom**: The bootstrap logs show "No suitable NVMe devices found"

**Solution**:
- Verify you're using instance types with NVMe storage (with "d" suffix)
- Check the instance type with:
  ```bash
  kubectl debug node/$NODE_NAME -it --image=busybox -- cat /host/etc/ec2_instance_type
  ```
- If using AWS, ensure you're using types like r6gd.2xlarge, not r6g.2xlarge

#### Pod Fails to Create Storage

**Symptom**: LVM setup fails or PVCs remain in Pending status

**Solution**:
- Check if OpenEBS components are running:
  ```bash
  kubectl get pods -n openebs
  ```
- Verify the volume group exists:
  ```bash
  kubectl debug node/$NODE_NAME -it --image=ubuntu -- vgs
  ```
- Check OpenEBS logs:
  ```bash
  kubectl logs -n openebs -l role=openebs-lvm
  ```

#### Permission Issues

**Symptom**: Permission denied errors in logs

**Solution**:
- Verify RBAC resources are correctly applied:
  ```bash
  kubectl get clusterrole node-taint-manager
  kubectl get clusterrolebinding nvme-setup-taint-binding
  ```
- Check the service account:
  ```bash
  kubectl get serviceaccount nvme-setup-sa -n kube-system
  ```
