# Bootstrap LVM

This bootstrap container provides a solution for configuring local instance store volumes on cloud instances.

## Supported Cloud Providers

- **AWS**: Detects Amazon EC2 NVMe Instance Storage devices, with special handling for Bottlerocket OS
- **GCP**: Detects Google Cloud local SSD devices at the `/dev/disk/by-id/google-local-ssd-*` path
- **Azure**: _To be implemented_

## Usage

```bash
Usage: ./configure-disks.sh [options]
Options:
  --cloud-provider, -c PROVIDER   Specify cloud provider (aws, gcp, azure, generic)
  --vg-name, -v NAME             Specify volume group name (default: instance-store-vg)
  --help, -h                     Show this help message
```

Examples:

```bash
# Specify GCP as the cloud provider
./configure-disks.sh --cloud-provider gcp

# Use a custom volume group name
./configure-disks.sh --vg-name custom-vg-name -c aws
```

## Kubernetes Integration

This solution is designed to be deployed as a Kubernetes DaemonSet to automatically configure instance store volumes on nodes.

### Deployment Process

1. The DaemonSet runs on nodes with the `materialize.cloud/disk=true` label
2. The init container runs with privileged access to configure the disks
3. Once disks are configured, the node taint `disk-unconfigured` is removed
4. Pods can then be scheduled on the node

### Terraform Example

```hcl
resource "kubernetes_daemonset" "disk_setup" {
  count = var.enable_disk_setup ? 1 : 0
  metadata {
    name      = "disk-setup"
    namespace = kubernetes_namespace.disk_setup[0].metadata[0].name
    labels = {
      "app.kubernetes.io/managed-by" = "terraform"
      "app.kubernetes.io/part-of"    = "materialize"
      "app"                          = "disk-setup"
    }
  }
  spec {
    selector {
      match_labels = {
        app = "disk-setup"
      }
    }
    template {
      metadata {
        labels = {
          app = "disk-setup"
        }
      }
      spec {
        security_context {
          run_as_non_root = false
          run_as_user     = 0
          fs_group        = 0
          seccomp_profile {
            type = "RuntimeDefault"
          }
        }
        affinity {
          node_affinity {
            required_during_scheduling_ignored_during_execution {
              node_selector_term {
                match_expressions {
                  key      = "materialize.cloud/disk"
                  operator = "In"
                  values   = ["true"]
                }
              }
            }
          }
        }
        # Node taint to prevent regular workloads from being scheduled until disks are configured
        toleration {
          key      = "disk-unconfigured"
          operator = "Exists"
          effect   = "NoSchedule"
        }
        # Use host network and PID namespace
        host_network = true
        host_pid     = true
        init_container {
          name    = "disk-setup"
          image   = var.disk_setup_image
          command = ["/usr/local/bin/configure-disks.sh"]
          args    = ["--cloud-provider", var.cloud_provider]
          resources {
            limits = {
              memory = "128Mi"
            }
            requests = {
              memory = "128Mi"
              cpu    = "50m"
            }
          }
          security_context {
            privileged  = true
            run_as_user = 0
          }
          env {
            name = "NODE_NAME"
            value_from {
              field_ref {
                field_path = "spec.nodeName"
              }
            }
          }
          # Mount all necessary host paths
          volume_mount {
            name       = "dev"
            mount_path = "/dev"
          }
          volume_mount {
            name       = "host-root"
            mount_path = "/host"
          }
        }
        # Taint management container
        init_container {
          name    = "taint-management"
          image   = var.disk_setup_image
          command = ["/usr/local/bin/manage-taints.sh", "remove"]
          resources {
            limits = {
              memory = "64Mi"
            }
            requests = {
              memory = "64Mi"
              cpu    = "10m"
            }
          }
          security_context {
            run_as_user = 0
          }
          env {
            name = "NODE_NAME"
            value_from {
              field_ref {
                field_path = "spec.nodeName"
              }
            }
          }
        }
        container {
          name  = "pause"
          image = "gcr.io/google_containers/pause:3.2"
          resources {
            limits = {
              memory = "8Mi"
            }
            requests = {
              memory = "8Mi"
              cpu    = "1m"
            }
          }
          security_context {
            allow_privilege_escalation = false
            read_only_root_filesystem  = true
            run_as_non_root            = true
            run_as_user                = 65534
          }
        }
        volume {
          name = "dev"
          host_path {
            path = "/dev"
          }
        }
        volume {
          name = "host-root"
          host_path {
            path = "/"
          }
        }
        service_account_name = kubernetes_service_account.disk_setup[0].metadata[0].name
      }
    }
  }
}

# Service account for the disk setup daemon
resource "kubernetes_service_account" "disk_setup" {
  count = var.enable_disk_setup ? 1 : 0
  metadata {
    name      = "disk-setup"
    namespace = kubernetes_namespace.disk_setup[0].metadata[0].name
  }
}

# RBAC role to allow removing taints
resource "kubernetes_cluster_role" "disk_setup" {
  count = var.enable_disk_setup ? 1 : 0
  metadata {
    name = "disk-setup"
  }
  rule {
    api_groups = [""]
    resources  = ["nodes"]
    verbs      = ["get", "patch"]
  }
}

# Bind the role to the service account
resource "kubernetes_cluster_role_binding" "disk_setup" {
  count = var.enable_disk_setup ? 1 : 0
  metadata {
    name = "disk-setup"
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.disk_setup[0].metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.disk_setup[0].metadata[0].name
    namespace = kubernetes_namespace.disk_setup[0].metadata[0].name
  }
}
```
