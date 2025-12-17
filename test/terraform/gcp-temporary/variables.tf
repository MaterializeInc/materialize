# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

variable "project_id" {
  description = "The ID of the project where resources will be created"
  type        = string
  default     = "materialize-ci"
}

variable "region" {
  description = "The region where resources will be created"
  type        = string
  default     = "us-east1"
}

variable "name_prefix" {
  description = "Prefix to be used for resource names"
  type        = string
  default     = "gcp-test-dev"
}

variable "labels" {
  description = "Labels to apply to resources created"
  type        = map(string)
  default = {
    environment = "dev"
    terraform   = "true"
  }
}

variable "operator_version" {
  description = "Version of the Materialize operator"
  type        = string
  default     = "v26.0.0-beta.1"
}

variable "orchestratord_version" {
  description = "Version of orchestratord (optional override)"
  type        = string
  default     = null
}

variable "force_rollout" {
  description = "UUID to force a rollout"
  type        = string
  default     = "00000000-0000-0000-0000-000000000001"
}

variable "request_rollout" {
  description = "UUID to request a rollout"
  type        = string
  default     = "00000000-0000-0000-0000-000000000001"
}

variable "license_key" {
  description = "Materialize license key"
  type        = string
  default     = ""
  sensitive   = true
}

variable "environmentd_version" {
  description = "Materialize environmentd version tag"
  type        = string
  default     = "v26.0.0-beta.1"
}

variable "k8s_apiserver_authorized_networks" {
  description = "The CIDR blocks that are allowed to reach the Kubernetes master endpoint"
  type = list(object({
    cidr_block   = string
    display_name = string
  }))
  default = [{
    cidr_block   = "0.0.0.0/0"
    display_name = "Default Placeholder for authorized networks"
  }]
  nullable = false

  validation {
    condition = alltrue([
      for network in var.k8s_apiserver_authorized_networks : can(cidrhost(network.cidr_block, 0))
    ])
    error_message = "All k8s_apiserver_authorized_networks must be valid CIDR notation (e.g., '10.0.0.0/8' or '0.0.0.0/0')."
  }
}
