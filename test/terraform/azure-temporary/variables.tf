# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

variable "subscription_id" {
  description = "The ID of the Azure subscription"
  type        = string
  default     = "9bc1ad3f-3401-42a3-99cd-7faeeb51e059"
}

variable "resource_group_name" {
  description = "The name of the resource group which will be created"
  type        = string
  default     = "mz-tf-test-rg"
}

variable "location" {
  description = "The location of the Azure resources"
  type        = string
  default     = "eastus2"
}

variable "name_prefix" {
  description = "Prefix for resource names"
  type        = string
  default     = "mz-tf-test"
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default = {
    environment = "dev"
    managed_by  = "terraform"
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

variable "environmentd_version" {
  description = "Materialize environmentd version tag"
  type        = string
  default     = "v26.0.0-beta.1"
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

variable "k8s_apiserver_authorized_networks" {
  description = "List of authorized IP ranges that can access the Kubernetes API server"
  type        = list(string)
  default     = ["0.0.0.0/0"]
  nullable    = true

  validation {
    condition = (
      var.k8s_apiserver_authorized_networks == null ||
      alltrue([
        for cidr in var.k8s_apiserver_authorized_networks :
        can(cidrhost(cidr, 0))
      ])
    )
    error_message = "All k8s_apiserver_authorized_networks must be valid CIDR blocks."
  }
}
