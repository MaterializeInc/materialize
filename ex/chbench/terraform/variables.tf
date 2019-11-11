# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

variable "docker_username" {
  type = string
  default = "<detect>"
}

variable "docker_password" {
  type = string
  default = "<detect>"
}
