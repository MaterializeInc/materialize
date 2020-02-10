# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

output "instance_id" {
    value = "${aws_instance.chbench.id}"
}

output "instance_ip" {
    value = "${aws_instance.chbench.public_ip}"
}

output "instance_username" {
    value = "ubuntu"
}

output "ssh_command" {
    value = "ssh ubuntu@${aws_instance.chbench.public_ip}"
}
