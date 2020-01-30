# Copyright 2019-2020 Materialize Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

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
