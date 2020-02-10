# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

provider "aws" {
    region = "us-east-2"
}

resource "aws_instance" "chbench" {
    # See the README for instructions on updating this AMI.
    ami = "ami-0bae027361530dc31"
    instance_type = "r5ad.4xlarge"
    associate_public_ip_address = true
    vpc_security_group_ids = ["${aws_security_group.chbench.id}"]

    tags = {
        Name = "chbench"
    }

    provisioner "remote-exec" {
        inline = [
            "ssh-keyscan github.com >> ~/.ssh/known_hosts",
            "git clone git@github.com:MaterializeInc/materialize.git",
            "mkdir ~/.docker",
        ]
    }

    provisioner "file" {
        content = templatefile("docker-config.tmpl.json", {
            username = data.external.docker_credentials.result["Username"]
            password = data.external.docker_credentials.result["Secret"]
        })
        destination = "~/.docker/config.json"
    }

    provisioner "remote-exec" {
        inline = ["cd materialize/demo/chbench && docker-compose build --pull"]
    }

    connection {
        type = "ssh"
        user = "ubuntu"
        host = "${self.public_ip}"
    }
}

resource "aws_security_group" "chbench" {
  name = "chbench"

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port = "22"
    to_port   = "22"
    protocol  = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port = "3000"
    to_port   = "3000"
    protocol  = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port = "3030"
    to_port   = "3030"
    protocol  = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

data "external" "docker_credentials" {
    program = ["${path.module}/docker-credentials.sh", var.docker_username, var.docker_password]
}
