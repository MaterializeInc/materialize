#!/bin/bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Install necessary prerequisites for a remote EC2 host to run Materialize
# demos, load tests, etc.

set -euo pipefail

# Install APT dependencies.
apt-get update
apt-get install -y \
    cmake \
    docker.io \
    docker-compose \
    g++ \
    postgresql \
    python3-venv \
    unzip

# Install Rust.
sudo -u ubuntu sh -c "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -q -y"

# Install the AWS CLI.
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" > awscliv2.zip
unzip awscliv2.zip
aws/install
rm -r aws awscliv2.zip

# Allow the Ubuntu user to access the Docker daemon.
adduser ubuntu docker

# Configure PostgreSQL for passwordless use by the `ubuntu` user. Both stash and
# persist are backed by postgres when doing local development, so seems
# reasonable to have around. Set it up so it's easy to connect to and export the
# MZDEV_POSTGRES env var with connection details.
apt-get install -y postgresql
sudo -u postgres createuser ubuntu
sudo -u postgres createdb ubuntu -O ubuntu
echo "export MZDEV_POSTGRES=postgresql://ubuntu@%2Fvar%2Frun%2Fpostgresql" >> /home/ubuntu/.bashrc

# Report that provisioning has completed.
mkdir /opt/provision
touch /opt/provision/done
