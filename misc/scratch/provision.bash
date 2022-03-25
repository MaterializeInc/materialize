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

# Progress notes are reported to this directory.
mkdir /opt/provision

# Step 1. Update APT repositories.
apt-get update

# Step 2. Install Docker and grant the `ubuntu` user permission to access the
# Docker daemon.
apt-get install -y docker.io docker-compose python3-venv
adduser ubuntu docker
touch /opt/provision/docker-installed

# Step 3. Install Materialize build prerequisites. These are just a convenience
# for manual debugging/development.
apt-get install -y cmake g++
sudo -u ubuntu sh -c "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -q -y"
touch /opt/provision/materialize-build-deps-installed

# Step 4. Install the AWS CLI. Again, this is just a convenience for manual
# debugging/development.
apt-get install unzip
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" > awscliv2.zip
unzip awscliv2.zip
aws/install
rm -r aws awscliv2.zip
touch /opt/provision/awscli-installed

# Step 5. Install the psql CLI
apt-get install -y postgresql-client
touch /opt/provision/psql-installed

touch /opt/provision/done
