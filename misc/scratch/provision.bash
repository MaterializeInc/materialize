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
    g++ \
    postgresql \
    python3-venv \
    unzip

# Update ec2-instance-connect scripts to a version that works with OpenSSL 3.
# https://github.com/aws/aws-ec2-instance-connect-config/issues/38
curl -L "https://github.com/aws/aws-ec2-instance-connect-config/archive/refs/tags/1.1.17.zip" > ec2-instance-connect.zip
unzip ec2-instance-connect.zip
cp aws-ec2-instance-connect-config-1.1.17/src/bin/* /usr/share/ec2-instance-connect/
rm -r ec2-instance-connect.zip aws-ec2-instance-connect-config-1.1.17

# Install docker as per the instructions from https://docs.docker.com/engine/install/ubuntu/
sudo apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    lsb-release

sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

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

# Install tools for Kubernetes testing and debugging
## kubectl
sudo sh -c 'curl -L "https://dl.k8s.io/release/v1.24.3/bin/linux/amd64/kubectl" > /usr/local/bin/kubectl'
sudo chmod +x /usr/local/bin/kubectl
## kind
sudo sh -c 'curl -L "https://kind.sigs.k8s.io/dl/v0.14.0/kind-linux-amd64" > /usr/local/bin/kind'
sudo chmod +x /usr/local/bin/kind
## k9s
curl -L 'https://github.com/derailed/k9s/releases/download/v0.26.3/k9s_Linux_x86_64.tar.gz' \
  | tar xzf - k9s
chmod +x k9s
sudo mv k9s /usr/local/bin

# Report that provisioning has completed.
mkdir /opt/provision
touch /opt/provision/done
