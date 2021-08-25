#!/bin/bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Install necessary prerequisites for a remote EC2 host to run Materialize demos, load tests, etc.

# From https://docs.docker.com/engine/install/ubuntu/
apt update && apt install -y apt-transport-https ca-certificates curl gnupg lsb-release python3-venv python3-pip
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo \
  "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
apt update && apt install -y docker-ce docker-ce-cli containerd.io docker-compose

# Allow using docker as non-root.
usermod -aG docker ubuntu

# Install prerequisites for building Materialize
sudo -u ubuntu sh -c "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -q -y"
apt install -y cmake

touch /DONE
