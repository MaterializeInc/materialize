#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Install necessary prerequisites for a remote Hetzner Cloud host to run
# Materialize demos, load tests, etc.

set -euo pipefail

# Hetzner images default to root; create ubuntu user and copy SSH keys
# immediately so SSH access works while the rest of provisioning runs.
id -u ubuntu &>/dev/null || useradd -m -s /bin/bash ubuntu
echo "ubuntu ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/ubuntu
mkdir -p /home/ubuntu/.ssh
cp /root/.ssh/authorized_keys /home/ubuntu/.ssh/authorized_keys
chown -R ubuntu:ubuntu /home/ubuntu/.ssh
chmod 700 /home/ubuntu/.ssh
chmod 600 /home/ubuntu/.ssh/authorized_keys

ARCH=$(uname -m)
ARCH_GO=$(echo "$ARCH" | sed -e "s/aarch64/arm64/" -e "s/x86_64/amd64/")
DOCKER_VERSION=29.1.3
DOCKER_COMPOSE_VERSION=2.37.3
NODE_VERSION=22.22.1

# Everything runs in parallel — no serial APT bottleneck.

# Docker: static binary
(curl -fsSL "https://download.docker.com/linux/static/stable/$ARCH/docker-${DOCKER_VERSION}.tgz" \
    | tar -xz -C /usr/bin --strip-components=1 docker/
mkdir -p /usr/local/lib/docker/cli-plugins
curl -fsSL "https://github.com/docker/compose/releases/download/v${DOCKER_COMPOSE_VERSION}/docker-compose-linux-$ARCH" \
    -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
groupadd -f docker
usermod -aG docker ubuntu
cat > /etc/systemd/system/docker.service <<'UNIT'
[Unit]
Description=Docker
After=network-online.target
[Service]
ExecStart=/usr/bin/dockerd
Restart=on-failure
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable --now docker.service) &

# Node.js: static binary (skip nodesource APT repo)
(curl -fsSL "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-x64.tar.xz" \
    | tar -xJ -C /usr/local --strip-components=1 --no-same-owner) &

# APT packages
(export DEBIAN_FRONTEND=noninteractive
apt-get update -qq >/dev/null
apt-get install -y -qq --no-install-recommends \
    build-essential \
    cmake \
    g++ \
    libclang-dev \
    lld \
    postgresql-client \
    python3-dev \
    python3-venv \
    screen \
    unzip) &

# Rust
(sudo -u ubuntu sh -c "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -q -y" >/dev/null 2>&1) &

# uv
(curl -fsSL https://astral.sh/uv/install.sh | sudo -u ubuntu sh >/dev/null 2>&1) &

# kubectl + kind + k9s
(curl -fsSL "https://dl.k8s.io/release/v1.24.3/bin/linux/$ARCH_GO/kubectl" -o /usr/local/bin/kubectl
chmod +x /usr/local/bin/kubectl) &
(curl -fsSL "https://kind.sigs.k8s.io/dl/v0.29.0/kind-linux-$ARCH_GO" -o /usr/local/bin/kind
chmod +x /usr/local/bin/kind) &
(curl -fsSL "https://github.com/derailed/k9s/releases/download/v0.50.18/k9s_Linux_$ARCH_GO.tar.gz" \
    | tar xzf - -C /usr/local/bin k9s
chmod +x /usr/local/bin/k9s) &

wait

# npm install needs Node.js to be available (installed above)
NPM_CONFIG_UPDATE_NOTIFIER=false npm install -g @anthropic-ai/claude-code @openai/codex >/dev/null 2>&1

# Ensure cargo, uv, and local bins are on PATH for all sessions
cat > /etc/profile.d/scratch-path.sh <<'PATHSETUP'
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
PATHSETUP
# Also set in /etc/environment for non-interactive SSH commands
sed -i 's|^PATH="\(.*\)"|PATH="/home/ubuntu/.cargo/bin:/home/ubuntu/.local/bin:\1"|' /etc/environment

# Set up shell completions for bin/scratch and bin/mzcompose.
# The repo will be cloned to ~/materialize by the scratch tool after provisioning.
cat >> /home/ubuntu/.bashrc <<'BASH_COMP'
source /etc/profile.d/scratch-path.sh
[ -f ~/materialize/misc/completions/bash/_scratch ] && source ~/materialize/misc/completions/bash/_scratch
[ -f ~/materialize/misc/completions/bash/_mzcompose ] && source ~/materialize/misc/completions/bash/_mzcompose
BASH_COMP
cat >> /home/ubuntu/.zshrc <<'ZSH_COMP'
source /etc/profile.d/scratch-path.sh
fpath=(~/materialize/misc/completions/zsh $fpath)
ZSH_COMP
chown -R ubuntu:ubuntu /home/ubuntu/.bashrc /home/ubuntu/.zshrc

# Report that provisioning has completed.
mkdir /opt/provision
touch /opt/provision/done
