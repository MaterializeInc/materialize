#!/usr/bin/env bash

# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

set -euxo pipefail

echo "Updating packages"
apt-get update -qy
apt-get upgrade -qy

echo "Setting up Docker"
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu xenial stable"
apt-get install docker-ce docker-ce-cli containerd.io -qy
mv /tmp/packer/docker-daemon.json /etc/docker/daemon.json
usermod -aG docker ubuntu

echo "Setting up Docker Compose"
curl -L "https://github.com/docker/compose/releases/download/1.24.1/docker-compose-Linux-x86_64" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

echo "Configuring disks"
cat >> /etc/fstab <<EOF
/dev/nvme0n1 none        swap sw       0 0
/dev/nvme1n1 /mnt/docker ext4 defaults 0 0
EOF
mv /tmp/packer/fmtdisks.service /etc/systemd/system/fmtdisks.service
mkdir -p /usr/local/libexec
mv /tmp/packer/fmtdisks.sh /usr/local/libexec/fmtdisks.sh
chmod +x /usr/local/libexec/fmtdisks.sh
systemctl enable fmtdisks.service

echo "Setting defaults"
git config --global pull.rebase true
git config --global rebase.autoStash true

echo "Preparing to auto-sync SSH keys"
curl -fsSL https://github.com/ernoaapa/fetch-ssh-keys/releases/download/v1.1.7/linux_amd64_fetch-ssh-keys > /usr/local/bin/fetch-ssh-keys
chmod +x /usr/local/bin/fetch-ssh-keys
echo "AuthorizedKeysFile .ssh/authorized_keys .ssh/authorized_keys2 .ssh/auto_authorized_keys" >> /etc/ssh/sshd_config
mv /tmp/packer/ssh-keysync /usr/local/bin/ssh-keysync
chmod +x /usr/local/bin/ssh-keysync
mv /tmp/packer/ssh-keysync.{service,timer} /etc/systemd/user
systemctl enable --user --global ssh-keysync.timer
# Enable linger so that we start fetching keys for ubuntu immediately on boot,
# because our AuthorizedKeysFile configuration prevents cloud-init from
# installing the SSH key selected in the AWS console.
#
# See: https://bugs.launchpad.net/cloud-init/+bug/1404060
loginctl enable-linger ubuntu
