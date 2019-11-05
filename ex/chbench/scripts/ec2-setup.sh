#! /usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

# EXPERIMENTAL script to set up an EC2 instance to run a load test.
#
# Assumes the following:
# * Instance type `m5ad.4xlarge`, which has two local SSDs
# * Ubuntu 18.04 AMI

set -xeo pipefail

# config
# TODO: automate this somehow
swap_partition="/dev/nvme0n1"
docker_device="/dev/nvme1n1"
docker_mount="/mnt/docker"
docker_dir="/var/lib/docker"

if [[ $USER != "root" ]]; then
    echo Please re-run this script as root
    exit 1
fi

apt update -y -qq
apt upgrade -y -qq

echo "Setting up Docker"
apt install linux-aws -y -qq
docker=$(command -v docker || true)
if [[ -z $docker ]]; then
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
    add-apt-repository \
       "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
       $(lsb_release -cs) \
       stable"
    apt install docker-ce docker-ce-cli containerd.io -qy
fi
docker_compose=$(command -v docker-compose || true)
if [[ -z $docker_compose ]]; then
    curl -L "https://github.com/docker/compose/releases/download/1.24.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    chmod 755 /usr/local/bin/docker-compose
fi
groupadd -f docker
usermod -aG docker ubuntu

# Mount local SSD as swap
existing_swap=$(grep swap /etc/fstab || true)
if [[ -z $existing_swap ]]; then
    # TODO(cuongdo): this will not work if the block device names change
    echo "Enabling swap"
    mkswap $swap_partition
    echo "$swap_partition none swap sw 0 0" >> /etc/fstab
    swapon --all
fi

mkdir -p /etc/docker
cp daemon.json /etc/docker/

# Mount EBS volume as Docker data directory

if [[ ! -L $docker_dir ]]; then
    # shellcheck disable=SC2174
    mkdir -p -m 777 $docker_mount
    chown -R "$USER":docker $docker_mount
    docker_partition="${docker_device}p1"

    # partition
    if [[ ! -b $docker_partition ]]; then
        echo 'type=83' | sudo sfdisk $docker_device
    fi

    # make ext4 file system
    ext4=$(file -sL $docker_partition | grep ext4 || true)
    if [[ -z $ext4 ]]; then
        while [[ ! -b $docker_partition ]]; do
            # Work around a race with fdisk.
            echo "Waiting for $docker_partition to become available"
            sleep 1
        done
        mkfs.ext4 $docker_partition
    fi

    # create fstab entry
    mount_point=$(grep $docker_partition /etc/fstab || true)
    if [[ -z $mount_point ]]; then
        echo "$docker_partition /mnt/docker ext4 defaults 0 0" >> /etc/fstab
    fi

    mount /mnt/docker || true
    service docker stop || true
    rm -rf $docker_dir
    ln -sf $docker_mount $docker_dir
    service docker start
fi

git config --global pull.rebase true
git config --global rebase.autoStash true
