#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

set -xeo pipefail

# Volume group name
VG_NAME="instance-store-vg"

# Function to detect cloud provider
detect_cloud_provider() {
    # Check for AWS
    if curl -s -m 5 http://169.254.169.254/latest/meta-data/ >/dev/null; then
        echo "aws"
        return
    fi

    # Default to generic
    echo "generic"
}

# Cloud provider-specific device detection
find_nvme_devices() {
    local cloud=$1
    local nvme_devices=()

    case $cloud in
        aws)
            # Handle both standard Linux and Bottlerocket paths
            if [ -d "/.bottlerocket" ]; then
                # Bottlerocket specific path
                BOTTLEROCKET_ROOT="/.bottlerocket/rootfs"
                mapfile -t SSD_NVME_DEVICE_LIST < <(lsblk --json --output-all | jq -r '.blockdevices[] | select(.model // empty | contains("Amazon EC2 NVMe Instance Storage")) | .path')
                for device in "${SSD_NVME_DEVICE_LIST[@]}"; do
                    nvme_devices+=("$BOTTLEROCKET_ROOT$device")
                done
            else
                # Standard EC2 instances
                mapfile -t nvme_devices < <(lsblk --json --output-all | jq -r '.blockdevices[] | select(.model // empty | contains("Amazon EC2 NVMe Instance Storage")) | .path')
            fi
            ;;
        # Add more cloud providers here
        *)
            # Generic approach - find all NVMe devices that are not mounted and don't have children (partitions)
            mapfile -t nvme_devices < <(lsblk --json --output-all | jq -r '.blockdevices[] | select(.name | startswith("nvme")) | select(.mountpoint == null and (.children | length == 0)) | .path')
            ;;
    esac

    echo "${nvme_devices[@]}"
}

# Initialize LVM on discovered devices
setup_lvm() {
    local -a devices=("$@")

    if [ ${#devices[@]} -eq 0 ]; then
        echo "No suitable NVMe devices found"
        exit 1
    fi

    echo "Found devices: ${devices[*]}"

    # Check if volume group already exists
    if vgs | grep -q "$VG_NAME"; then
        echo "Volume group $VG_NAME already exists"
        return 0
    fi

    # Create physical volumes
    for device in "${devices[@]}"; do
        if ! pvs | grep -q "$device"; then
            echo "Creating physical volume on $device"
            pvcreate "$device"
        fi
    done

    # Create volume group with all devices
    echo "Creating volume group $VG_NAME"
    vgcreate "$VG_NAME" "${devices[@]}"

    echo "LVM setup completed successfully"
    return 0
}

# Main execution
echo "Starting NVMe disk configuration..."

# Detect cloud provider
CLOUD_PROVIDER=$(detect_cloud_provider)
echo "Detected cloud provider: $CLOUD_PROVIDER"

# Find NVMe devices
mapfile -t NVME_DEVICES < <(find_nvme_devices "$CLOUD_PROVIDER")

# Setup LVM
if setup_lvm "${NVME_DEVICES[@]}"; then
    echo "NVMe disk configuration completed successfully"
    # Call taint management script to remove the taint
    /usr/local/bin/manage-taints.sh remove

    # Keep the container running
    echo "Setup complete. Container will now stay running for monitoring purposes."
    while true; do
        sleep 3600
    done
else
    echo "NVMe disk configuration failed"
    exit 1
fi
