#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

set -xeuo pipefail

VG_NAME="instance-store-vg"
CLOUD_PROVIDER=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --cloud-provider|-c)
      CLOUD_PROVIDER="$2"
      shift 2
      ;;
    --vg-name|-v)
      VG_NAME="$2"
      shift 2
      ;;
    --help|-h)
      echo "Usage: $0 [options]"
      echo "Options:"
      echo "  --cloud-provider, -c PROVIDER   Specify cloud provider (aws, gcp, azure, generic)"
      echo "  --vg-name, -v NAME     Specify volume group name (default: instance-store-vg)"
      echo "  --help, -h             Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

detect_cloud_provider() {
    # Only attempt detection if not explicitly provided
    if [[ -n "$CLOUD_PROVIDER" ]]; then
        echo "$CLOUD_PROVIDER"
        return
    fi

    # Check for AWS
    if curl -s -m 5 --fail http://169.254.169.254/latest/meta-data/ >/dev/null 2>&1; then
        echo "aws"
        return
    fi

    # Check for GCP
    if curl -s -m 5 --fail -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/ >/dev/null 2>&1; then
        echo "gcp"
        return
    fi

    # Check for Azure
    # TODO: Implement Azure detection

    # Default to generic if detection fails
    echo "generic"
}

find_aws_bottlerocket_devices() {
    local nvme_devices=()
    local BOTTLEROCKET_ROOT="/.bottlerocket/rootfs"

    mapfile -t SSD_NVME_DEVICE_LIST < <(lsblk --json --output-all | \
        jq -r '.blockdevices[] | select(.model // empty | contains("Amazon EC2 NVMe Instance Storage")) | .path')

    for device in "${SSD_NVME_DEVICE_LIST[@]}"; do
        nvme_devices+=("$BOTTLEROCKET_ROOT$device")
    done

    echo "${nvme_devices[@]}"
}

find_aws_standard_devices() {
    lsblk --json --output-all | \
        jq -r '.blockdevices[] | select(.model // empty | contains("Amazon EC2 NVMe Instance Storage")) | .path'

    if [ $? -ne 0 ]; then
        nvme list | grep "Amazon EC2 NVMe Instance Storage" | awk '{print $1}'
    fi
}

find_aws_devices() {
    local nvme_devices=()

    # Check if running in Bottlerocket
    if [[ -d "/.bottlerocket" ]]; then
        mapfile -t nvme_devices < <(find_aws_bottlerocket_devices)
    else
        mapfile -t nvme_devices < <(find_aws_standard_devices)
    fi

    echo "${nvme_devices[@]}"
}

find_gcp_devices() {
    local ssd_devices=()

    # Check for Google Local SSD devices
    local devices
    devices=$(find /dev/disk/by-id/ -name "google-local-ssd-*" 2>/dev/null || true)

    if [ -n "$devices" ]; then
        while read -r device; do
            ssd_devices+=("$device")
        done <<< "$devices"
    fi

    echo "${ssd_devices[@]}"
}

find_generic_devices() {
    lsblk --json --output-all | \
        jq -r '.blockdevices[] | select(.name | startswith("nvme")) | select(.mountpoint == null and (.children | length == 0)) | .path'
}

find_nvme_devices() {
    local cloud=$1
    local nvme_devices=()

    case $cloud in
        aws)
            mapfile -t nvme_devices < <(find_aws_devices)
            ;;
        gcp)
            mapfile -t nvme_devices < <(find_gcp_devices)
            ;;
        # TODO: Add support for Azure
        # azure)
        #     mapfile -t nvme_devices < <(find_azure_devices)
        #     ;;
        *)
            # Generic approach for any other cloud or environment
            mapfile -t nvme_devices < <(find_generic_devices)
            ;;
    esac

    echo "${nvme_devices[@]}"
}

# Initialize LVM on discovered devices
setup_lvm() {
    local -a devices=("$@")

    if [[ ${#devices[@]} -eq 0 ]]; then
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
            pvcreate -f "$device"
        fi
    done

    # Create volume group with all devices
    echo "Creating volume group $VG_NAME"
    vgcreate "$VG_NAME" "${devices[@]}"

    # Display results
    pvs
    vgs

    echo "LVM setup completed successfully"
    return 0
}

echo "Starting NVMe disk configuration..."

# Detect cloud provider
CLOUD_PROVIDER=$(detect_cloud_provider)
echo "Using cloud provider: $CLOUD_PROVIDER"

# Find NVMe devices
mapfile -t NVME_DEVICES < <(find_nvme_devices "$CLOUD_PROVIDER")

# Setup LVM
if setup_lvm "${NVME_DEVICES[@]}"; then
    echo "NVMe disk configuration completed successfully"
    exit 0
else
    echo "NVMe disk configuration failed"
    exit 1
fi
