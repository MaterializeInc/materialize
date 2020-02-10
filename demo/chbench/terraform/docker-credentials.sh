#!/usr/bin/env bash

# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# docker-credentials.sh — attempts to extract the host's Docker credentials.

set -uo pipefail

if [[ "$#" -ne 2 ]]; then
    echo "usage: $0 OVERRIDE-USERNAME OVERRIDE-PASSWORD" >&2
    exit 1
fi

if [[ "$1" != "<detect>" && "$2" != "<detect>" ]]; then
    echo "{ \"Username\": \"$1\", \"Secret\": \"$2\" }"
elif ! echo "https://index.docker.io/v1/" | docker-credential-desktop get; then
    echo "fatal: unable to determine Docker credentials automatically" >&2
    echo "try: \`terraform apply -var=docker_username=USERNAME -var=docker_password=PASSWORD\`" >&2
    exit 1
fi
