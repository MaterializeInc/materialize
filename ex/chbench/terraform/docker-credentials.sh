#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
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

