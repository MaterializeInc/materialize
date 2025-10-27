#!/bin/sh
# Copyright Materialize, Inc. and contributors. All rights reserved.

# Use of this software is governed by the Business Source License
# included in the LICENSE file.

# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

set -euox pipefail
echo "$@"

OVR=/etc/dnsmasq.conf
: > "$OVR"

touch /dns_overrides.conf
chmod 644 /dns_overrides.conf

# Always keep Docker's embedded DNS for container-name resolution.
echo "server=127.0.0.11" >> "$OVR"

# Collect --entry flags: supports --entry=LINE and --entry LINE
while [ $# -gt 0 ]; do
  case "$1" in
    --entry=*)
      printf '%s\n' "${1#--entry=}" >> "$OVR"
      ;;
    --entry)
      shift
      [ $# -gt 0 ] || { echo "missing value after --entry" >&2; exit 2; }
      echo '%s' "$1" >> "$OVR"
      ;;
    --help|-h)
      cat <<EOF
Usage: entrypoint.sh [--entry <dnsmasq-directive>]...
Examples:
  --entry "address=/target.test.lan/10.0.0.123"
  --entry "cname=/alias.test.lan/target.test.lan"
  --entry "txt-record=_acme-challenge.app.test.lan,challenge-token"
Notes:
  - Lines are written verbatim to $OVR
  - Docker DNS passthrough is preserved: server=127.0.0.11
EOF
      exit 0
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 2
      ;;
  esac
  shift
done

# Run dnsmasq using our generated config
/usr/sbin/dnsmasq \
  --no-resolv \
  --keep-in-foreground \
  --log-queries \
  --log-facility=- \
  --conf-file="$OVR"
