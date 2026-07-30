#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# prepare-corpus.sh: seed the catalog_objects_serde_roundtrip corpus with one
# valid document per catalog type for the raw-bytes arm.
#
# That arm feeds the input straight to `serde_json::from_slice::<T>`, which
# returns nothing at all until the input is a complete object with the right
# field names and variant tags. For the nested types that is far out of reach of
# byte mutation starting from nothing: a run seeded only by libFuzzer itself
# keeps a corpus of 2-4 byte entries, because growing an input yields no new
# coverage until it parses, so there is no gradient to climb. The dictionary
# supplies the tokens, and these seeds supply the shape to insert them into.
#
# Each seed carries the two-byte prefix the target reads first: an odd `mode`
# byte to select the raw arm, then the type index. The JSON is deliberately
# minimal rather than a dump of a realistic value, so libFuzzer has short inputs
# to mutate.
#
# A seed that stops parsing (a field added to one of these types, say) costs
# only the coverage it was buying, never a false pass: the target's oracle runs
# on whatever it manages to decode. Field sets were taken from the types
# themselves, all twelve verified to deserialize at the time of writing.

set -euo pipefail

cd "$(dirname "$0")"

corpus=corpus/catalog_objects_serde_roundtrip
mkdir -p "$corpus"
find "$corpus" -maxdepth 1 -name 'seed_*' -delete

# `mode` byte 0x01 selects the raw arm; the second byte is the type index in the
# target's `dispatch!` list.
seed() {
    local index="$1" name="$2" json="$3"
    local file
    file="$corpus/seed_$(printf '%02d' "$index")_$name"
    printf '%b' "\\0001\\0$(printf '%03o' "$index")" > "$file"
    printf '%s' "$json" >> "$file"
}

cluster_value='{"name":"c","owner_id":"Public","privileges":[],"config":{"workload_class":null,"variant":"Unmanaged"}}'

seed 0 state_update_kind "{\"kind\":\"Cluster\",\"key\":{\"id\":{\"User\":1}},\"value\":$cluster_value}"
seed 1 cluster_value "$cluster_value"
seed 2 item_value '{"schema_id":{"User":1},"name":"i","definition":{"V1":{"create_sql":"SELECT 1"}},"owner_id":"Public","privileges":[],"oid":1,"global_id":{"User":1},"extra_versions":[]}'
seed 3 role_value '{"name":"r","attributes":{"inherit":true,"superuser":null,"login":null,"auto_provision_source":null},"membership":{"map":[]},"vars":{"entries":[]},"oid":1}'
seed 4 network_policy_value '{"name":"n","rules":[{"name":"a","address":"0.0.0.0/0","action":"Allow","direction":"Ingress"}],"owner_id":"Public","privileges":[],"oid":1}'
seed 5 cluster_replica_value '{"cluster_id":{"User":1},"name":"r1","config":{"logging":{"log_logging":false,"interval":null},"location":{"Managed":{"size":"1","availability_zones":[],"internal":false,"billed_as":null,"pending":false}},"arrangement_compression":false},"owner_id":"Public"}'
seed 6 cluster_config '{"workload_class":null,"variant":"Unmanaged"}'
seed 7 gid_mapping_value '{"catalog_id":1,"global_id":1,"fingerprint":"f"}'
seed 8 mz_acl_item '{"grantee":"Public","grantor":{"User":1},"acl_mode":{"bitflags":0}}'
seed 9 role_id '{"User":1}'
seed 10 config_value '{"value":1}'
seed 11 setting_value '{"value":"s"}'

echo "Seeded:"
printf "  %-40s %4d seeds\n" "$corpus/" "$(find "$corpus" -maxdepth 1 -name 'seed_*' | wc -l)"
