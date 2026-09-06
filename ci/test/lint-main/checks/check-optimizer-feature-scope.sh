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
# check-optimizer-feature-scope.sh — keep env-wide optimizer features out of cluster-scoped plans.

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash

# `SystemVars::env_wide_optimizer_features` and `OptimizerConfig::env_wide` read
# the base layer of the optimizer-feature stack: no `CLUSTER ... FEATURES(...)`
# pin, no cluster-scoped system parameter. They are correct only for a plan that
# runs on no cluster. Anything installed on or executed by a cluster must go
# through `CatalogState::optimizer_{features,config}_for_cluster`, which layers
# all three; assembling the layers at the call site is how one gets dropped.
#
# Every file below has been checked to plan on no cluster, or to define the
# accessors themselves. Before adding one, confirm the code has no cluster in
# scope. If it does, use the resolver instead.
ALLOWED=(
  src/adapter/src/catalog/state.rs
  src/adapter/src/catalog/transact.rs
  src/adapter/src/client.rs
  src/adapter/src/coord/sequencer/inner.rs
  src/adapter/src/coord/sequencer/inner/copy_from.rs
  src/adapter/src/coord/sequencer/inner/create_view.rs
  src/adapter/src/coord/sequencer/inner/explain_timestamp.rs
  src/adapter/src/optimize.rs
  src/adapter/src/optimize/metric_sink.rs
  src/sql/src/session/vars/definitions.rs
)

MATCHES=$(git grep -n -E 'env_wide_optimizer_features\(|OptimizerConfig::env_wide\(' -- 'src/**/*.rs' \
  | grep -v -F -f <(printf '%s:\n' "${ALLOWED[@]}") || true)

if [ -n "$MATCHES" ]; then
  echo "Env-wide optimizer features read outside the allowlist in check-optimizer-feature-scope.sh."
  echo "If the code has a cluster in scope, resolve through CatalogState::optimizer_features_for_cluster"
  echo "or CatalogState::optimizer_config_for_cluster instead. If it genuinely plans on no cluster,"
  echo "add the file to ALLOWED with that justification."
  echo "$MATCHES"
  exit 1
fi

try_status_report
