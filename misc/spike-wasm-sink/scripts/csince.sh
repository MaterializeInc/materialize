#!/usr/bin/env bash
# THROWAWAY: sample lag of the controller's persist critical since, the sink's
# reported write frontier, and the controller-reported read frontier.
S=$(dirname "$(readlink -f "$0")")
for i in $(seq 1 "${1:-5}"); do
  now=$(date +%s%3N)
  c=$("$S/readers.sh" sb2116565-c51f-4b9d-8070-3aab46e01d48 | grep "controller data" | grep -oE "\[[0-9]+\]" | tr -d '[]')
  o=$("$S/obs.sh" 1 1 | grep -oE "since_lag=[0-9]+|sink_lag=[0-9]+" | tr '\n' ' ')
  echo "persist_controller_since_lag_ms=$((now - c)) $o"
  sleep "${2:-7}"
done
