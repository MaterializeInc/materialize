#!/usr/bin/env bash
# THROWAWAY: run envd for the spike, with the WASM sink swap enabled in clusterd.
# Usage: envd.sh [extra spike JSON fields, e.g. '"fuel_base":1000000,']
S=/tmp/claude-1000/-home-moritz-dev-repos-materialize/aa72d2eb-d8d5-4ced-904e-9120a5e17007/scratchpad/wasm-sink
cd /home/moritz/dev/repos/materialize/.claude/worktrees/wasm-sink-spike || exit 1
export MZDEV_POSTGRES=postgres://postgres@localhost:5440/materialize
export MZ_SPIKE_WASM_SINK="{${1:-}\"wasm\":\"$S/guest/target/wasm32-wasip2/release/wasm_sink_guest.wasm\",\"valkey\":\"127.0.0.1:6390\"}"
export CARGO_INCREMENTAL=0
exec bin/environmentd --optimized >> "$S/envd.log" 2>&1
