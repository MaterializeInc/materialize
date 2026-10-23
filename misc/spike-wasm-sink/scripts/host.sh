#!/usr/bin/env bash
# THROWAWAY: run the spike host against the local envd MV shard.
# Usage: host.sh SHARD READER_ID_FILE [extra host args...]
set -euo pipefail
WT=/home/moritz/dev/repos/materialize/.claude/worktrees/wasm-sink-spike
SHARD=$1; RID=$2; shift 2
exec memcap 4G "$WT/target/optimized/mz-wasm-sink-spike" \
  --blob "file://$WT/mzdata/persist/blob" \
  --consensus "postgres://postgres@localhost:5440/materialize?options=--search_path=consensus" \
  --shard "$SHARD" \
  --columns 'id:int8!,v:text!,n:int8!' \
  --reader-id-file "$RID" \
  --valkey "${VALKEY:-127.0.0.1:6390}" \
  "$@"
