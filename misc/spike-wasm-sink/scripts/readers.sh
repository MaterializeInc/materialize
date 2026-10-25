#!/usr/bin/env bash
# THROWAWAY: list a shard's readers and their sinces via persistcli.
WT=/home/moritz/dev/repos/materialize/.claude/worktrees/wasm-sink-spike
"$WT/target/optimized/persistcli" inspect state \
  --shard-id "$1" \
  --consensus-uri "postgres://postgres@localhost:5440/materialize?options=--search_path=consensus" \
  --blob-uri "file://$WT/mzdata/persist/blob" 2>/dev/null \
  | python3 -I -c '
import json, sys
s = json.load(sys.stdin)
for kind in ("leased_readers", "critical_readers"):
    for rid, r in (s.get(kind) or {}).items():
        print(kind, rid, "since=", r.get("since"), "purpose=", r.get("debug", {}).get("purpose"), "hb=", r.get("last_heartbeat_timestamp_ms"))
print("since", s.get("since"), "upper", s.get("upper"))
'
