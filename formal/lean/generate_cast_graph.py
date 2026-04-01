#\!/usr/bin/env python3
"""Generate CastGraph.lean from cast_corpus.json."""

import json
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CORPUS_PATH = os.path.join(SCRIPT_DIR, "..", "shared", "cast_corpus.json")
OUTPUT_PATH = os.path.join(SCRIPT_DIR, "FormalTypeSystem", "CastGraph.lean")

def main():
    with open(CORPUS_PATH) as f:
        casts = json.load(f)

    lines = []
    lines.append("import FormalTypeSystem.Types")
    lines.append("")
    lines.append("open ScalarBaseType CastContext")
    lines.append("")
    lines.append(f"def allCasts : Array CastEntry := #[")

    for i, c in enumerate(casts):
        from_ty = c["from"]
        to_ty = c["to"]
        ctx = c["context"]
        comma = "," if i < len(casts) - 1 else ""
        lines.append(f"  ⟨.{from_ty}, .{to_ty}, .{ctx}⟩{comma}")

    lines.append("]")
    lines.append("")
    lines.append("def hasCast (from to : ScalarBaseType) (ctx : CastContext) : Bool :=")
    lines.append("  allCasts.any fun e => e.from == from && e.to == to && e.ctx == ctx")
    lines.append("")
    lines.append("def hasImplicitCast (from to : ScalarBaseType) : Bool :=")
    lines.append("  hasCast from to .Implicit")
    lines.append("")

    with open(OUTPUT_PATH, "w") as f:
        f.write("\n".join(lines))

    print(f"Generated {OUTPUT_PATH} with {len(casts)} cast entries.")

if __name__ == "__main__":
    main()
