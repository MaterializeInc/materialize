#\!/usr/bin/env python3
"""Generate CastGraph.v from cast_corpus.json."""

import json
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
corpus_path = os.path.join(script_dir, "..", "shared", "cast_corpus.json")
output_path = os.path.join(script_dir, "CastGraph.v")

with open(corpus_path, "r") as f:
    casts = json.load(f)

lines = []
lines.append("(** * Cast Graph — auto-generated from cast_corpus.json *)")
lines.append("(**    DO NOT EDIT BY HAND.  Re-run generate_cast_graph.py instead. *)")
lines.append("")
lines.append("Require Import Coq.Lists.List.")
lines.append("Require Import Coq.Bool.Bool.")
lines.append("Import ListNotations.")
lines.append("Require Import FormalTypeSystem.Types.")
lines.append("")
lines.append("Definition all_casts : list CastEntry :=")

for i, c in enumerate(casts):
    prefix = "  [ " if i == 0 else "    "
    suffix = " ]." if i == len(casts) - 1 else ";"
    lines.append(f"{prefix}mkCast {c['from']} {c['to']} {c['context']}{suffix}")

lines.append("")
lines.append("Definition has_cast (src dst : ScalarBaseType) (ctx : CastContext) : bool :=")
lines.append("  existsb (fun e =>")
lines.append("    ScalarBaseType_beq (cast_from e) src &&")
lines.append("    ScalarBaseType_beq (cast_to e) dst &&")
lines.append("    CastContext_beq (cast_ctx e) ctx")
lines.append("  ) all_casts.")
lines.append("")
lines.append("Definition has_implicit_cast (src dst : ScalarBaseType) : bool :=")
lines.append("  has_cast src dst Implicit.")
lines.append("")

with open(output_path, "w") as f:
    f.write("\n".join(lines))

print(f"Generated {output_path} with {len(casts)} cast entries.")
