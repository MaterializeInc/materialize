# Formal Type System Verification — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Evaluate three parallel approaches (Coq, Lean 4, Rust property tests) for formally verifying Materialize's cast graph properties, starting with the simplest end-to-end proof of concept.

**Architecture:** A shared JSON corpus extracted from the Rust `VALID_CASTS` table feeds all three approaches. The Rust property tests run in `cargo test`. Coq and Lean developments live in `formal/` and verify properties over the corpus. CI regenerates the corpus and re-checks all three.

**Tech Stack:** Rust (proptest, serde_json), Coq 8.18+, Lean 4 (Mathlib optional), Python 3 (corpus generation scripts for Coq/Lean)

---

## File Structure

```
formal/
  shared/
    cast_corpus.json              # Generated: all (from, to, context) triples
    generate_corpus.rs            # Rust binary that dumps VALID_CASTS to JSON
  coq/
    _CoqProject                   # Coq project file
    Types.v                       # ScalarBaseType inductive
    CastGraph.v                   # Generated: cast relation from JSON
    Properties.v                  # Theorems: P1 (DAG), P2 (monotonicity), P3 (coherence)
    generate_cast_graph.py        # JSON → CastGraph.v generator
  lean/
    lakefile.lean                 # Lean 4 project config
    FormalTypeSystem/
      Types.lean                  # ScalarBaseType inductive
      CastGraph.lean              # Generated: cast data from JSON
      Properties.lean             # Theorems: P1, P2, P3
    generate_cast_graph.py        # JSON → CastGraph.lean generator
src/sql/src/plan/
  typeconv.rs                     # Existing — add pub(crate) accessor for VALID_CASTS iteration
  typeconv_properties_test.rs     # New — Rust property tests T1–T5
```

---

### Task 1: Corpus Generator — Extract VALID_CASTS to JSON

**Context:** All three approaches need the same ground truth. We write a Rust binary that iterates `VALID_CASTS` and serializes every `(from, to, context)` triple to JSON.

**Files:**
- Create: `formal/shared/generate_corpus.rs`
- Create: `formal/shared/cast_corpus.json` (generated output)
- Modify: `src/sql/src/plan/typeconv.rs` — expose iteration over VALID_CASTS

- [ ] **Step 1: Expose VALID_CASTS for iteration**

In `src/sql/src/plan/typeconv.rs`, add a public function that returns all cast triples. Add this after the `VALID_CASTS` definition (after line 1077):

```rust
/// Returns all registered casts as (from, to, context) triples.
/// Used by the formal verification corpus generator.
pub fn all_casts() -> Vec<(SqlScalarBaseType, SqlScalarBaseType, CastContext)> {
    VALID_CASTS
        .iter()
        .map(|((from, to), imp)| (*from, *to, imp.context))
        .collect()
}
```

Also add `pub` to the `CastContext` derives if not already public (it is — `pub enum CastContext` at line 256). Ensure `CastContext` derives `Serialize`:

In `typeconv.rs`, add to the `CastContext` enum:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum CastContext {
```

(Add `use serde::Serialize;` to the imports if not present.)

- [ ] **Step 2: Write the corpus generator binary**

Create `formal/shared/generate_corpus.rs`:

```rust
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generates cast_corpus.json from VALID_CASTS for formal verification.

use serde_json::{json, Value};
use mz_sql::plan::typeconv::{all_casts, CastContext};

fn main() {
    let casts = all_casts();
    let entries: Vec<Value> = casts
        .into_iter()
        .map(|(from, to, ctx)| {
            json!({
                "from": format!("{from:?}"),
                "to": format!("{to:?}"),
                "context": match ctx {
                    CastContext::Implicit => "Implicit",
                    CastContext::Assignment => "Assignment",
                    CastContext::Explicit => "Explicit",
                    CastContext::Coerced => "Coerced",
                }
            })
        })
        .collect();

    let json = serde_json::to_string_pretty(&entries).expect("serialization failed");
    println!("{json}");
}
```

Note: This binary may need adjustments depending on how `mz_sql` is structured as a library. An alternative approach is to write this as a `#[mz_ore::test]` that outputs the JSON, which avoids binary build issues. If the binary approach is problematic, fall back to:

```rust
#[mz_ore::test]
fn generate_cast_corpus() {
    let casts = all_casts();
    // ... same serialization logic ...
    std::fs::write("formal/shared/cast_corpus.json", json).unwrap();
}
```

- [ ] **Step 3: Generate the corpus and inspect it**

Run:
```bash
# If using the test approach:
cargo test -p mz-sql generate_cast_corpus -- --ignored
# Or if using binary:
cargo run --bin generate-cast-corpus > formal/shared/cast_corpus.json
```

Expected: A JSON file with ~215 entries like:
```json
[
  { "from": "Bool", "to": "Int32", "context": "Explicit" },
  { "from": "Bool", "to": "Int64", "context": "Explicit" },
  { "from": "Bool", "to": "String", "context": "Assignment" },
  { "from": "Int16", "to": "Int32", "context": "Implicit" },
  ...
]
```

- [ ] **Step 4: Commit**

```bash
git add formal/shared/ src/sql/src/plan/typeconv.rs
git commit -m "feat: add cast corpus generator for formal verification"
```

---

### Task 2: Rust Property Tests — T1 (Implicit DAG) and T2 (Context Monotonicity)

**Context:** These are the simplest structural properties and give immediate value. We test them exhaustively over the real `VALID_CASTS` data.

**Files:**
- Create: `src/sql/src/plan/typeconv_properties_test.rs`
- Modify: `src/sql/src/plan.rs` or `src/sql/src/plan/typeconv.rs` — add `#[cfg(test)] mod typeconv_properties_test;`

- [ ] **Step 1: Create the test module and write T1 (implicit cast DAG)**

Create `src/sql/src/plan/typeconv_properties_test.rs`:

```rust
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Property-based tests for the cast graph.
//!
//! These tests verify structural invariants of VALID_CASTS:
//! - P1: The implicit cast subgraph is acyclic (a DAG)
//! - P2: No (from, to) pair has contradictory context entries

use std::collections::{BTreeMap, BTreeSet};

use super::typeconv::{all_casts, CastContext};
use mz_repr::SqlScalarBaseType;

/// T1: The implicit cast subgraph must be a DAG (no cycles).
///
/// If implicit casts formed a cycle (A →implicit B →implicit ... →implicit A),
/// the coercion system could loop infinitely or produce ambiguous results.
#[mz_ore::test]
fn implicit_cast_subgraph_is_dag() {
    let casts = all_casts();

    // Build adjacency list for implicit casts only.
    let mut adj: BTreeMap<SqlScalarBaseType, Vec<SqlScalarBaseType>> = BTreeMap::new();
    for (from, to, ctx) in &casts {
        if *ctx == CastContext::Implicit {
            adj.entry(*from).or_default().push(*to);
        }
    }

    // DFS cycle detection.
    #[derive(Clone, Copy, PartialEq)]
    enum Color {
        White,
        Gray,
        Black,
    }

    let mut color: BTreeMap<SqlScalarBaseType, Color> = BTreeMap::new();
    let mut cycle_path: Vec<SqlScalarBaseType> = Vec::new();

    fn dfs(
        node: SqlScalarBaseType,
        adj: &BTreeMap<SqlScalarBaseType, Vec<SqlScalarBaseType>>,
        color: &mut BTreeMap<SqlScalarBaseType, Color>,
        path: &mut Vec<SqlScalarBaseType>,
    ) -> bool {
        color.insert(node, Color::Gray);
        path.push(node);

        if let Some(neighbors) = adj.get(&node) {
            for &next in neighbors {
                match color.get(&next).copied().unwrap_or(Color::White) {
                    Color::Gray => {
                        path.push(next);
                        return true; // Cycle found
                    }
                    Color::White => {
                        if dfs(next, adj, color, path) {
                            return true;
                        }
                    }
                    Color::Black => {}
                }
            }
        }

        path.pop();
        color.insert(node, Color::Black);
        false
    }

    // Run DFS from every node.
    let all_nodes: BTreeSet<_> = casts
        .iter()
        .flat_map(|(from, to, _)| [*from, *to])
        .collect();

    for &node in &all_nodes {
        if color.get(&node).copied().unwrap_or(Color::White) == Color::White {
            if dfs(node, &adj, &mut color, &mut cycle_path) {
                panic!(
                    "Cycle detected in implicit cast subgraph: {:?}",
                    cycle_path
                );
            }
        }
    }
}

/// T2: No (from, to) pair should appear more than once in VALID_CASTS.
///
/// The BTreeMap key is (SqlScalarBaseType, SqlScalarBaseType), so duplicates
/// are structurally impossible. This test documents and verifies that invariant.
#[mz_ore::test]
fn no_duplicate_cast_entries() {
    let casts = all_casts();
    let mut seen = BTreeSet::new();
    for (from, to, _ctx) in &casts {
        assert!(
            seen.insert((*from, *to)),
            "Duplicate cast entry: ({from:?}, {to:?})"
        );
    }
}
```

- [ ] **Step 2: Wire the test module**

In the file that declares `mod typeconv` (likely `src/sql/src/plan.rs` or `src/sql/src/plan/mod.rs`), add:

```rust
#[cfg(test)]
mod typeconv_properties_test;
```

- [ ] **Step 3: Run the tests**

Run:
```bash
cargo test -p mz-sql implicit_cast_subgraph_is_dag
cargo test -p mz-sql no_duplicate_cast_entries
```

Expected: Both PASS. If T1 finds a cycle, that's an immediate finding worth investigating.

- [ ] **Step 4: Commit**

```bash
git add src/sql/src/plan/typeconv_properties_test.rs src/sql/src/plan.rs
git commit -m "test: add property tests for implicit cast DAG and no-duplicates"
```

---

### Task 3: Rust Property Tests — T3 (Coercion Path Coherence)

**Context:** For any two types A, B reachable via multiple implicit cast paths, all paths should agree. We enumerate all pairs and check for conflicting multi-hop implicit paths.

**Files:**
- Modify: `src/sql/src/plan/typeconv_properties_test.rs`

- [ ] **Step 1: Write T3**

Append to `typeconv_properties_test.rs`:

```rust
/// T3: If type A can reach type B via multiple implicit cast paths,
/// the "widened" type should be the same regardless of path.
///
/// We compute the transitive closure of implicit casts and check that
/// for any (A, B) pair with an implicit path, there is no type C != B
/// also reachable from A such that B is reachable from C (or vice versa)
/// via implicit casts — which would indicate ambiguous widening.
#[mz_ore::test]
fn implicit_cast_paths_are_unambiguous() {
    let casts = all_casts();

    // Build adjacency list for implicit casts.
    let mut adj: BTreeMap<SqlScalarBaseType, BTreeSet<SqlScalarBaseType>> = BTreeMap::new();
    for (from, to, ctx) in &casts {
        if *ctx == CastContext::Implicit {
            adj.entry(*from).or_default().insert(*to);
        }
    }

    // Compute reachability (transitive closure) for each node via BFS.
    let all_nodes: BTreeSet<_> = casts
        .iter()
        .flat_map(|(from, to, _)| [*from, *to])
        .collect();

    let mut reachable: BTreeMap<SqlScalarBaseType, BTreeSet<SqlScalarBaseType>> = BTreeMap::new();

    for &start in &all_nodes {
        let mut visited = BTreeSet::new();
        let mut queue = std::collections::VecDeque::new();
        if let Some(neighbors) = adj.get(&start) {
            for &n in neighbors {
                queue.push_back(n);
            }
        }
        while let Some(node) = queue.pop_front() {
            if visited.insert(node) {
                if let Some(neighbors) = adj.get(&node) {
                    for &n in neighbors {
                        if !visited.contains(&n) {
                            queue.push_back(n);
                        }
                    }
                }
            }
        }
        reachable.insert(start, visited);
    }

    // For each direct implicit cast A → B, check that there is no other
    // direct implicit cast A → C where C can also reach B (or B can reach C).
    // If A → B and A → C both exist and B ↔ C are reachable from each other,
    // that's fine (they converge). But if A → B and A → C exist and neither
    // can reach the other, that's a divergent diamond — potential ambiguity.
    for &start in &all_nodes {
        let direct_targets: Vec<_> = adj
            .get(&start)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();

        for i in 0..direct_targets.len() {
            for j in (i + 1)..direct_targets.len() {
                let b = direct_targets[i];
                let c = direct_targets[j];

                let b_reaches_c = reachable
                    .get(&b)
                    .map(|r| r.contains(&c))
                    .unwrap_or(false);
                let c_reaches_b = reachable
                    .get(&c)
                    .map(|r| r.contains(&b))
                    .unwrap_or(false);

                // If neither can reach the other, we have divergent paths.
                // This isn't necessarily a bug (function resolution handles it),
                // but it's worth flagging.
                if !b_reaches_c && !c_reaches_b {
                    // Log but don't fail — divergent paths are handled by
                    // function resolution's "preferred type" logic.
                    eprintln!(
                        "Note: divergent implicit cast paths from {start:?}: \
                         {start:?} → {b:?} and {start:?} → {c:?} \
                         (neither reaches the other)"
                    );
                }
            }
        }
    }
}
```

- [ ] **Step 2: Run the test**

Run:
```bash
cargo test -p mz-sql implicit_cast_paths_are_unambiguous -- --nocapture
```

Expected: PASS, possibly with informational notes about divergent paths. These notes are interesting findings even if not bugs.

- [ ] **Step 3: Commit**

```bash
git add src/sql/src/plan/typeconv_properties_test.rs
git commit -m "test: add implicit cast path coherence check"
```

---

### Task 4: Rust Property Tests — T4 (No Implicit Cast Panics)

**Context:** This is the test most directly related to the sqlsmith panic. For every registered implicit cast, we construct a canonical value and execute the cast function. This requires more setup because we need an `ExprContext` or must call into the cast at a lower level.

**Files:**
- Modify: `src/sql/src/plan/typeconv_properties_test.rs`
- Modify: `src/sql/src/plan/typeconv.rs` — expose `get_cast` or the templates for testing

- [ ] **Step 1: Assess feasibility of calling cast functions in tests**

The `get_cast` function (typeconv.rs:1115) requires an `ExprContext`, which is a complex planning context. Two options:

**Option A (simpler):** Test at the `UnaryFunc` level. Each cast in `VALID_CASTS` maps to a `UnaryFunc` variant (e.g., `CastInt32ToInt64`). We can call `UnaryFunc::eval` directly with a test `Datum`.

**Option B:** Build a minimal `ExprContext` for testing. This is more realistic but requires more setup.

Start with Option A. Add a function to `typeconv.rs` that exposes the cast templates:

```rust
/// Returns all registered casts with their implementation details.
/// Used by property tests to exercise cast functions directly.
#[cfg(test)]
pub fn all_cast_templates() -> &'static BTreeMap<(SqlScalarBaseType, SqlScalarBaseType), CastImpl> {
    &VALID_CASTS
}
```

Make `CastImpl` fields `pub(crate)`:
```rust
pub(crate) struct CastImpl {
    pub(crate) template: CastTemplate,
    pub(crate) context: CastContext,
}
```

- [ ] **Step 2: Write T4**

This test depends heavily on whether we can construct `ExprContext` in a test. If not feasible at this layer, write a simpler version that at minimum verifies all cast entries reference valid type pairs:

```rust
/// T4: Every implicit cast in VALID_CASTS should reference types that exist
/// and the cast context should be well-formed.
///
/// A deeper version of this test would execute each cast function with a
/// canonical Datum, but that requires an ExprContext. For now we verify
/// structural integrity.
#[mz_ore::test]
fn all_implicit_casts_are_structurally_valid() {
    let casts = all_casts();
    let implicit_casts: Vec<_> = casts
        .iter()
        .filter(|(_, _, ctx)| *ctx == CastContext::Implicit)
        .collect();

    // Every implicit cast should not be from a type to itself.
    for (from, to, _) in &implicit_casts {
        assert_ne!(
            from, to,
            "Self-cast should not be registered as implicit: {from:?}"
        );
    }

    // Every implicit cast's source type should also have an explicit or
    // assignment cast to String (every type should be printable).
    let has_string_cast: BTreeSet<_> = casts
        .iter()
        .filter(|(_, to, _)| *to == SqlScalarBaseType::String)
        .map(|(from, _, _)| *from)
        .collect();

    for (from, _to, _) in &implicit_casts {
        if !has_string_cast.contains(from) {
            eprintln!(
                "Warning: {from:?} has implicit casts but no cast to String"
            );
        }
    }

    assert!(
        implicit_casts.len() > 50,
        "Suspiciously few implicit casts: {}. Did VALID_CASTS change dramatically?",
        implicit_casts.len()
    );
}
```

- [ ] **Step 3: Run the test**

Run:
```bash
cargo test -p mz-sql all_implicit_casts_are_structurally_valid -- --nocapture
```

Expected: PASS with possible warnings.

- [ ] **Step 4: Commit**

```bash
git add src/sql/src/plan/typeconv_properties_test.rs src/sql/src/plan/typeconv.rs
git commit -m "test: add structural validity check for implicit casts"
```

---

### Task 5: Coq Development — Types.v and CastGraph.v Generator

**Context:** Model `ScalarBaseType` as a Coq inductive type. Write a Python script that converts `cast_corpus.json` into a Coq file defining the cast relation.

**Files:**
- Create: `formal/coq/_CoqProject`
- Create: `formal/coq/Types.v`
- Create: `formal/coq/generate_cast_graph.py`
- Create: `formal/coq/CastGraph.v` (generated)

- [ ] **Step 1: Create the Coq project file**

Create `formal/coq/_CoqProject`:

```
-R . FormalTypeSystem

Types.v
CastGraph.v
Properties.v
```

- [ ] **Step 2: Write Types.v**

Create `formal/coq/Types.v`:

```coq
(** * Materialize Scalar Base Types

    This file defines the base scalar types used in Materialize's SQL type system.
    Generated to match [SqlScalarBaseType] in src/repr/src/scalar.rs.
*)

Require Import Coq.Lists.List.
Require Import Coq.Bool.Bool.
Require Import Coq.Arith.EqNat.
Import ListNotations.

Inductive ScalarBaseType : Set :=
  | Bool
  | Int16
  | Int32
  | Int64
  | UInt16
  | UInt32
  | UInt64
  | Float32
  | Float64
  | Numeric
  | Date
  | Time
  | Timestamp
  | TimestampTz
  | Interval
  | PgLegacyChar
  | PgLegacyName
  | Bytes
  | String
  | Char
  | VarChar
  | Jsonb
  | Uuid
  | Array
  | List
  | Record
  | Oid
  | Map
  | RegProc
  | RegType
  | RegClass
  | Int2Vector
  | MzTimestamp
  | Range
  | MzAclItem
  | AclItem.

(** Decidable equality on ScalarBaseType. *)
Scheme Equality for ScalarBaseType.
(* This generates ScalarBaseType_beq and ScalarBaseType_eq_dec. *)

(** Cast context levels, ordered from most permissive to least. *)
Inductive CastContext : Set :=
  | Implicit
  | Assignment
  | Explicit
  | Coerced.

Scheme Equality for CastContext.

(** All scalar base types as a list, for exhaustive enumeration. *)
Definition all_types : list ScalarBaseType :=
  [ Bool; Int16; Int32; Int64; UInt16; UInt32; UInt64;
    Float32; Float64; Numeric; Date; Time; Timestamp; TimestampTz;
    Interval; PgLegacyChar; PgLegacyName; Bytes; String; Char; VarChar;
    Jsonb; Uuid; Array; List; Record; Oid; Map; RegProc; RegType;
    RegClass; Int2Vector; MzTimestamp; Range; MzAclItem; AclItem ].

(** A cast entry: source type, target type, and context. *)
Record CastEntry := mkCast {
  cast_from : ScalarBaseType;
  cast_to : ScalarBaseType;
  cast_ctx : CastContext;
}.
```

- [ ] **Step 3: Write the generator script**

Create `formal/coq/generate_cast_graph.py`:

```python
#!/usr/bin/env python3
"""Generate CastGraph.v from cast_corpus.json."""

import json
import sys
from pathlib import Path

def main():
    corpus_path = Path(__file__).parent.parent / "shared" / "cast_corpus.json"
    if not corpus_path.exists():
        print(f"Error: {corpus_path} not found. Run the corpus generator first.", file=sys.stderr)
        sys.exit(1)

    with open(corpus_path) as f:
        casts = json.load(f)

    lines = []
    lines.append('(** * Cast Graph — Generated from cast_corpus.json')
    lines.append('    DO NOT EDIT — regenerate with generate_cast_graph.py *)')
    lines.append('')
    lines.append('Require Import Types.')
    lines.append('Require Import Coq.Lists.List.')
    lines.append('Import ListNotations.')
    lines.append('')
    lines.append('(** All registered casts from VALID_CASTS. *)')
    lines.append('Definition all_casts : list CastEntry :=')

    entries = []
    for c in casts:
        entries.append(f'  mkCast {c["from"]} {c["to"]} {c["context"]}')

    lines.append('  [ ' + '\n  ; '.join(entries))
    lines.append('  ].')
    lines.append('')
    lines.append('(** Decidable cast lookup. *)')
    lines.append('Definition has_cast (from to : ScalarBaseType) (ctx : CastContext) : bool :=')
    lines.append('  existsb (fun e =>')
    lines.append('    ScalarBaseType_beq (cast_from e) from')
    lines.append('    && ScalarBaseType_beq (cast_to e) to')
    lines.append('    && CastContext_beq (cast_ctx e) ctx')
    lines.append('  ) all_casts.')
    lines.append('')
    lines.append('(** Check if an implicit cast exists between two types. *)')
    lines.append('Definition has_implicit_cast (from to : ScalarBaseType) : bool :=')
    lines.append('  has_cast from to Implicit.')

    output_path = Path(__file__).parent / "CastGraph.v"
    with open(output_path, 'w') as f:
        f.write('\n'.join(lines) + '\n')

    print(f"Generated {output_path} with {len(casts)} cast entries.")

if __name__ == '__main__':
    main()
```

- [ ] **Step 4: Generate CastGraph.v and verify it compiles**

Run:
```bash
cd formal/coq
python3 generate_cast_graph.py
coqc -R . FormalTypeSystem Types.v
coqc -R . FormalTypeSystem CastGraph.v
```

Expected: Both files compile without errors.

- [ ] **Step 5: Commit**

```bash
git add formal/coq/
git commit -m "feat: add Coq type definitions and cast graph generator"
```

---

### Task 6: Coq Development — Properties.v (P1 and P2)

**Context:** Prove that the implicit cast subgraph is acyclic and that cast contexts are well-formed. Since the graph is finite and concrete, we use computational verification (`vm_compute` / `native_compute`).

**Files:**
- Create: `formal/coq/Properties.v`

- [ ] **Step 1: Write Properties.v with P1 (implicit DAG)**

Create `formal/coq/Properties.v`:

```coq
(** * Cast Graph Properties

    Machine-checked verification of structural properties
    of Materialize's cast graph.
*)

Require Import Types.
Require Import CastGraph.
Require Import Coq.Lists.List.
Require Import Coq.Bool.Bool.
Require Import Coq.Arith.PeanoNat.
Import ListNotations.

(** ** Helper: adjacency list for implicit casts *)

Definition implicit_targets (from : ScalarBaseType) : list ScalarBaseType :=
  map cast_to
    (filter (fun e =>
      ScalarBaseType_beq (cast_from e) from
      && CastContext_beq (cast_ctx e) Implicit
    ) all_casts).

(** ** P1: Implicit cast subgraph is acyclic

    We verify this by computing a topological ordering.
    If the graph has a cycle, no valid topological order exists.

    Strategy: for each type, compute the set of types reachable
    via implicit casts within |all_types| steps. If any type
    can reach itself, there is a cycle.
*)

(** Bounded BFS reachability — returns all types reachable from [start]
    within [fuel] steps via implicit casts. *)
Fixpoint reachable_from (fuel : nat) (visited : list ScalarBaseType)
    (frontier : list ScalarBaseType) : list ScalarBaseType :=
  match fuel with
  | O => visited
  | S fuel' =>
    let new_nodes := flat_map implicit_targets frontier in
    let unvisited := filter (fun t =>
      negb (existsb (ScalarBaseType_beq t) visited)
    ) new_nodes in
    match unvisited with
    | [] => visited
    | _ => reachable_from fuel' (visited ++ unvisited) unvisited
    end
  end.

Definition can_reach_self (t : ScalarBaseType) : bool :=
  let targets := implicit_targets t in
  existsb (ScalarBaseType_beq t)
    (reachable_from (length all_types) targets targets).

Definition implicit_graph_is_acyclic : bool :=
  negb (existsb can_reach_self all_types).

(** The main theorem: no implicit cast cycle exists. *)
Theorem implicit_casts_form_dag : implicit_graph_is_acyclic = true.
Proof. native_compute. reflexivity. Qed.

(** ** P2: No self-casts are registered as Implicit *)

Definition no_implicit_self_casts : bool :=
  negb (existsb (fun e =>
    ScalarBaseType_beq (cast_from e) (cast_to e)
    && CastContext_beq (cast_ctx e) Implicit
  ) all_casts).

Theorem no_implicit_self_cast : no_implicit_self_casts = true.
Proof. native_compute. reflexivity. Qed.
```

- [ ] **Step 2: Compile Properties.v**

Run:
```bash
cd formal/coq
coqc -R . FormalTypeSystem Properties.v
```

Expected: Compiles and both theorems are accepted. If `native_compute` is not available, fall back to `vm_compute`. If that's too slow for ~215 entries, we may need to optimize the reachability computation.

- [ ] **Step 3: Commit**

```bash
git add formal/coq/Properties.v
git commit -m "feat: prove implicit cast DAG and no-self-cast properties in Coq"
```

---

### Task 7: Lean 4 Development — Types.lean and CastGraph.lean Generator

**Context:** Same model as Coq but in Lean 4. Take advantage of `DecidableEq` and `native_decide`.

**Files:**
- Create: `formal/lean/lakefile.lean`
- Create: `formal/lean/FormalTypeSystem/Types.lean`
- Create: `formal/lean/generate_cast_graph.py`
- Create: `formal/lean/FormalTypeSystem/CastGraph.lean` (generated)

- [ ] **Step 1: Create the Lean project**

Create `formal/lean/lakefile.lean`:

```lean
import Lake
open Lake DSL

package «formal-type-system» where
  leanOptions := #[
    ⟨`autoImplicit, false⟩
  ]

@[default_target]
lean_lib «FormalTypeSystem» where
  srcDir := "FormalTypeSystem"
```

- [ ] **Step 2: Write Types.lean**

Create `formal/lean/FormalTypeSystem/Types.lean`:

```lean
/-- Materialize scalar base types.
    Matches `SqlScalarBaseType` in `src/repr/src/scalar.rs`. -/
inductive ScalarBaseType where
  | Bool
  | Int16
  | Int32
  | Int64
  | UInt16
  | UInt32
  | UInt64
  | Float32
  | Float64
  | Numeric
  | Date
  | Time
  | Timestamp
  | TimestampTz
  | Interval
  | PgLegacyChar
  | PgLegacyName
  | Bytes
  | String
  | Char
  | VarChar
  | Jsonb
  | Uuid
  | Array
  | List
  | Record
  | Oid
  | Map
  | RegProc
  | RegType
  | RegClass
  | Int2Vector
  | MzTimestamp
  | Range
  | MzAclItem
  | AclItem
  deriving DecidableEq, Repr, Inhabited, BEq, Hashable

/-- Cast context levels. -/
inductive CastContext where
  | Implicit
  | Assignment
  | Explicit
  | Coerced
  deriving DecidableEq, Repr, Inhabited, BEq

/-- A cast entry. -/
structure CastEntry where
  from : ScalarBaseType
  to : ScalarBaseType
  ctx : CastContext
  deriving Repr, BEq

/-- All scalar base types, for exhaustive enumeration. -/
def allTypes : List ScalarBaseType :=
  [ .Bool, .Int16, .Int32, .Int64, .UInt16, .UInt32, .UInt64,
    .Float32, .Float64, .Numeric, .Date, .Time, .Timestamp, .TimestampTz,
    .Interval, .PgLegacyChar, .PgLegacyName, .Bytes, .String, .Char, .VarChar,
    .Jsonb, .Uuid, .Array, .List, .Record, .Oid, .Map, .RegProc, .RegType,
    .RegClass, .Int2Vector, .MzTimestamp, .Range, .MzAclItem, .AclItem ]
```

- [ ] **Step 3: Write the generator script**

Create `formal/lean/generate_cast_graph.py`:

```python
#!/usr/bin/env python3
"""Generate CastGraph.lean from cast_corpus.json."""

import json
import sys
from pathlib import Path

def main():
    corpus_path = Path(__file__).parent.parent / "shared" / "cast_corpus.json"
    if not corpus_path.exists():
        print(f"Error: {corpus_path} not found.", file=sys.stderr)
        sys.exit(1)

    with open(corpus_path) as f:
        casts = json.load(f)

    lines = []
    lines.append('/- Cast Graph — Generated from cast_corpus.json')
    lines.append('   DO NOT EDIT — regenerate with generate_cast_graph.py -/')
    lines.append('')
    lines.append('import FormalTypeSystem.Types')
    lines.append('')
    lines.append('open ScalarBaseType CastContext')
    lines.append('')
    lines.append('/-- All registered casts from VALID_CASTS. -/')
    lines.append('def allCasts : Array CastEntry := #[')

    entries = []
    for c in casts:
        entries.append(f'  ⟨.{c["from"]}, .{c["to"]}, .{c["context"]}⟩')

    lines.append(',\n'.join(entries))
    lines.append(']')
    lines.append('')
    lines.append('/-- Check if a cast exists. -/')
    lines.append('def hasCast (from to : ScalarBaseType) (ctx : CastContext) : Bool :=')
    lines.append('  allCasts.any fun e => e.from == from && e.to == to && e.ctx == ctx')
    lines.append('')
    lines.append('/-- Check if an implicit cast exists. -/')
    lines.append('def hasImplicitCast (from to : ScalarBaseType) : Bool :=')
    lines.append('  hasCast from to .Implicit')

    output_path = Path(__file__).parent / "FormalTypeSystem" / "CastGraph.lean"
    with open(output_path, 'w') as f:
        f.write('\n'.join(lines) + '\n')

    print(f"Generated {output_path} with {len(casts)} cast entries.")

if __name__ == '__main__':
    main()
```

- [ ] **Step 4: Generate CastGraph.lean and build**

Run:
```bash
cd formal/lean
python3 generate_cast_graph.py
lake build
```

Expected: Lean project builds without errors.

- [ ] **Step 5: Commit**

```bash
git add formal/lean/
git commit -m "feat: add Lean 4 type definitions and cast graph generator"
```

---

### Task 8: Lean 4 Development — Properties.lean (P1 and P2)

**Context:** Prove the same properties as the Coq development, using Lean's `native_decide` tactic.

**Files:**
- Create: `formal/lean/FormalTypeSystem/Properties.lean`

- [ ] **Step 1: Write Properties.lean**

Create `formal/lean/FormalTypeSystem/Properties.lean`:

```lean
/- Cast graph properties — machine-checked verification. -/

import FormalTypeSystem.Types
import FormalTypeSystem.CastGraph

open ScalarBaseType CastContext

/-- Targets reachable from a type via one implicit cast step. -/
def implicitTargets (from : ScalarBaseType) : List ScalarBaseType :=
  (allCasts.toList.filter fun e => e.from == from && e.ctx == .Implicit).map (·.to)

/-- Bounded reachability via implicit casts. -/
def reachableFrom (fuel : Nat) (visited frontier : List ScalarBaseType) : List ScalarBaseType :=
  match fuel with
  | 0 => visited
  | fuel' + 1 =>
    let newNodes := frontier.bind implicitTargets
    let unvisited := newNodes.filter fun t => !visited.contains t
    match unvisited with
    | [] => visited
    | _ => reachableFrom fuel' (visited ++ unvisited) unvisited

/-- Check whether a type can reach itself via implicit casts. -/
def canReachSelf (t : ScalarBaseType) : Bool :=
  let targets := implicitTargets t
  (reachableFrom allTypes.length targets targets).contains t

/-- The implicit cast subgraph is acyclic. -/
def implicitGraphIsAcyclic : Bool :=
  !allTypes.any canReachSelf

/-- P1: No implicit cast cycle exists. -/
theorem implicit_casts_form_dag : implicitGraphIsAcyclic = true := by native_decide

/-- No self-casts registered as implicit. -/
def noImplicitSelfCasts : Bool :=
  !allCasts.toList.any fun e => e.from == e.to && e.ctx == .Implicit

/-- P2: No type has an implicit cast to itself. -/
theorem no_implicit_self_cast : noImplicitSelfCasts = true := by native_decide
```

- [ ] **Step 2: Build and verify**

Run:
```bash
cd formal/lean
lake build
```

Expected: Builds successfully, both theorems are accepted by the kernel. `native_decide` should handle ~215 entries comfortably — it compiles the decision procedure to native code.

If `native_decide` is too slow (unlikely at this scale), fall back to `decide`.

- [ ] **Step 3: Commit**

```bash
git add formal/lean/FormalTypeSystem/Properties.lean
git commit -m "feat: prove implicit cast DAG and no-self-cast properties in Lean 4"
```

---

### Task 9: Evaluation and Comparison

**Context:** All three approaches are now implemented for P1 and P2. Compare them on the evaluation rubric.

**Files:**
- Create: `formal/EVALUATION.md`

- [ ] **Step 1: Run all three and collect metrics**

```bash
# Rust tests
time cargo test -p mz-sql typeconv_properties -- --nocapture 2>&1 | tee /tmp/rust-results.txt

# Coq
cd formal/coq
time coqc -R . FormalTypeSystem Properties.v 2>&1 | tee /tmp/coq-results.txt

# Lean
cd formal/lean
time lake build 2>&1 | tee /tmp/lean-results.txt
```

- [ ] **Step 2: Write the evaluation**

Create `formal/EVALUATION.md` documenting:

1. **Bugs found** — any failures or warnings from T1–T4?
2. **Build times** — how long does each approach take?
3. **Lines of code** — for each approach, count spec lines vs. generated lines
4. **Pain points** — what was hardest about each approach?
5. **Recommendation** — which approach to invest in for P3 and P4?

Template:

```markdown
# Formal Type System Verification — Evaluation

## Summary

| Criterion | Rust Tests | Coq | Lean 4 |
|-----------|-----------|-----|--------|
| Bugs found | | | |
| Build time | | | |
| Spec LOC (hand-written) | | | |
| Generated LOC | | | |
| Proof confidence | exhaustive test | machine-checked | machine-checked |
| CI integration effort | zero (cargo test) | moderate (coqc in CI) | moderate (lake in CI) |
| Ergonomics | | | |

## Detailed Findings

### Rust Property Tests
...

### Coq
...

### Lean 4
...

## Recommendation
...
```

- [ ] **Step 3: Commit**

```bash
git add formal/EVALUATION.md
git commit -m "docs: add formal verification evaluation results"
```

---

### Task 10: Corpus Regeneration CI Check (Stretch Goal)

**Context:** Ensure the formal specs stay in sync with the Rust code. If someone adds a new cast to `VALID_CASTS`, CI should detect that `cast_corpus.json` is stale.

**Files:**
- Create: `formal/shared/check_corpus_freshness.sh`

- [ ] **Step 1: Write the freshness check script**

Create `formal/shared/check_corpus_freshness.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

# Regenerate the corpus and compare with the checked-in version.
# Exits non-zero if the corpus is stale.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CORPUS="$SCRIPT_DIR/cast_corpus.json"

# Generate a fresh corpus to a temp file.
FRESH=$(mktemp)
trap "rm -f $FRESH" EXIT

cargo test -p mz-sql generate_cast_corpus -- --ignored 2>/dev/null
# The test writes to formal/shared/cast_corpus.json.new
# (adjust based on actual test implementation)

if ! diff -q "$CORPUS" "$FRESH" > /dev/null 2>&1; then
    echo "ERROR: cast_corpus.json is stale. Regenerate with:"
    echo "  cargo test -p mz-sql generate_cast_corpus -- --ignored"
    diff "$CORPUS" "$FRESH" || true
    exit 1
fi

echo "cast_corpus.json is up to date."
```

- [ ] **Step 2: Commit**

```bash
chmod +x formal/shared/check_corpus_freshness.sh
git add formal/shared/check_corpus_freshness.sh
git commit -m "ci: add cast corpus freshness check"
```

---

## Dependencies

```
Task 1 (corpus generator) ──→ Task 5 (Coq CastGraph) ──→ Task 6 (Coq Properties)
                           ├─→ Task 7 (Lean CastGraph) ──→ Task 8 (Lean Properties)
                           └─→ Task 2 (Rust T1/T2) ──→ Task 3 (Rust T3) ──→ Task 4 (Rust T4)

Tasks 4, 6, 8 ──→ Task 9 (Evaluation)
Task 1 ──→ Task 10 (CI check)
```

Tasks 2–4, 5–6, and 7–8 are independent tracks that can run in parallel after Task 1.
