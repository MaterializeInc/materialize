# User-provided LIR plans: findings

Findings from the prototype of [user-provided LIR plans](./20261019_user_provided_lir_plans.md), on branch `user-lir-plans`.

## Summary

The prototype shows that an LIR plan from outside the optimizer can be extracted, edited, validated, installed as a materialized view, swapped under an existing view, and reloaded after a restart, as `test_lir_plan_materialized_view_survives_restart` in `src/environmentd/tests/server.rs`, `test/sqllogictest/lir_plans.slt`, and the demo in `misc/lir-datalog/demo/` show end to end.
The validator in `src/compute-types/src/plan/validate.rs` and its submodules runs on every optimizer plan under soft assertions, and `bin/sqllogictest --optimized` over the SLT files listed in [Extraction](#extraction) passes after four validator fixes (commits `737d0ddbcf`, `5c54785f21`, `80b7fe492d`, `8ffad63147`).
The mutation test `src/clusterd-test-driver/tests/lir_mutation.rs`, run against a replica with its default seed in a local run on 2026-10-20, generated 1598 mutants, rendered the 429 it accepted, and passed, which requires zero crashes and stalls (see [Panics and the mutation test](#panics-and-the-mutation-test)).
A toy Datalog compiler in `misc/lir-datalog` targets the format with Python types generated from the serde registry, and its plans validate and match SQL in `test/sqllogictest/lir_plans_datalog.slt`.
The prototype does not show that the validator is sound in general, it says nothing about production readiness or format compatibility, and it accepts no key or monotonicity claims, which the sections below explain.

## Persist shards as first-class objects

A `FROM PLAN` view is a materialized view with the SQL split off: a persist shard, a declared `RelationDesc`, and a dataflow that writes to the shard.
The catalog item keeps the `CatalogItem::MaterializedView` shape with `raw_expr` and `locally_optimized_expr` made optional (`src/catalog/src/memory/objects.rs:1421`, commit `cf61aa6e12`), and its description comes from the declared column definitions without keys.
Boot reads the plan back from `create_sql`, validates it again, and bypasses the expression cache (`src/adapter/src/coord.rs:3639`).
Every place that assumed a SQL definition needed a plan-only branch, listed below.
These branches are the shape of the shard object the spec asks about, since each one separates the shard from the definition that writes it.

* `EXPLAIN RAW`, `LOCALLY OPTIMIZED` and `OPTIMIZED PLAN` of a plan view report `has no SQL definition` (`PlanError::NoSqlDefinition`, `src/sql/src/plan/error.rs:337`).
* SQL queries cannot read a replacement defined by a plan, because there is no definition to inline (`src/adapter/src/optimize/dataflows.rs:351`, `src/adapter/src/coord/indexes.rs:59`).
* Timeline checks walk the view's catalog dependencies instead of its MIR (`src/adapter/src/catalog/timeline.rs:228`).
* Read-then-write statements take the view's temporal filters from the temporal bounds of its installed physical plan (`src/adapter/src/coord/read_then_write.rs:309`). A plan view's dependencies include the indexes it reads, which reached the `unreachable!()` for indexes at `read_then_write.rs:348` before the walk skipped them at `read_then_write.rs:328`. This was found by reading the code, not observed.
* A sink's `from_arrangement` is chosen by lowering for optimizer plans. For a plan, `validate_dataflow_choosing_sink_forms` (`src/compute-types/src/plan/validate.rs:139`) chooses it the same way, the raw collection if available and otherwise the first available arrangement, which needs the validator's simulation of available forms (test `chooses_sink_forms`).
* Most `PlanError`s report SQLSTATE `XX000`, so the `FROM PLAN` path uses `PlanError::InvalidLirPlan` (`42P17`) for invalid plans and `PlanError::NoSqlDefinition` (`0A000`) for EXPLAIN stages a plan does not have, while errors shared with all SQL, such as a disabled feature flag, still report `XX000` (`src/adapter/src/error.rs:1001`, test `test_lir_plan_errors_are_user_errors`).

A first-class shard object would own the name, the `RelationDesc` and the shard, and would take its writer as a separate attachment, a SQL definition or a plan.
Replacement already behaves this way: `CREATE REPLACEMENT MATERIALIZED VIEW ... FOR target FROM PLAN` installs a read-only second writer for the target's shard, and `APPLY REPLACEMENT` swaps it in (`lir_plans.slt`, and step 6 of the demo).
Plan views that import a replaced SQL materialized view keep running and read its new definition (`lir_plans.slt:734` and `:772` check the rewritten import id).
The prototype did not build such an object, so this is the design the seams suggest, not a tested one.
Reading a shard without a definition is exactly what the replacement read error above refuses today.

## Input references and ids

Users never need to write a `CatalogItemId`.
An envelope names each import as `ImportSource::Name`, a SQL item name parsed with SQL identifier rules, or `ImportSource::Item` with an item id and a relation version (`src/compute-types/src/plan/envelope.rs:61`).
Purification rewrites every name to `Item` and records the items in `resolved_ids` (`purify_create_materialized_view_plan`, `src/sql/src/pure.rs:2962`), so the stored `create_sql` names imports by id and survives `ALTER ... RENAME`.
Extraction prints names, quoted where SQL would otherwise fold or split them (commit `28edf0d50d`, `lir_plans_explain.slt`), and both the toy compiler and the demo write names only.
Ids appear only in the stored `create_sql`, which `lir_plans.slt` checks through `mz_materialized_views.create_sql` (`lir_plans.slt:245`).

Placeholders inside an envelope are `GlobalId::Transient`, and `parse_user_envelope` (`src/sql/src/plan/lir_plan.rs:45`) rejects a non-transient import id, object id, or `Get` of a global id.
The check cannot live in `validate_dataflow`, because optimizer dataflows use user and system ids, so it belongs to envelope resolution.
Since a plan's object ids are transient ids the user chose, a transient sink id could collide with them, so `EXPLAIN ... FOR CREATE ... FROM PLAN` exports its sink as `GlobalId::Explain` (`src/adapter/src/coord/sequencer/inner/create_materialized_view.rs:510`).
Only tables have relation versions in the SQL catalog interface (`CatalogItem::latest_version`), so purification records the root version for every other import and planning reads it at its latest collection, as SQL references do (`version_selector`, `src/sql/src/plan/lir_plan.rs:190`).
After `APPLY REPLACEMENT` on an imported materialized view, a plan view therefore reads the new collection.

Ids inside the plan string are invisible to every visitor of the SQL AST, which caused one boot failure, needed one rewrite, and leaves two known gaps.

* Boot orders derived items by the ids `mz_sql::names::dependencies` finds in `create_sql`. A plan view whose imports had larger ids than the view failed at boot with `unknown catalog item` until the visitor also read the plan string (`src/sql/src/names.rs:2656`, test `dependencies_include_plan_imports`).
* `ALTER MATERIALIZED VIEW ... APPLY REPLACEMENT` rewrites ids in dependents' `create_sql` through `create_stmt_replace_ids`, which now parses a plan body, rewrites its import ids, and prints it again (`replace_plan_import_ids`, `src/sql/src/ast/transform.rs:551`, tests `replace_ids_rewrites_plan_imports` and `replace_ids_keeps_unaffected_plan_text`). The name-based rewrites need no change, because purified plans name imports by id.
* `mz_internal.mz_object_dependencies` derives dependencies from `collect_item_references` (`src/catalog/src/builtin/mz_object_dependencies.rs:144`), which sees only `[id AS name]` references, so plan imports are missing there. The catalog still records them, and dropping an import requires `CASCADE` (`lir_plans.slt`).
* The `mz-deploy` dependency analysis misses plan imports for the same reason (`src/mz-deploy/src/project/analysis/deps.rs:419`), and `mz-deploy` cannot typecheck a plan view whose imports are written as names, because it plans without purification.

Every place that reads ids from the SQL AST of a `create_sql` needs the same treatment as the boot order and the id rewrite.
The gaps come from storing the plan as a string literal inside `create_sql`, which the spec chose because it needs no new durable state.
Storing plans outside `create_sql`, as pinned LIR intends to, would move plan ids out of the SQL text that these visitors walk.
The envelope is also parsed up to three times per statement, by name resolution, purification and planning, at a cost proportional to the plan's size, which the prototype accepts.

## Extraction

`EXPLAIN PHYSICAL PLAN AS LIR JSON FOR MATERIALIZED VIEW` prints the envelope of an installed view (`extract_envelope`, `src/compute-types/src/plan/envelope.rs:255`), and other explainees and stages are rejected.
Extraction replaces each source import's pushed-down operators with a leading `Mfp` object over the raw import, planned with `MapFilterProject::into_plan` as rendering does, because translating to LIR first fails on the `mz_now()` of temporal predicates.
Under soft assertions, `check_extract_round_trip` (`envelope.rs:424`) extracts, prints, parses, and maps back every optimizer dataflow, and `bin/sqllogictest --optimized -- test/sqllogictest/lir_plans_explain.slt` plus `test/sqllogictest/explain/*.slt`, `materialized_views.slt`, `joins.slt`, `topk.slt`, `with_mutually_recursive.slt`, `temporal.slt`, `aggregates.slt`, `ldbc_bi.slt`, `ldbc_bi_eager.slt`, `tpch_create_index.slt`, `tpch_create_materialized_view.slt`, `tpch_select.slt`, `transform/*.slt`, `transform/fold_vs_dataflow/*.slt`, `session-window-wmr.slt`, `recursive_type_unioning.slt`, `window_funcs.slt`, `outer_join.slt`, `subquery.slt`, `temporal_bucketing.slt`, `reduce_mfp.slt`, `group_size_hints.slt`, `jsonb.slt`, `list.slt`, `record.slt`, `arrays.slt` and `map.slt` passed with no mismatch, 5746 records including every builtin index at startup, in local runs on 2026-10-20 at commit `28edf0d50d`.
The pushdown object costs performance that was not measured: rendering passes source operators to `persist_source`, which reads only the demanded columns and filters while decoding, while an `Mfp` object sees full-width rows.
A relation that has both a persist import with non-identity operators and index imports cannot be extracted, because the pushdown object would hide the indexes, and the sweep produced no such dataflow (test `extract_rejects_pushdown_on_indexed_relation`).

In one local run before commit `fa03b94f0c`, with a temporary probe printing the JSON depth in `check_extract_round_trip`, the deepest optimizer envelope of the sweep nested 128 JSON levels for SLT queries and 107 for builtin indexes (`mz_internal.pg_type_all_databases_ind`).
A chain of 400 views of `SELECT DISTINCT a + 1 ... WHERE`, built into one materialized view, nests 816 levels, against `MAX_JSON_DEPTH` of 4096 (`envelope.rs:117`).
A local probe of a chain of 800 such views failed in the optimizer with "exceeded recursion limit of 2048" before lowering, so optimizer plans stay under the cap.
`envelope_to_json` writes rows without a readable form, for example intervals, as `StableRow`'s default byte-array serialization, which `parse_envelope` accepts, so such plans round trip but are not writable by hand (test `extract_then_parse_roundtrip`).

## Function allowlist exclusions

The allowlist admits 23 functions and excludes every other function the format can express (`src/compute-types/src/plan/validate/allowlist.rs:117`, `:163`, `:187`).
It admits 16 of the 593 scalar function variants in the registry snapshot `lir_v1.json` (338 unary, 212 binary, 43 variadic): `Not`, `IsNull`, `CastInt32ToInt64`, `CastInt64ToMzTimestamp`, `StepMzTimestamp`, `Eq`, `NotEq`, `Lt`, `Lte`, `Gt`, `Gte`, `AddInt64`, `SubInt64`, `MulInt64`, `And` and `Or`.
It admits one of 25 table functions, `Wrap`, only with `width >= 1` and `width` declared types, and 6 of 67 aggregates: `Count`, `SumInt64`, `MaxInt64`, `MinInt64`, `Any` and `All`.
Each entry carries a hand-written `TypingRule` (`Exact`, `SameAsEachOther`, `AllBool`, `Any` or `Rows`), and was admitted only after reading its body and the `eval` dispatch that reaches it.
A call to an excluded function fails with `Rule::Function` "function ... is not supported in user plans" (`src/compute-types/src/plan/validate/types.rs:308`, test `rejects_unlisted_function_when_enforced`).

Admitting more functions takes an exact input signature per function, and the code shows where those can come from and where they cannot.
Functions defined with `#[sqlfunc]` over typed parameters do not panic on a datum of another type: `try_from_result` fails, and the eager `eval` returns `EvalError::Internal("invalid input type")`, and the macro emits `sqlfunc_input_types` for them (`src/expr-derive-impl/src/source.rs:158`).
The panics sit in hand-written bodies and in bodies over bare `Datum` that call `unwrap_*`, for example `And` and `Or` (`unreachable!()` on a non-boolean), and in aggregate rendering, where `datum_to_accumulator` (`src/compute/src/render/reduce.rs:1770`) panics on a non-boolean for `any` and on a non-integer for integer sums.
Output types are not total either, since `output_type` implementations unwrap their input types, so the validator computes them under `catch_unwind` and marks a panicking column's type unknown, which user plans reject.
A signature generated from `sqlfunc_input_types` would cover the typed `#[sqlfunc]` functions, while hand-written bodies, bare-`Datum` bodies, table functions and aggregates each need a rule written by reading the body, as the 23 entries were.

Typing has further gaps that the allowlist closes in user plans.

* LIR constants carry no column types, so the validator infers them from datums. Optimizer plans contain all-null columns (`SELECT NULL`), containers whose element type an empty or all-null container does not show, and strings, numerics and booleans that may be `jsonb`. In user plans, all-null and container columns are rejected, and string, numeric and boolean datums take their non-JSON type, so a user plan cannot have a `jsonb` constant column of scalar datums (tests `rejects_all_null_constant_column`, `infers_constant_types`).
* Source import operators are MIR expressions, typed through their LIR translation, which fails on temporal filters. User plans therefore cannot read a source whose pushed-down operators hold a temporal filter, or an index whose key applies a function outside the allowlist (test `rejects_index_key_lir_cannot_express`).
* `TableFunc::output_arity` overflows on a `WithOrdinality` around a `Wrap` or `CsvExtract` of width `usize::MAX`, so the validator uses `TableFunc::checked_output_arity`.
* `Wrap` splits its arguments with `chunks(width)`, which panics for width 0, and an argument count that is not a multiple of `width` produces a short last row that the `FlatMap` MFP indexes past (tests `rejects_wrap_of_width_zero_when_enforced`, `rejects_wrap_of_partial_row`).

## Replacing keyed objects

A plan cannot replace a materialized view whose SQL definition has keys, for example `SELECT a, count(*) FROM t GROUP BY a`.
A plan view's description is its declared schema, which carries no keys, and replacement requires the replacement's description to equal the target's, keys included (`RelationDesc::diff` computes `key_diff` at `src/repr/src/relation.rs:1315`).
The statement fails with `replacement schema differs from target schema` (`src/adapter/src/error.rs:1617`, `test/sqllogictest/lir_plans.slt:711`).
Targets without inferred keys, such as a `UNION ALL` or the demo's semi-join view, can be replaced, and the demo swaps the improved plan under the step-1 SQL view itself.
Replacing a keyed object needs either a key claim that the validator checks or key inference on LIR, and the prototype has neither.

A key inference on LIR would mirror what the optimizer infers on MIR for the same operators, for example that the group key of a `Reduce` is a key of its output.
A claim could then be accepted only where the inference proves it, which matches the spec's open question on declared keys from #39596 being checked against the keys a plan structurally implies.
The prototype did not implement or test either option, so both remain proposals.
On the in-place question of the spec, the prototype supports in-place replacement only through the replacement statements, and the demo used them without needing new syntax.

## Format obstacles from the toy compiler

The toy compiler (`misc/lir-datalog/datalog2lir.py`, 577 lines) builds plans from Python types that `gen_lir_types.py` generates from the serde registry snapshot `src/compute-types/tests/snapshots/lir_v1.json` (673 containers).
The registry describes the shape of the format faithfully, but four details of the JSON encoding are missing or surprising.

* The registry keys struct fields and enum variants by name and sorts them (`canonicalize_for_json` in the `lir_schema` test), so it does not record declaration order. A generated encoder writes fields in name order, serde_json writes declaration order, and outside producers cannot reproduce the optimizer's output byte for byte, although both parse to the same value (`test_roundtrip_extracted_envelopes` in `misc/lir-datalog/test_datalog2lir.py` compares parsed JSON for this reason).
* The registry does not record `#[serde(default)]`. Six reachable fields are optional on input: `ColumnOrder::desc` and `nulls_last`, `LirAggregateExpr::distinct`, `ReprColumnType::nullable` and `SqlColumnType::nullable` (default `true`), and `SqlRelationType::keys`, and the generator lists them by hand.
* The registry describes `StableRow` as opaque `BYTES`, the protobuf encoding of a row, and says nothing about the readable `{"$row": [...]}` form that envelopes use. That form covers nine datum kinds (`Bool`, `Int16`, `Int32`, `Int64`, `UInt64`, `Float64`, `Numeric`, `String`, `MzTimestamp`), and other rows and every `StableEvalError` are protobuf bytes that an outside producer can only write by reimplementing `ProtoRow` and `ProtoEvalError`.
* 520 of the 673 containers are unit structs, all of them scalar function payloads, which serde_json writes as `null`, so a function reads `{"Eq": null}`. Serde names that are Python keywords (`from`, `global`, the variant `OrdinalMode::None`) need escaping. `LiteralValue` and `ConstantRows` are serde mirror enums behind `#[serde(with)]`, and `TreatAsEqual` serializes transparently, which the registry shows correctly, so the generator needs no special case, but their names do not appear in the Rust plan types.

The semantics of the format are not in the registry, and the compiler learned them from optimizer envelopes of the equivalent `WITH MUTUALLY RECURSIVE` views and from the validator's errors.
Arrangement layouts must be recomputed with `permutation_for_arrangement`, a join closure reads the row `key ++ stream values ++ lookup values` after thinning, a `Reduce` produces only its arrangement so a binding needs an `ArrangeBy` with `input_key` to form the raw collection, `LetRec` values must provide raw collections, and an MFP predicate states the number of columns it needs.
With those modeled on the optimizer's plan, every compiled plan in `test/sqllogictest/lir_plans_datalog.slt` validates and matches its SQL counterpart, including a transitive closure, mutual recursion in one `LetRec`, a `Let` chain, a three-way linear join, an equality filter, and a cross join on the empty key.
Two further obstacles concern the SQL surface: a `LetRec` limit with `return_at_limit: false` reports the SQL error text "Use RETURN AT RECURSION LIMIT to not error ...", which names syntax a plan author never wrote (`lir_plans_datalog.slt:77`), and `CREATE MATERIALIZED VIEW name () FROM PLAN` does not parse, so a plan cannot output a relation without columns, although zero-arity relations validate and render inside a plan (the `guarded` case in `lir_plans_datalog.slt`).
A format meant for outside producers needs these semantics documented next to the registry, which the [JSON Schema section](#a-json-schema-for-the-format) returns to.

## Compatibility promise and pinned LIR

A user plan needs a stronger promise than pinned LIR's best effort, because it has no SQL to fall back to.
The pinned LIR design starts as best effort, where "at any point, we may simply throw up our hands and replan" (`20260826_pinned_lir.md:176`), while a plan view has no SQL definition to replan from (`PlanError::NoSqlDefinition`).
The prototype stores the plan in `create_sql`, validates it again on every boot, and panics `environmentd` when it fails (the boot arm at `src/adapter/src/coord.rs:3639` panics at `coord.rs:3673`), as the spec accepts for a prototype.
A stored plan can fail for two reasons: an `LIR_VERSION` change, which `parse_envelope` rejects with `EnvelopeError::Version`, or a change to what the validator accepts.
Validator rules changed several times during the prototype, for example when the optimizer sweep removed a rule on bucketed reductions without buckets (`737d0ddbcf`) and scaled the `LetRec` pass limit with the bindings (`8ffad63147`), and a rule added after a plan is stored fails that plan at boot, so validator rules are part of the format a promise would cover.

A promise for user plans would have to cover three things: the serde shape, which `LIR_VERSION` and the `lir_schema` snapshot already track (`LIR_VERSION_POLICY`, `src/compute-types/src/plan.rs:232`), the validator rules, which nothing versions today, and the semantics of each operator, which the registry does not describe.
Once a version ships, a stored user plan at an old version needs a migration to the new one, not a replan, which conflicts with pinned LIR's plan to treat migrations as best effort.
Pinned LIR can keep its best effort for optimizer plans only if the two populations are told apart, for example by whether the object has a SQL definition, as `raw_expr.is_none()` does at boot.
What should happen at boot when a stored user plan no longer validates, an open question of the spec, has no good answer in the prototype: panicking takes down the environment, and skipping the view leaves its shard without a writer.

## Key and monotonicity claims

The validator accepts no claims, and the demo's improvement needed none.
The optimizer plans the demo's semi-join on `a = x OR b = x` as a cross join on arrangements by the empty key, and a union of one equi-join per equality under the same distinct reduction computes the same result, which the validator checks structurally.
The monotonic-aggregate candidate of the spec gave no demo, because user plans take import monotonicity from the same catalog analysis as the optimizer (`DataflowBuilder::monotonic_object`, `src/adapter/src/optimize/dataflows.rs:528`, called from `src/adapter/src/optimize/from_plan.rs:94`), and no gap between the MIR and LIR monotonicity analyses was found.
So the prototype found no demo-relevant optimization that needs a trusted claim.
The candidates users would want are monotonicity beyond what the catalog marks and keys, which the following paragraphs take in turn.

Monotonicity is derived, not claimed.
Monotonic reduce and top-k variants turn every retraction that reaches them into an error in the output (`ensure_monotonic`, `src/compute/src/render/top_k.rs:238`), so the validator requires a logically monotonic input, computed per operator as the optimizer's MIR `Monotonic` analysis does (`src/compute-types/src/plan/validate/monotonic.rs`).
The analysis is stronger than the spec's operator list, since it also passes distinct reductions, thresholds and constants of positive diffs, without which optimizer plans of a monotonic top-k over a `DISTINCT` of a monotonic source would fail (test `accepts_monotonic_topk_over_distinct_of_monotonic_import`).
An import is monotonic only if it is marked monotonic and none of its source imports has a temporal predicate (test `rejects_monotonic_topk_over_temporally_filtered_source`), and inside recursive values the analysis takes the greatest fixpoint, as the MIR analysis does, so a binding judged monotonic can still carry retractions within iterations and produce spurious errors, not panics.
A false monotonicity claim would fail the same way, with errors in the output and not panics, so such a claim is a correctness risk for the claimant only.

Single-time dataflows relax the rule further, as lowering does.
They also accept a physically monotonic input as `SingleTimeMonotonic` judges it, and, outside recursions, any input the operator consolidates, because lowering upgrades every hierarchical reduce and top-k there to a consolidating monotonic variant (test `single_time_consolidation_serves_outside_recursion_only`).
`DataflowDescription::is_single_time` soft-asserts that the as-of is not beyond the until, so the validator checks that first and treats a dataflow that breaks it as not single-time.
With these rules the validator reproduces the optimizer's monotonicity reasoning on LIR, so a claim could only add facts about imports that the catalog does not record.

Keys have no runtime check, so a key claim is safe only where the plan proves it.
Their main use found here is replacing keyed objects, and the checkable form is key inference on LIR, as [Replacing keyed objects](#replacing-keyed-objects) describes.
Neither keys nor monotonicity claims were implemented, so which claims users would make in practice remains open.
The demo is evidence that at least one class of plan improvements, splitting a disjunctive join, needs no claim at all.

## Panics and the mutation test

The mutation test found no rendering or evaluation panic that the validator did not prevent.
`src/clusterd-test-driver/tests/lir_mutation.rs` applies 28 mutation operators to 8 optimizer base plans (join with an index import, reduce, top-k, `LetRec` transitive closure, temporal filter, constant, and monotonic variants of reduce and top-k), validates each mutant in user plan mode under `catch_unwind`, and renders every accepted mutant on a replica that runs with `MZ_SOFT_ASSERTIONS=1` and `ore_overflowing_behavior=panic`.
The test asserts zero failures and, with the default 200 mutants per base, at least 1000 mutants, and it runs with the command in its module doc (`LIR_MUTATION_CLUSTERD`, `COCKROACH_URL`, `LIR_MUTATION_CLUSTERD_WRAPPER='memcap 4G'`).
Before the mutants, a self-check renders a plan that projects past the last column and asserts that the replica crashes, which the index `datums[*c]` at `src/expr/src/linear.rs:1670` causes.
The replica phase is not wired into CI and runs only with `LIR_MUTATION_CLUSTERD` and `COCKROACH_URL` set.

In local runs on 2026-10-20 at commit `0853e9906c`, the default seed `0x5eed0013` generated 1598 mutants, of which 77 did not parse, 1092 were rejected, and 429 were accepted and rendered, and `LIR_MUTATION_SEED` 1 to 10 each accepted 415 to 438, all without a validator panic, crash, stall, or panic line in the replica log.
Across the 11 seeds, about 17,500 mutants were generated and about 4,700 rendered, and a rerun at the same seed and commit draws the same mutants, since each comes from the fixed seed.
The coverage of accepted mutants is weak: in those runs, about 60% of them came from benign operators (`generic_int`, `lir_id`, `generic_array`), and some operators apply rarely, so the result shows soundness against single local mutations of these shapes, not in general.

The one failure was in the harness and is a finding about consumers of user-defined collections.
The validator accepts constant rows with any diff, including `i64::MAX`, and the replica rendered and persisted such a mutant without trouble, since collections carry multiplicities.
The harness's read-back expanded the sink output into one row per copy and exhausted the test's memory, until it counted multiplicities instead (`Driver::peek_count`, `src/clusterd-test-driver/src/driver.rs:206`, commit `481e769051`).
Crafted diffs can also overflow the `count` and `sum` accumulators of a reduction, as reading `src/compute/src/render/reduce.rs` shows, which panics under `ore_overflowing_behavior=panic`, as CI runs, and soft-panics in production, and the type rules do not bound diffs.
Readers of collections that user plans define must not assume small diffs.

The panic sites the validator rejects were found by reading rendering and evaluation code, and each has a unit test in `src/compute-types/src/plan/validate/tests.rs`.

* Scoping and shape: `Let` or `LetRec` outside the binding prefix that `RenderPlan::try_from` accepts (`expect("valid plan")`, `src/compute-client/src/controller/instance.rs:1597`, test `rejects_misplaced_let`), `LetRec` and `Union` vectors of different lengths (`zip_eq`, tests `rejects_letrec_length_mismatch`, `rejects_union_strategy_length_mismatch`), duplicate `LirId`s (`rejects_duplicate_lir_id`), a repeated local id in one `LetRec` (`src/compute/src/render.rs:1050`, `rejects_rebound_local_id`), and `DataflowDescription::depends_on`, which panics on an export of a collection that is neither an import nor a built object and asserts that object ids are unique (`build_desc`, `src/compute-types/src/dataflows.rs:401`).
* Arrangement availability: rendering panics while building the dataflow when an operator reads a form its input's `CollectionBundle` lacks, whether or not the input has data, and a node whose output has no form panics in hydration logging (`CollectionBundle invariant`, `render.rs:1609`). The validator simulates the forms of every node (tests `rejects_missing_join_arrangement`, `rejects_get_passing_no_form`, `rejects_topk_without_raw_collection`, `rejects_letrec_value_without_raw`, and others).
* Layouts: rendering never reads the permutation and thinning a plan states for an existing arrangement and looks arrangements up by exact key, so the validator requires canonical triples from `permutation_for_arrangement` for `ArrangeBy`, `Threshold` and stream thinnings (`rejects_non_canonical_stream_thinning`, `rejects_wrong_thinning`). Lowering states non-canonical triples in `Get` keys, which rendering ignores, so only their lengths are checked there (`accepts_get_keys_with_identity_permutation`).
* Reduce and top-k: a predicate in a reduce key or value plan (`render/reduce.rs:139`, `rejects_predicate_in_reduce_key_plan`), a zero bucket (`rejects_bucketed_reduce_with_zero_bucket`, `rejects_topk_bucket_of_zero`), a monotonic reduce without a monoid (`render/reduce.rs:1274`, `rejects_monotonic_reduce_with_non_monoid_aggregate`), a multiple basic reduce of fewer than two aggregates (`rejects_multiple_basic_plan_of_one_aggregate`), an aggregate the plan variant does not accumulate (`finalize_accum`, `rejects_accumulable_plan_with_hierarchical_aggregate`), an accumulable plan whose `simple_aggrs` differ from `full_aggrs` (`rejects_accumulated_dummy`, `rejects_accumulated_any_over_int64`), and `fused_unnest_list` on an aggregate that cannot fuse (`src/expr/src/relation/func.rs:2352`, `rejects_fused_unnest_list_for_unfusable_aggregate`).
* Joins: an empty equivalence class (`JoinClosure::apply` reads `exprs[0]`), a delta path that reads its source raw from a relation other than 0 (`render/join/delta_join.rs:889`, `rejects_raw_delta_path_not_at_relation_zero`), and paths that do not cover their inputs, which render without panicking but compute another result (`rejects_delta_join_missing_path`, `rejects_linear_join_stage_missing_input`).
* Evaluation: a literal with no datum (`unpack_first`, `rejects_literal_without_datum`), a temporal bound that is not an `mz_timestamp` (`rejects_temporal_bound_not_mz_timestamp`), source operators that `MfpPlan::create_from` rejects (`render.rs:284`, `rejects_invalid_source_operators`), and `IndexDesc::as_lir` on keys LIR cannot express, such as `mz_now()` (`rejects_index_key_lir_cannot_express`).
* Soft assertions, which panic where enabled: an `ArrangeBy` of a key its input already has (`rejects_arrange_by_existing_key`), a temporally bucketed monotonic top-k without consolidation (`rejects_temporally_bucketed_monotonic_topk_without_consolidation`), and unsorted sink non-null assertions (`rejects_unsorted_non_null_assertions`).
* Liveness: every recursive binding of a user plan must state an iteration limit, because a recursion that never converges occupies the replica's workers indefinitely (`rejects_letrec_without_limit`). Optimizer plans of `WITH MUTUALLY RECURSIVE` without `RETURN AT RECURSION LIMIT` have no limits, so the rule applies only in user plan mode.

Three panic sites need no rule.
The top-k stage error collection (`render/top_k.rs:423`) is always set, because the final stage validates whenever no bucket stage did.
The source import MFP identity assertion in `render_dataflow` always holds, because `persist_source` takes the MFP and leaves the identity behind (`MfpPlan::take`).
The half-join closures (`Closure claimed to never error`, `render/join/delta_join.rs:574`) rely on `could_error`, which is accurate for well-typed expressions, and the type rules and the allowlist keep user plans well typed.
The allowlist, not the type rules alone, is what keeps validated plans away from the `unwrap_*` panics in function bodies.

## Validator limits

The validator never recurses without a guard and bounds its total work, with four limits.

* Depth: the walk runs under a `RecursionGuard` with `RECURSION_LIMIT` of 2048 operators (`src/compute-types/src/plan/validate.rs:478`, `src/expr/src/relation.rs:70`, test `deep_plan_does_not_overflow`). The JSON cap `MAX_JSON_DEPTH` of 4096 levels admits about 1360 nested single-input operators, since a nested `Negate` costs three levels, so the JSON depth, not the validator, bounds the depth of user plans. No SQL reached the 2048 limit in local probes while developing commits `09def05de9` to `8ffad63147`, since the SQL parser rejected 200 nested subqueries and the optimizer rejected a chain of 800 views.
* Work budget: every operator visit is charged its own serialized content (`src/compute-types/src/plan/validate/cost.rs`), constants by the encoded length of their rows, plus the width of its output, and the budget is `WORK_BUDGET` (1,000,000) plus `PLAN_SIZE_FACTOR` (8) times the content of the plan and the width of its imports (`validate.rs:325`, `:332`, `:606`, `:1740`). The builtin `mz_catalog.mz_views`, which holds a constant of the builtin views' SQL, needs about 686,000 units for one pass (commit `d336b98743`). In one local run on 2026-10-19 at commit `d336b98743`, with a temporary print of work and budget in `validate_dataflow` over `bin/sqllogictest --optimized` of `with_mutually_recursive.slt`, `ldbc_bi.slt`, `ldbc_bi_eager.slt`, `tpch_create_index.slt`, `tpch_create_materialized_view.slt`, `tpch_select.slt`, `transform/*.slt`, `transform/fold_vs_dataflow/*.slt`, `session-window-wmr.slt` and `recursive_type_unioning.slt`, its budget was 6,490,568, the tightest of 947 distinct dataflows, all others had 17.5 times headroom or more, and the largest non-builtin plan used 16,008 of 1,044,168 units.
* `LetRec` passes: the values of a `LetRec` are visited until the binding types reach a fixpoint, at most `LET_REC_TYPE_ITERATIONS` (64) passes per binding (`validate.rs:307`, `:1533`). A chain where each binding reads a later one, as `WITH MUTUALLY RECURSIVE` produces when each definition reads the next, needs one pass per binding (test `infers_types_of_long_reversed_binding_chains`). Nested `LetRec`s multiply the visits of the values inside them, which the work budget bounds (tests `bounds_work_on_deeply_nested_let_rec`, `bounds_work_on_large_constant_in_nested_let_rec`).
* Type size: column types are trees, and a type of more than `TYPE_SIZE_LIMIT` (256) nodes is unknown (`src/compute-types/src/plan/validate/types.rs:66`), which bounds each clone, comparison and unification by a constant. In user plans such a type is reported where it arises, and every function application or check that meets an unknown type is rejected (tests `rejects_oversized_literal_type_when_enforced`, `rejects_oversized_source_column_type_when_enforced`, `bounds_work_on_copies_of_large_column_types`).

Without the content and width charges, a constant of many rows or a wide collection visited many times inside nested `LetRec`s costs orders of magnitude more than its visit count: in one local run on 2026-10-19 with the charges clamped to one unit by a temporary edit, `cargo test -p mz-compute-types --lib bounds_work_on_large_constant_in_nested_let_rec` and `bounds_work_on_wide_union` ran for more than 60 s, and with the charges both return the budget error in under 2 s in a debug build.
Local bindings are looked up through a map from id to a stack of positions, since a `LetRec` binds all its ids at once and a linear scan per `Get` costs quadratic work (test `resolves_many_locals_quickly`).
A validation of nested `LetRec`s that used up 1,000,000 units took 0.2 s in a debug build, and one of a plan of 200,000 operators that used up 17,000,000 units took 5 s (`WORK_BUDGET` doc).
The budget scales with the plan's content, so a large plan passes and only amplification is rejected.

## Pre-existing issues

`test/sqllogictest/recursion_limit.slt` overflows the stack of a `tokio-rt-worker` thread in `environmentd` on its first query, which has 388 `UNION`s, independent of this work.
A gdb backtrace shows the derived `Clone` of the SQL AST (`SetExpr::clone`, recursing through `Box<SetExpr>`), called from `mz_adapter::frontend_peek::try_frontend_peek`, with no frame in any code this work adds.
The same file and stack reproduced on 2026-10-19 with `bin/sqllogictest --optimized -- test/sqllogictest/recursion_limit.slt` in a worktree at `d3b5806a06`, which adds only design documents on top of upstream main.
The overflow therefore predates the validator and the rest of this work, and it needs its own fix in the adapter.
It also kept `recursion_limit.slt` out of the validator sweep.

Whether a bucketed hierarchical reduce without buckets can panic was open while the operator rules were written, and the answer is that it cannot.
Lowering emits that shape when the expected group size is at most 16 (`bucketing_of_expected_group_size`, `src/compute-types/src/plan.rs:1035`), and the builtin index `mz_wallclock_global_lag_recent_history_ind` has it and renders at every startup.
`build_bucketed` takes the first modulus as 1 (`src/compute/src/render/reduce.rs:872`), and its final stage validates the input when no bucket stage did (`reduce.rs:955`), so `err_output.expect` cannot fail.
In a local probe, a materialized view with `AGGREGATE INPUT GROUP SIZE = 1`, which plans `buckets=[]`, returned correct results across inserts and deletes, and the validator rule that rejected the shape was removed (test `accepts_bucketed_reduce_without_buckets`).
This is the only rule the optimizer sweep removed (`737d0ddbcf`), and of the other three sweep failures, one restricted the empty-constant arity rule to user plans (`5c54785f21`) and two were gaps in the validator's arity and type inference (`80b7fe492d`, `8ffad63147`).

## A JSON Schema for the format

A JSON Schema derived with `schemars` is the long-term option for describing the format to outside producers, since standard tooling generates types and validators from it.
The prototype generated Python types from the serde registry `lir_v1.json` instead, because it needs no change to the Rust types (`misc/lir-datalog/gen_lir_types.py`).
Every type reachable from `LirPlanEnvelope` would need a `JsonSchema` derive, including the scalar function types (593 variants in the registry), and `Row` and `StableRow` would need hand-written schemas for their custom serialization.
The schema would also be a second description of the format to keep in sync with the registry snapshot, unless it replaced it.
A schema could record what the registry loses, field defaults and the readable row form, but not the semantics of the [format obstacles](#format-obstacles-from-the-toy-compiler), which need prose or the validator's error messages.

## Demo results

The demo replaces a cross join that the optimizer produces from SQL with a plan that is about 100 times faster to hydrate.
`misc/lir-datalog/demo/flagged_transfers.sql` selects the transfers from or to a flagged account with `EXISTS (... WHERE f.account = t.src OR f.account = t.dst)`, which the optimizer decorrelates into a cross join of the distinct `(src, dst)` pairs with `flagged`, both arranged by the empty key, under a distinct reduction.
`improve_plan.py` replaces the cross join with a `Union` of one linear equi-join per equality, and binds both inputs with `Let` so the joins share arrangements.
`setup.sql` inserts 50,000 transfers, which are 50,000 distinct `(src, dst)` pairs, and 1,000 flagged accounts, and 1,982 transfers match: a run of `demo.sh` shows both views with 1,982 rows and no difference (`compare.sql`), and the same count follows from the generating expressions.

The step 1 measurement is recorded in `misc/lir-datalog/demo/README.md` (commit `b0b8081c4f`), from a run of `misc/lir-datalog/demo/demo.sh` against a fresh local `bin/environmentd --optimized` with the default cluster on 2026-10-20.

| Dataflow | Hydration | CPU time | Arranged records | Arranged size |
|---|---|---|---|---|
| SQL view (cross join) | 10.0 s | 10.9 s | 204,964 | 1826 kB |
| Edited plan | 0.1 s | 0.4 s | 254,964 | 2637 kB |

Hydration is the wait of the first `SELECT count(*)` after `CREATE`, and CPU time is the root operator's `mz_scheduling_elapsed` per dataflow, since summing over all operators double counts nested scopes (`cost.sql`).
The edited plan keeps one more arrangement of the pairs, because each equi-join reads them by a different key, which costs about 25% more arranged records and 45% more arranged bytes.
In other local runs on 2026-10-20 on an environment with leftover dataflows, the SQL view took 8.8 to 21 s to hydrate, so the absolute numbers depend on the machine and its load.
Writing the rewrite needed the closure row layout, canonical arrangement layouts, and `Let` bindings to share arrangements, none of which the registry states.
Because the SQL view has no keys, step 6 replaces it in place with the improved plan.

The comparison flatters the plan, because a SQL rewrite also avoids the cross join.
`t.src IN (SELECT account FROM flagged) OR t.dst IN (SELECT account FROM flagged)` plans with equi-joins, but computes each membership test separately, with negations and further joins, and its plan is larger than the edited one (`misc/lir-datalog/demo/README.md`).
The demo did not measure that rewrite, so the claim is that the optimizer cannot produce the edited plan from the given SQL, not that no SQL reaches comparable cost.
Two parts of the demo script assert less than they show: the step 6 comparison is tautological after the swap, since both views then run the same plan, and the step 4 rejection is printed but not asserted, since the script ignores statement failures.
The rejection itself is covered by `lir_plans.slt`, which asserts the validator's errors for broken plans.
