// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::sync::Arc;

use mz_catalog::builtin::BUILTINS;
use mz_expr::{Eval, MirScalarExpr};
use mz_ore::now::to_datetime;
use mz_ore::{soft_assert_eq_or_log, task};
use mz_repr::{CatalogItemId, Datum, Row, RowArena, SqlRelationType, SqlScalarType, Timestamp};
use mz_sql::catalog::SessionCatalog;
use mz_sql::func::{Func, FuncImpl, OP_IMPLS, Operation};
use mz_sql::names::{
    ItemQualifiers, PartialItemName, QualifiedItemName, ResolvedDatabaseSpecifier, SchemaSpecifier,
};
use mz_sql::plan::{
    CoercibleScalarExpr, ExprContext, HirScalarExpr, HirToMirConfig, PlanContext, QueryContext,
    QueryLifetime, Scope, StatementContext,
};
use mz_sql::session::vars::{SystemVars, VarInput};

use crate::catalog::Catalog;
use crate::optimize::dataflows::{EvalTime, ExprPrep, ExprPrepOneShot};
use crate::session::Session;

/// System sessions have an empty `search_path` so it's necessary to
/// schema-qualify all referenced items.
///
/// Dummy (and ostensibly client) sessions contain system schemas in their
/// search paths, so do not require schema qualification on system objects such
/// as types.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_minimal_qualification() {
    Catalog::with_debug(|catalog| async move {
        struct TestCase {
            input: QualifiedItemName,
            system_output: PartialItemName,
            normal_output: PartialItemName,
        }

        let test_cases = vec![
            TestCase {
                input: QualifiedItemName {
                    qualifiers: ItemQualifiers {
                        database_spec: ResolvedDatabaseSpecifier::Ambient,
                        schema_spec: SchemaSpecifier::Id(catalog.get_pg_catalog_schema_id()),
                    },
                    item: "numeric".to_string(),
                },
                system_output: PartialItemName {
                    database: None,
                    schema: None,
                    item: "numeric".to_string(),
                },
                normal_output: PartialItemName {
                    database: None,
                    schema: None,
                    item: "numeric".to_string(),
                },
            },
            TestCase {
                input: QualifiedItemName {
                    qualifiers: ItemQualifiers {
                        database_spec: ResolvedDatabaseSpecifier::Ambient,
                        schema_spec: SchemaSpecifier::Id(catalog.get_mz_catalog_schema_id()),
                    },
                    item: "mz_array_types".to_string(),
                },
                system_output: PartialItemName {
                    database: None,
                    schema: None,
                    item: "mz_array_types".to_string(),
                },
                normal_output: PartialItemName {
                    database: None,
                    schema: None,
                    item: "mz_array_types".to_string(),
                },
            },
        ];

        for tc in test_cases {
            assert_eq!(
                catalog
                    .for_system_session()
                    .minimal_qualification(&tc.input),
                tc.system_output
            );
            assert_eq!(
                catalog
                    .for_session(&Session::dummy())
                    .minimal_qualification(&tc.input),
                tc.normal_output
            );
        }
        catalog.expire().await;
    })
    .await
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_effective_search_path() {
    Catalog::with_debug(|catalog| async move {
        let mz_catalog_schema = (
            ResolvedDatabaseSpecifier::Ambient,
            SchemaSpecifier::Id(catalog.state().get_mz_catalog_schema_id()),
        );
        let pg_catalog_schema = (
            ResolvedDatabaseSpecifier::Ambient,
            SchemaSpecifier::Id(catalog.state().get_pg_catalog_schema_id()),
        );
        let mz_temp_schema = (
            ResolvedDatabaseSpecifier::Ambient,
            SchemaSpecifier::Temporary,
        );

        // Behavior with the default search_schema (public)
        let session = Session::dummy();
        let conn_catalog = catalog.for_session(&session);
        assert_ne!(
            conn_catalog.effective_search_path(false),
            conn_catalog.search_path()
        );
        assert_ne!(
            conn_catalog.effective_search_path(true),
            conn_catalog.search_path()
        );
        assert_eq!(
            conn_catalog.effective_search_path(false),
            vec![
                mz_catalog_schema.clone(),
                pg_catalog_schema.clone(),
                conn_catalog.search_path()[0].clone()
            ]
        );
        assert_eq!(
            conn_catalog.effective_search_path(true),
            vec![
                mz_temp_schema.clone(),
                mz_catalog_schema.clone(),
                pg_catalog_schema.clone(),
                conn_catalog.search_path()[0].clone()
            ]
        );

        // missing schemas are added when missing
        let mut session = Session::dummy();
        session
            .vars_mut()
            .set(
                &SystemVars::new(),
                "search_path",
                VarInput::Flat(mz_repr::namespaces::PG_CATALOG_SCHEMA),
                false,
            )
            .expect("failed to set search_path");
        let conn_catalog = catalog.for_session(&session);
        assert_ne!(
            conn_catalog.effective_search_path(false),
            conn_catalog.search_path()
        );
        assert_ne!(
            conn_catalog.effective_search_path(true),
            conn_catalog.search_path()
        );
        assert_eq!(
            conn_catalog.effective_search_path(false),
            vec![mz_catalog_schema.clone(), pg_catalog_schema.clone()]
        );
        assert_eq!(
            conn_catalog.effective_search_path(true),
            vec![
                mz_temp_schema.clone(),
                mz_catalog_schema.clone(),
                pg_catalog_schema.clone()
            ]
        );

        let mut session = Session::dummy();
        session
            .vars_mut()
            .set(
                &SystemVars::new(),
                "search_path",
                VarInput::Flat(mz_repr::namespaces::MZ_CATALOG_SCHEMA),
                false,
            )
            .expect("failed to set search_path");
        let conn_catalog = catalog.for_session(&session);
        assert_ne!(
            conn_catalog.effective_search_path(false),
            conn_catalog.search_path()
        );
        assert_ne!(
            conn_catalog.effective_search_path(true),
            conn_catalog.search_path()
        );
        assert_eq!(
            conn_catalog.effective_search_path(false),
            vec![pg_catalog_schema.clone(), mz_catalog_schema.clone()]
        );
        assert_eq!(
            conn_catalog.effective_search_path(true),
            vec![
                mz_temp_schema.clone(),
                pg_catalog_schema.clone(),
                mz_catalog_schema.clone()
            ]
        );

        let mut session = Session::dummy();
        session
            .vars_mut()
            .set(
                &SystemVars::new(),
                "search_path",
                VarInput::Flat(mz_repr::namespaces::MZ_TEMP_SCHEMA),
                false,
            )
            .expect("failed to set search_path");
        let conn_catalog = catalog.for_session(&session);
        assert_ne!(
            conn_catalog.effective_search_path(false),
            conn_catalog.search_path()
        );
        assert_ne!(
            conn_catalog.effective_search_path(true),
            conn_catalog.search_path()
        );
        // Because we lazily initialize the `mz_temp` schema,
        // an explicit `mz_temp` search path before the first
        // temporary item creation gets filtered out in
        // `effective_search_path`.
        assert_eq!(
            conn_catalog.effective_search_path(false),
            vec![mz_catalog_schema.clone(), pg_catalog_schema.clone(),]
        );
        assert_eq!(
            conn_catalog.effective_search_path(true),
            vec![mz_temp_schema, mz_catalog_schema, pg_catalog_schema]
        );
        catalog.expire().await;
    })
    .await
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_smoketest_all_builtins() {
    fn inner(catalog: Catalog) -> Vec<mz_ore::task::JoinHandle<()>> {
        let catalog = Arc::new(catalog);
        let conn_catalog = catalog.for_system_session();

        let resolve_type_oid = |item: &str| conn_catalog.get_system_type(item).oid();
        let mut handles = Vec::new();

        // Extracted during planning; always panics when executed.
        let ignore_names = BTreeSet::from([
            "avg",
            "avg_internal_v1",
            "bool_and",
            "bool_or",
            "has_table_privilege", // > 3 s each
            "has_type_privilege",  // > 3 s each
            "mod",
            "mz_panic",
            "mz_sleep",
            "pow",
            "stddev_pop",
            "stddev_samp",
            "stddev",
            "var_pop",
            "var_samp",
            "variance",
        ]);

        let fns = BUILTINS::funcs()
            .map(|func| (&func.name, func.inner))
            .chain(OP_IMPLS.iter());

        for (name, func) in fns {
            if ignore_names.contains(name) {
                continue;
            }
            let Func::Scalar(impls) = func else {
                continue;
            };

            'outer: for imp in impls {
                let details = imp.details();
                let mut styps = Vec::new();
                for item in details.arg_typs.iter() {
                    let oid = resolve_type_oid(item);
                    let Ok(pgtyp) = mz_pgrepr::Type::from_oid(oid) else {
                        continue 'outer;
                    };
                    styps.push(SqlScalarType::try_from(&pgtyp).expect("must exist"));
                }
                let datums = styps
                    .iter()
                    .map(|styp| {
                        let mut datums = vec![Datum::Null];
                        datums.extend(styp.interesting_datums());
                        datums
                    })
                    .collect::<Vec<_>>();
                // Skip nullary fns.
                if datums.is_empty() {
                    continue;
                }

                let return_oid = details
                    .return_typ
                    .map(resolve_type_oid)
                    .expect("must exist");
                let return_styp = mz_pgrepr::Type::from_oid(return_oid)
                    .ok()
                    .map(|typ| SqlScalarType::try_from(&typ).expect("must exist"));

                let mut idxs = vec![0; datums.len()];
                while idxs[0] < datums[0].len() {
                    let mut args = Vec::with_capacity(idxs.len());
                    for i in 0..(datums.len()) {
                        args.push(datums[i][idxs[i]]);
                    }

                    let op = &imp.op;
                    let scalars = args
                        .iter()
                        .enumerate()
                        .map(|(i, datum)| {
                            CoercibleScalarExpr::Coerced(HirScalarExpr::literal(
                                datum.clone(),
                                styps[i].clone(),
                            ))
                        })
                        .collect();

                    let call_name = format!(
                        "{name}({}) (oid: {})",
                        args.iter()
                            .map(|d| d.to_string())
                            .collect::<Vec<_>>()
                            .join(", "),
                        imp.oid
                    );
                    let catalog = Arc::clone(&catalog);
                    let call_name_fn = call_name.clone();
                    let return_styp = return_styp.clone();
                    let handle = task::spawn_blocking(
                        || call_name,
                        move || {
                            smoketest_fn(
                                name,
                                call_name_fn,
                                op,
                                imp,
                                args,
                                catalog,
                                scalars,
                                return_styp,
                            )
                        },
                    );
                    handles.push(handle);

                    // Advance to the next datum combination.
                    for i in (0..datums.len()).rev() {
                        idxs[i] += 1;
                        if idxs[i] >= datums[i].len() {
                            if i == 0 {
                                break;
                            }
                            idxs[i] = 0;
                            continue;
                        } else {
                            break;
                        }
                    }
                }
            }
        }
        handles
    }

    let handles = Catalog::with_debug(|catalog| async { inner(catalog) }).await;
    for handle in handles {
        handle.await;
    }
}

fn smoketest_fn(
    name: &&str,
    call_name: String,
    op: &Operation<HirScalarExpr>,
    imp: &FuncImpl<HirScalarExpr>,
    args: Vec<Datum<'_>>,
    catalog: Arc<Catalog>,
    scalars: Vec<CoercibleScalarExpr>,
    return_styp: Option<SqlScalarType>,
) {
    let conn_catalog = catalog.for_system_session();
    let pcx = PlanContext::zero();
    let scx = StatementContext::new(Some(&pcx), &conn_catalog);
    let qcx = QueryContext::root(&scx, QueryLifetime::OneShot);
    let ecx = ExprContext {
        qcx: &qcx,
        name: "smoketest",
        scope: &Scope::empty(),
        relation_type: &SqlRelationType::empty(),
        allow_aggregates: false,
        allow_subqueries: false,
        allow_parameters: false,
        allow_windows: false,
    };
    let arena = RowArena::new();
    let mut session = Session::dummy();
    session
        .start_transaction(to_datetime(0), None, None)
        .expect("must succeed");
    let prep_style = ExprPrepOneShot {
        logical_time: EvalTime::Time(Timestamp::MIN),
        session: &session,
        catalog_state: catalog.state(),
    };

    // Execute the function as much as possible, ensuring no panics occur, but
    // otherwise ignoring eval errors. We also do various other checks.
    let res = (op.0)(&ecx, scalars, &imp.params, vec![]);
    if let Ok(hir) = res {
        let uneliminated_result_row = {
            if let HirScalarExpr::CallUnary { func, .. } = &hir
                && func.is_eliminable_cast()
            {
                let mut uneliminated_mir = hir
                    .clone()
                    .lower_uncorrelated(HirToMirConfig {
                        enable_cast_elimination: false,
                        ..catalog.system_config().into()
                    })
                    .expect("lowering eliminable cast should always succeed");
                prep_style
                    .prep_scalar_expr(&mut uneliminated_mir)
                    .expect("must succeed");

                // Pack the row, to avoid lifetime issues with the MIR we lowered here
                uneliminated_mir
                    .eval(&[], &arena)
                    .ok()
                    .map(|datum| Row::pack([datum]))
            } else {
                None
            }
        };

        if let Ok(mut mir) = hir.lower_uncorrelated(catalog.system_config()) {
            // Populate unmaterialized functions.
            prep_style.prep_scalar_expr(&mut mir).expect("must succeed");

            if let Ok(eval_result_datum) = mir.eval(&[], &arena) {
                if let Some(return_styp) = return_styp {
                    let mir_typ = mir.typ(&[]);
                    // MIR type inference should be consistent with the type
                    // we get from the catalog.
                    soft_assert_eq_or_log!(
                        mir_typ.scalar_type,
                        (&return_styp).into(),
                        "MIR type did not match the catalog type (cast elimination/repr type error)"
                    );
                    // The following will check not just that the scalar type
                    // is ok, but also catches if the function returned a null
                    // but the MIR type inference said "non-nullable".
                    if !eval_result_datum.is_instance_of(&mir_typ) {
                        panic!(
                            "{call_name}: expected return type of {return_styp:?}, got {eval_result_datum}"
                        );
                    }
                    // Check the consistency of `is_eliminable_cast`---we should get the same datum either way.
                    if let Some(row) = uneliminated_result_row {
                        let uneliminated_result_datum = row.unpack_first();
                        assert_eq!(
                            uneliminated_result_datum, eval_result_datum,
                            "datums should not change if cast is eliminable"
                        );
                    }
                    // Check the consistency of `introduces_nulls` and
                    // `propagates_nulls` with `MirScalarExpr::typ`.
                    if let Some((introduces_nulls, propagates_nulls)) =
                        call_introduces_propagates_nulls(&mir)
                    {
                        if introduces_nulls {
                            // If the function introduces_nulls, then the return
                            // type should always be nullable, regardless of
                            // the nullability of the input types.
                            assert!(
                                mir_typ.nullable,
                                "fn named `{}` called on args `{:?}` (lowered to `{}`) yielded mir_typ.nullable: {}",
                                name, args, mir, mir_typ.nullable
                            );
                        } else {
                            let any_input_null = args.iter().any(|arg| arg.is_null());
                            if !any_input_null {
                                assert!(
                                    !mir_typ.nullable,
                                    "fn named `{}` called on args `{:?}` (lowered to `{}`) yielded mir_typ.nullable: {}",
                                    name, args, mir, mir_typ.nullable
                                );
                            } else if propagates_nulls {
                                // propagates_nulls means the optimizer short-circuits
                                // all-null inputs, so the output must be nullable.
                                assert!(
                                    mir_typ.nullable,
                                    "fn named `{}` called on args `{:?}` (lowered to `{}`) yielded mir_typ.nullable: {}",
                                    name, args, mir, mir_typ.nullable
                                );
                            }
                            // When propagates_nulls is false, the output may still
                            // be nullable if a non-nullable parameter received a null
                            // input (per-position null rejection). The is_instance_of
                            // check above ensures type consistency.
                        }
                    }
                    // Check that `MirScalarExpr::reduce` yields the same result
                    // as the real evaluation.
                    let mut reduced = mir.clone();
                    reduced.reduce(&[]);
                    match reduced {
                        MirScalarExpr::Literal(reduce_result, ctyp) => {
                            match reduce_result {
                                Ok(reduce_result_row) => {
                                    let reduce_result_datum = reduce_result_row.unpack_first();
                                    assert_eq!(
                                        reduce_result_datum,
                                        eval_result_datum,
                                        "eval/reduce datum mismatch: fn named `{}` called on args `{:?}` (lowered to `{}`) evaluated to `{}` with typ `{:?}`, but reduced to `{}` with typ `{:?}`",
                                        name,
                                        args,
                                        mir,
                                        eval_result_datum,
                                        mir_typ.scalar_type,
                                        reduce_result_datum,
                                        ctyp.scalar_type
                                    );
                                    // Let's check that the types also match.
                                    // (We are not checking nullability here,
                                    // because it's ok when we know a more
                                    // precise nullability after actually
                                    // evaluating a function than before.)
                                    assert_eq!(
                                        ctyp.scalar_type,
                                        mir_typ.scalar_type,
                                        "eval/reduce type mismatch: fn named `{}` called on args `{:?}` (lowered to `{}`) evaluated to `{}` with typ `{:?}`, but reduced to `{}` with typ `{:?}`",
                                        name,
                                        args,
                                        mir,
                                        eval_result_datum,
                                        mir_typ.scalar_type,
                                        reduce_result_datum,
                                        ctyp.scalar_type
                                    );
                                }
                                Err(..) => {} // It's ok, we might have given invalid args to the function
                            }
                        }
                        _ => unreachable!(
                            "all args are literals, so should have reduced to a literal"
                        ),
                    }
                }
            }
        }
    }
}

/// If the given MirScalarExpr
///  - is a function call, and
///  - all arguments are literals
/// then it returns whether the called function (introduces_nulls, propagates_nulls).
fn call_introduces_propagates_nulls(mir_func_call: &MirScalarExpr) -> Option<(bool, bool)> {
    match mir_func_call {
        MirScalarExpr::CallUnary { func, expr } => {
            if expr.is_literal() {
                Some((func.introduces_nulls(), func.propagates_nulls()))
            } else {
                None
            }
        }
        MirScalarExpr::CallBinary { func, expr1, expr2 } => {
            if expr1.is_literal() && expr2.is_literal() {
                Some((func.introduces_nulls(), func.propagates_nulls()))
            } else {
                None
            }
        }
        MirScalarExpr::CallVariadic { func, exprs } => {
            if exprs.iter().all(|arg| arg.is_literal()) {
                Some((func.introduces_nulls(), func.propagates_nulls()))
            } else {
                None
            }
        }
        _ => None,
    }
}

use mz_catalog::catalog::test_support::insert_synthetic_view_chain;
/// Read-then-write dependency validation walks the transitive `uses()` of
/// the read set. A deep chain of stacked views (user controlled, arbitrarily
/// deep) must be validated without overflowing the coordinator thread's
/// stack, so the traversal must not recurse.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
async fn validate_read_then_write_deep_chain_no_stack_overflow() {
    use crate::coord::read_then_write::{DependencyPolicy, validate_read_then_write_dependencies};

    Catalog::with_debug(|mut catalog| async move {
        // Deep enough that the previous recursive implementation overflowed
        // the stack.
        const DEPTH: usize = 100_000;
        const BASE: u64 = 1 << 40;
        insert_synthetic_view_chain(&mut catalog, BASE, DEPTH);

        // A generous bound so this test isolates the no-overflow property
        // rather than the dependency limit.
        validate_read_then_write_dependencies(
            &catalog,
            [CatalogItemId::User(BASE)],
            usize::MAX,
            DependencyPolicy::UserDml,
        )
        .expect("deep chain of user views is valid for read-then-write");

        catalog.expire().await;
    })
    .await
}

/// Read-then-write dependency validation is bounded: a read set with more
/// transitive dependencies than the limit is rejected with a clean error
/// rather than walking an unbounded graph.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
async fn validate_read_then_write_dependency_limit() {
    use crate::coord::read_then_write::{DependencyPolicy, validate_read_then_write_dependencies};
    use crate::error::AdapterError;

    Catalog::with_debug(|mut catalog| async move {
        const DEPTH: usize = 100;
        const BASE: u64 = 1 << 40;
        insert_synthetic_view_chain(&mut catalog, BASE, DEPTH);

        // The chain has DEPTH + 1 distinct objects (root plus DEPTH links).
        const OBJECTS: usize = DEPTH + 1;

        // Exactly at the limit is allowed.
        validate_read_then_write_dependencies(
            &catalog,
            [CatalogItemId::User(BASE)],
            OBJECTS,
            DependencyPolicy::UserDml,
        )
        .expect("chain at the limit is valid");

        // One below the limit is rejected with a clean error.
        let err = validate_read_then_write_dependencies(
            &catalog,
            [CatalogItemId::User(BASE)],
            OBJECTS - 1,
            DependencyPolicy::UserDml,
        )
        .expect_err("chain over the limit is rejected");
        assert!(matches!(
            err,
            AdapterError::ReadThenWriteDependencyLimitExceeded {
                max_rw_dependencies
            } if max_rw_dependencies == OBJECTS - 1
        ));

        catalog.expire().await;
    })
    .await
}
