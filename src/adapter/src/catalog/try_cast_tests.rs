// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests that `TRY_CAST` plans as `CAST` with every fallible stage wrapped,
//! and that the two agree on every value.

use std::collections::{BTreeMap, BTreeSet};

use itertools::Itertools;
use mz_expr::Eval;
use mz_repr::{Datum, RowArena, SqlRelationType, SqlScalarType};
use mz_sql::names::{
    DatabaseId, ItemQualifiers, QualifiedItemName, ResolvedDatabaseSpecifier, SchemaId,
    SchemaSpecifier,
};
use mz_sql::plan::{
    ExprContext, HirScalarExpr, PlanContext, QueryContext, QueryLifetime, Scope, StatementContext,
};
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
use mz_sql::session::vars::VarInput;

use crate::catalog::state::LocalExpressionCache;
use crate::catalog::{Catalog, Op};

/// `TRY_CAST` must plan as the strict cast with every unary stage wrapped
/// in `TryCast`, for every pair of types the strict cast supports, except
/// the SQL-implemented casts, which it must refuse with a dedicated error.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_try_cast_wraps_every_stage_of_every_cast() {
    use mz_expr::UnaryFunc;
    use mz_expr::visit::{Visit, VisitChildren};
    use mz_ore::collections::CollectionExt;
    use mz_repr::SqlScalarBaseType as B;
    use mz_repr::explain::ExprHumanizer;
    use mz_sql::plan::{HirRelationExpr, Params, Plan, PlanError, valid_cast_base_type_pairs};

    /// Calls `f` on every scalar expression node in `rel`, including those
    /// inside subqueries.
    fn for_each_scalar(rel: &HirRelationExpr, f: &mut impl FnMut(&HirScalarExpr)) {
        rel.visit_pre(&mut |rel: &HirRelationExpr| {
            rel.visit_children(|scalar: &HirScalarExpr| scalar.visit_pre(&mut *f));
        });
    }

    fn unary_calls(rel: &HirRelationExpr) -> Vec<UnaryFunc> {
        let mut funcs = vec![];
        for_each_scalar(rel, &mut |e| {
            if let HirScalarExpr::CallUnary { func, .. } = e {
                funcs.push(func.clone());
            }
        });
        funcs
    }

    fn has_subquery(rel: &HirRelationExpr) -> bool {
        let mut found = false;
        for_each_scalar(rel, &mut |e| {
            found |= matches!(e, HirScalarExpr::Exists(..) | HirScalarExpr::Select(..));
        });
        found
    }

    Catalog::with_debug(|mut catalog| async move {
        catalog
            .system_config_mut()
            .set("enable_try_cast", VarInput::Flat("on"))
            .expect("valid setting");
        // A record type can only be named in SQL through a user-defined
        // type, so create one to reach the record-to-record cast.
        let (id, gid) = catalog
            .allocate_user_id_for_test()
            .await
            .expect("allocate id");
        let item = catalog
            .state()
            .deserialize_item(
                gid,
                "CREATE TYPE materialize.public.rec AS (a pg_catalog.int8, b pg_catalog.text)",
                &BTreeMap::new(),
                &mut LocalExpressionCache::Closed,
                None,
            )
            .expect("valid type");
        let commit_ts = catalog.current_upper().await;
        catalog
            .transact(
                None,
                commit_ts,
                None,
                vec![Op::CreateItem {
                    item,
                    name: QualifiedItemName {
                        qualifiers: ItemQualifiers {
                            database_spec: ResolvedDatabaseSpecifier::Id(DatabaseId::User(1)),
                            schema_spec: SchemaSpecifier::Id(SchemaId::User(3)),
                        },
                        item: "rec".to_string(),
                    },
                    id,
                    owner_id: MZ_SYSTEM_ROLE_ID,
                }],
            )
            .await
            .expect("create type");
        let conn_catalog = catalog.for_system_session();
        let pcx = PlanContext::zero();
        let plan_select = |expr: std::string::String| -> Result<HirRelationExpr, PlanError> {
            let sql = format!("SELECT {expr}");
            let stmt = mz_sql_parser::parser::parse_statements(&sql)
                .expect("parses")
                .into_element()
                .ast;
            let (stmt, resolved_ids) = mz_sql::names::resolve(&conn_catalog, stmt)?;
            let params = Params::empty();
            let (plan, _) =
                mz_sql::plan::plan(Some(&pcx), &conn_catalog, stmt, &params, &resolved_ids)?;
            match plan {
                Plan::Select(plan) => Ok(plan.source),
                other => panic!("expected a SELECT plan, got {other:?}"),
            }
        };

        // `SqlScalarType::enumerate` has no instance of the parameterized
        // type families, nor of `name` and `aclitem`, so those come as
        // hand-written SQL. A source is an expression of the type; a target
        // is a type name. The anonymous record source has a field of a
        // different type than `rec`, so casting it there converts a field.
        let mut sources: Vec<(std::string::String, B)> = SqlScalarType::enumerate()
            .iter()
            .map(|ty| {
                let name = conn_catalog.humanize_sql_scalar_type(ty, false);
                (format!("NULL::{name}"), ty.into())
            })
            .collect();
        let mut targets: Vec<(std::string::String, B)> = SqlScalarType::enumerate()
            .iter()
            .map(|ty| (conn_catalog.humanize_sql_scalar_type(ty, false), ty.into()))
            .collect();
        let extra_types = [
            ("name", B::PgLegacyName),
            ("aclitem", B::AclItem),
            ("int4 list", B::List),
            ("int8 list", B::List),
            ("int2[]", B::Array),
            ("int4[]", B::Array),
            ("int8[]", B::Array),
            ("map[text=>int4]", B::Map),
            ("int4range", B::Range),
            ("materialize.public.rec", B::Record),
        ];
        for (name, base) in extra_types {
            sources.push((format!("NULL::{name}"), base));
            targets.push((name.to_string(), base));
        }
        sources.push(("ROW(1, 'a')".to_string(), B::Record));

        let mut exercised = BTreeSet::new();
        let mut unsupported = BTreeSet::new();
        let mut unplannable = BTreeSet::new();
        for (source, from) in &sources {
            for (to_name, to) in &targets {
                let strict = plan_select(format!("CAST({source} AS {to_name})"));
                let lenient = plan_select(format!("TRY_CAST({source} AS {to_name})"));
                let strict = match strict {
                    Ok(strict) => strict,
                    Err(PlanError::InvalidCast { .. }) => {
                        assert!(
                            matches!(lenient, Err(PlanError::InvalidCast { .. })),
                            "{source} to {to_name}: CAST is invalid, so TRY_CAST must be too, got {lenient:?}"
                        );
                        continue;
                    }
                    Err(_) => {
                        unplannable.insert((source.clone(), to_name.clone()));
                        continue;
                    }
                };
                let strict_funcs = unary_calls(&strict);
                assert!(
                    !strict_funcs.iter().any(|f| matches!(f, UnaryFunc::TryCast(_))),
                    "{source} to {to_name}: CAST must not produce TryCast, got {strict_funcs:?}"
                );
                exercised.insert((*from, *to));
                match lenient {
                    Ok(lenient) => {
                        // The expectation is checked stage by stage against
                        // `could_error` rather than built with
                        // `UnaryFunc::try_cast_if_fallible`, which the planner itself
                        // uses: an oracle built from the code under test
                        // would pass an always-wrap mutant.
                        let lenient_funcs = unary_calls(&lenient);
                        assert_eq!(
                            lenient_funcs.len(),
                            strict_funcs.len(),
                            "{source} to {to_name}: TRY_CAST must have one stage per CAST stage"
                        );
                        for (strict_stage, lenient_stage) in
                            strict_funcs.iter().zip_eq(&lenient_funcs)
                        {
                            match lenient_stage {
                                UnaryFunc::TryCast(wrapper) => assert!(
                                    strict_stage.could_error()
                                        && wrapper.inner() == strict_stage,
                                    "{source} to {to_name}: TRY_CAST wrapped {strict_stage}, which cannot error"
                                ),
                                bare => assert!(
                                    !strict_stage.could_error() && bare == strict_stage,
                                    "{source} to {to_name}: TRY_CAST left {strict_stage} unwrapped"
                                ),
                            }
                        }
                    }
                    Err(PlanError::TryCastUnsupported { .. }) => {
                        assert!(
                            has_subquery(&strict),
                            "{source} to {to_name}: TRY_CAST refused a cast that is not SQL-implemented"
                        );
                        unsupported.insert((*from, *to));
                    }
                    Err(err) => panic!("{source} to {to_name}: unexpected error {err}"),
                }
            }
        }
        assert!(unplannable.is_empty(), "casts that did not plan: {unplannable:?}");

        let table: BTreeSet<(B, B)> = valid_cast_base_type_pairs().collect();
        let unexercised: Vec<_> = table.difference(&exercised).collect();
        assert!(
            unexercised.is_empty(),
            "cast table entries the type lists above do not reach: {unexercised:?}"
        );

        // The string category routes through `text`, so each catalog-lookup
        // cast is refused for every string-like type.
        let string_like = [B::String, B::Char, B::VarChar, B::PgLegacyChar, B::PgLegacyName];
        let mut expected = BTreeSet::new();
        for s in string_like {
            for reg in [B::RegClass, B::RegProc, B::RegType] {
                expected.insert((s, reg));
                expected.insert((reg, s));
            }
            for acl in [B::AclItem, B::MzAclItem] {
                expected.insert((acl, s));
            }
        }
        expected.insert((B::AclItem, B::MzAclItem));
        expected.insert((B::MzAclItem, B::AclItem));
        assert_eq!(unsupported, expected);
    })
    .await;
}

/// For every type pair `TRY_CAST` supports and many values of the source
/// type, `TRY_CAST` agrees with `CAST` where `CAST` succeeds and is NULL
/// where `CAST` errors. It never errors itself.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn proptest_try_cast_agrees_with_cast() {
    use std::cell::Cell;

    use mz_repr::{ColumnName, arb_datum_for_scalar};
    use mz_sql::plan::{CastContext, plan_hypothetical_cast_with_failure_mode};
    use mz_sql_parser::ast::CastFailureMode;
    use proptest::test_runner::{Config, TestRunner};

    fn record(fields: &[(&str, SqlScalarType)]) -> SqlScalarType {
        SqlScalarType::Record {
            fields: fields
                .iter()
                .map(|(name, ty)| (ColumnName::from(*name), ty.clone().nullable(true)))
                .collect(),
            custom_id: None,
        }
    }

    Catalog::with_debug(|catalog| async move {
        let conn_catalog = catalog.for_system_session();
        let pcx = PlanContext::zero();
        let scx = StatementContext::new(Some(&pcx), &conn_catalog);
        let qcx = QueryContext::root(&scx, QueryLifetime::OneShot);
        let ecx = ExprContext {
            qcx: &qcx,
            name: "proptest_try_cast",
            scope: &Scope::empty(),
            relation_type: &SqlRelationType::empty(),
            allow_aggregates: false,
            allow_subqueries: true,
            allow_parameters: false,
            allow_windows: false,
        };
        let plan = |mode, from: &SqlScalarType, to: &SqlScalarType| {
            let ccx = CastContext::Explicit;
            plan_hypothetical_cast_with_failure_mode(&ecx, ccx, mode, from, to)
        };

        // `SqlScalarType::enumerate` has no instance of the parameterized
        // type families, nor of `name` and `aclitem`. Two widths of each
        // container so that the element casts are exercised.
        let mut types = SqlScalarType::enumerate().to_vec();
        types.extend([
            SqlScalarType::PgLegacyName,
            SqlScalarType::AclItem,
            SqlScalarType::List {
                element_type: Box::new(SqlScalarType::Int32),
                custom_id: None,
            },
            SqlScalarType::List {
                element_type: Box::new(SqlScalarType::Int64),
                custom_id: None,
            },
            SqlScalarType::Array(Box::new(SqlScalarType::Int32)),
            SqlScalarType::Array(Box::new(SqlScalarType::Int64)),
            SqlScalarType::Map {
                value_type: Box::new(SqlScalarType::Int32),
                custom_id: None,
            },
            SqlScalarType::Range {
                element_type: Box::new(SqlScalarType::Int32),
            },
            record(&[("a", SqlScalarType::Int32), ("b", SqlScalarType::String)]),
            record(&[("a", SqlScalarType::Int64), ("b", SqlScalarType::String)]),
        ]);

        let mut pairs = 0usize;
        // Counted from inside `check`, which the proptest runner requires
        // to be `Fn`, hence the `Cell`.
        let values = Cell::new(0usize);
        for from in &types {
            for to in &types {
                let Some(strict) = plan(CastFailureMode::Error, from, to) else {
                    continue;
                };
                let Some(lenient) = plan(CastFailureMode::NullFallback, from, to) else {
                    // The SQL-implemented casts, refused by the planner.
                    continue;
                };
                pairs += 1;
                let arena = RowArena::new();
                let check = |datum: Datum| {
                    values.set(values.get() + 1);
                    let row = [datum];
                    let strict_result = strict.eval(&row, &arena);
                    let lenient_result = lenient.eval(&row, &arena);
                    match (&strict_result, &lenient_result) {
                        (Ok(s), Ok(l)) => assert_eq!(
                            s, l,
                            "{from:?} to {to:?} on {datum}: TRY_CAST differs from CAST"
                        ),
                        (Err(_), Ok(Datum::Null)) => {}
                        (Err(err), Ok(l)) => panic!(
                            "{from:?} to {to:?} on {datum}: CAST errors ({err}) but TRY_CAST gives {l}"
                        ),
                        (_, Err(err)) => {
                            panic!("{from:?} to {to:?} on {datum}: TRY_CAST errored: {err}")
                        }
                    }
                };

                // Edge values, then NULL, then random values of the source
                // type.
                for datum in from.interesting_datums() {
                    check(datum);
                }
                check(Datum::Null);
                let mut runner = TestRunner::new(Config {
                    cases: 64,
                    ..Config::default()
                });
                runner
                    .run(&arb_datum_for_scalar(from.clone()), |pd| {
                        check(Datum::from(&pd));
                        Ok(())
                    })
                    .expect("oracle holds");

                // A random string almost never parses as the target type,
                // so for string sources also feed the rendering of random
                // target values, which exercises the success branch.
                let render = plan(CastFailureMode::Error, to, &SqlScalarType::String);
                if from.base_eq(&SqlScalarType::String) && let Some(render) = render {
                    runner
                        .run(&arb_datum_for_scalar(to.clone()), |pd| {
                            if let Ok(s) = render.eval(&[Datum::from(&pd)], &arena) {
                                check(s);
                            }
                            Ok(())
                        })
                        .expect("oracle holds");
                }
            }
        }
        assert!(pairs > 200, "only {pairs} pairs exercised");
        assert!(values.get() > 10_000, "only {} values checked", values.get());
    })
    .await;
}
