// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![recursion_limit = "256"]

use mz_adapter::catalog::Catalog;
use mz_ore::collections::CollectionExt;
use mz_repr::{Datum, Row, SqlScalarType};
use mz_sql::plan::{Params, Plan, PlanContext};

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_parameter_type_inference() {
    let test_cases = vec![
        (
            "SELECT $1, $2, $3",
            vec![
                SqlScalarType::String,
                SqlScalarType::String,
                SqlScalarType::String,
            ],
        ),
        (
            "VALUES($1, $2, $3)",
            vec![
                SqlScalarType::String,
                SqlScalarType::String,
                SqlScalarType::String,
            ],
        ),
        (
            "SELECT 1 GROUP BY $1, $2, $3",
            vec![
                SqlScalarType::String,
                SqlScalarType::String,
                SqlScalarType::String,
            ],
        ),
        (
            "SELECT 1 ORDER BY $1, $2, $3",
            vec![
                SqlScalarType::String,
                SqlScalarType::String,
                SqlScalarType::String,
            ],
        ),
        (
            "SELECT ($1), (((($2))))",
            vec![SqlScalarType::String, SqlScalarType::String],
        ),
        ("SELECT $1::pg_catalog.int4", vec![SqlScalarType::Int32]),
        ("SELECT 1 WHERE $1", vec![SqlScalarType::Bool]),
        ("SELECT 1 HAVING $1", vec![SqlScalarType::Bool]),
        (
            "SELECT 1 FROM (VALUES (1)) a JOIN (VALUES (1)) b ON $1",
            vec![SqlScalarType::Bool],
        ),
        (
            "SELECT CASE WHEN $1 THEN 1 ELSE 0 END",
            vec![SqlScalarType::Bool],
        ),
        (
            "SELECT CASE WHEN true THEN $1 ELSE $2 END",
            vec![SqlScalarType::String, SqlScalarType::String],
        ),
        (
            "SELECT CASE WHEN true THEN $1 ELSE 1 END",
            vec![SqlScalarType::Int32],
        ),
        ("SELECT pg_catalog.abs($1)", vec![SqlScalarType::Float64]),
        ("SELECT pg_catalog.ascii($1)", vec![SqlScalarType::String]),
        (
            "SELECT coalesce($1, $2, $3)",
            vec![
                SqlScalarType::String,
                SqlScalarType::String,
                SqlScalarType::String,
            ],
        ),
        ("SELECT coalesce($1, 1)", vec![SqlScalarType::Int32]),
        (
            "SELECT pg_catalog.substr($1, $2)",
            vec![SqlScalarType::String, SqlScalarType::Int32],
        ),
        (
            "SELECT pg_catalog.substring($1, $2)",
            vec![SqlScalarType::String, SqlScalarType::Int32],
        ),
        (
            "SELECT $1 LIKE $2",
            vec![SqlScalarType::String, SqlScalarType::String],
        ),
        ("SELECT NOT $1", vec![SqlScalarType::Bool]),
        (
            "SELECT $1 AND $2",
            vec![SqlScalarType::Bool, SqlScalarType::Bool],
        ),
        (
            "SELECT $1 OR $2",
            vec![SqlScalarType::Bool, SqlScalarType::Bool],
        ),
        ("SELECT +$1", vec![SqlScalarType::Float64]),
        ("SELECT $1 < 1", vec![SqlScalarType::Int32]),
        (
            "SELECT $1 < $2",
            vec![SqlScalarType::String, SqlScalarType::String],
        ),
        ("SELECT $1 + 1", vec![SqlScalarType::Int32]),
        (
            "SELECT $1 + 1.0",
            vec![SqlScalarType::Numeric { max_scale: None }],
        ),
        (
            "SELECT '1970-01-01 00:00:00'::pg_catalog.timestamp + $1",
            vec![SqlScalarType::Interval],
        ),
        (
            "SELECT $1 + '1970-01-01 00:00:00'::pg_catalog.timestamp",
            vec![SqlScalarType::Interval],
        ),
        (
            "SELECT $1::pg_catalog.int4, $1 + $2",
            vec![SqlScalarType::Int32, SqlScalarType::Int32],
        ),
        (
            "SELECT '[0, 1, 2]'::pg_catalog.jsonb - $1",
            vec![SqlScalarType::String],
        ),
    ];

    Catalog::with_debug(|catalog| async move {
        let conn_catalog = catalog.for_system_session();
        for (sql, types) in test_cases {
            let stmt = mz_sql::parse::parse(sql).unwrap().into_element().ast;
            let (stmt, _) = mz_sql::names::resolve(&conn_catalog, stmt).unwrap();
            let analysis = mz_sql::plan::describe_analyzed(
                &PlanContext::zero(),
                &conn_catalog,
                stmt.clone(),
                &[],
            )
            .unwrap();
            let desc =
                mz_sql::plan::describe(&PlanContext::zero(), &conn_catalog, stmt, &[]).unwrap();
            assert_eq!(desc.param_types, types);
            assert_eq!(analysis.desc, desc);
            assert!(analysis.select.is_some());
        }
        catalog.expire().await;
    })
    .await
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn test_analyzed_select_binding() {
    Catalog::with_debug(|catalog| async move {
        let conn_catalog = catalog.for_system_session();
        let pcx = PlanContext::zero();
        for sql in [
            "SELECT $1::int4 + x FROM (VALUES (1), (2)) t(x) WHERE x = $2::int4 ORDER BY 1 LIMIT $3 OFFSET $4",
            "SELECT CASE WHEN $1::int4 = 0 THEN 0 ELSE $2::int4 / $1 END LIMIT $3 OFFSET $4",
            "SELECT x FROM (VALUES (1), (2)) t(x) WHERE x IN (SELECT $1::int4) AND x > $2::int4 LIMIT $3 OFFSET $4",
            "SELECT current_timestamp, current_user, $1::int4, $2::int4 LIMIT $3 OFFSET $4",
            "SELECT $1::int4, $2::int4 LIMIT $3 OFFSET $4 AS OF 0",
            "SELECT x FROM (VALUES (1), (2)) t(x) WHERE EXISTS (SELECT 1 FROM (VALUES (1), (2)) u(y) WHERE y = x LIMIT $1 OFFSET $2) LIMIT $3 OFFSET $4",
        ] {
            let stmt = mz_sql::parse::parse(sql).unwrap().into_element().ast;
            let (stmt, resolved_ids) = mz_sql::names::resolve(&conn_catalog, stmt).unwrap();
            // Supplied int4 types also exercise casts to the bigint finishing types.
            let analysis = mz_sql::plan::describe_analyzed(
                &pcx, &conn_catalog, stmt.clone(), &vec![Some(SqlScalarType::Int32); 4],
            ).unwrap();
            let analyzed = analysis.select.as_ref().unwrap();
            let unbound = format!("{analyzed:?}");
            for values in [
                [Some(1), Some(1), Some(2), Some(0)],
                [Some(2), Some(2), Some(1), Some(1)],
                [None, Some(2), None, Some(0)],
                [None, Some(2), None, None],
                [Some(0), Some(2), Some(0), Some(0)],
                [Some(1), Some(1), Some(-1), Some(0)],
                [Some(1), Some(1), Some(1), Some(-1)],
            ] {
                let params = Params {
                    datums: Row::pack(values.map(|v| v.map(Datum::Int32).unwrap_or(Datum::Null))),
                    execute_types: vec![SqlScalarType::Int32; 4],
                    expected_types: analysis.desc.param_types.clone(),
                };
                let bound = mz_sql::plan::plan_analyzed_select(
                    &pcx, &conn_catalog, analyzed, &params, &resolved_ids,
                );
                let custom = mz_sql::plan::plan(
                    Some(&pcx), &conn_catalog, stmt.clone(), &params, &resolved_ids,
                );
                // A parameter-bound NULL OFFSET currently errors in offset_into_value.
                assert_eq!(bound.is_ok(), values[2].is_none_or(|v| v >= 0) && values[3].is_some_and(|v| v >= 0), "{sql}: {values:?}: {bound:?} / {custom:?}");
                match (bound, custom) {
                    (Ok((bound, binding_ids)), Ok((Plan::Select(custom), sql_impl_ids))) => {
                        assert!(!bound.source.contains_parameters().unwrap());
                        assert_eq!(bound.source, custom.source, "{sql}: {values:?}");
                        assert_eq!(bound.finishing, custom.finishing, "{sql}: {values:?}");
                        assert_eq!(format!("{:?}", bound.when), format!("{:?}", custom.when));
                        let mut all_impl_ids = analysis.sql_impl_ids.clone();
                        all_impl_ids.extend_from(&binding_ids);
                        assert_eq!(all_impl_ids, sql_impl_ids);
                    }
                    (Err(bound), Err(custom)) => {
                        assert_eq!(bound.to_string(), custom.to_string(), "{sql}: {values:?}");
                    }
                    (bound, custom) => panic!("binding diverged for {sql}: {values:?}: {bound:?} / {custom:?}"),
                }
                assert_eq!(format!("{analyzed:?}"), unbound, "binding mutated retained analysis");
            }
        }
        for sql in ["SELECT pg_cancel_backend(0)", "SHOW search_path"] {
            let stmt = mz_sql::parse::parse(sql).unwrap().into_element().ast;
            let (stmt, _) = mz_sql::names::resolve(&conn_catalog, stmt).unwrap();
            let analysis = mz_sql::plan::describe_analyzed(&pcx, &conn_catalog, stmt, &[]).unwrap();
            assert!(analysis.select.is_none(), "unexpected reusable SELECT for {sql}");
        }
        catalog.expire().await;
    }).await
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn test_analyzed_select_inferred_record_comparison() {
    Catalog::with_debug(|catalog| async move {
        let conn_catalog = catalog.for_system_session();
        let pcx = PlanContext::zero();
        let stmt = mz_sql::parse::parse("SELECT ROW(1, 2) = $1")
            .unwrap()
            .into_element()
            .ast;
        let (stmt, ids) = mz_sql::names::resolve(&conn_catalog, stmt).unwrap();
        let analysis =
            mz_sql::plan::describe_analyzed(&pcx, &conn_catalog, stmt.clone(), &[]).unwrap();
        let params = Params {
            datums: Row::pack([Datum::String("(1,2)")]),
            expected_types: analysis.desc.param_types.clone(),
            execute_types: analysis.desc.param_types.clone(),
        };
        let custom =
            mz_sql::plan::plan(Some(&pcx), &conn_catalog, stmt, &params, &ids).unwrap_err();
        assert!(custom.to_string().contains("operator does not exist"));
        assert!(
            analysis.select.is_none(),
            "invalid typed analysis was retained"
        );
        catalog.expire().await;
    })
    .await;
}
