// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;

use tempfile::TempDir;

use mz_coord::catalog::Catalog;
use mz_ore::collections::CollectionExt;
use mz_ore::now::NOW_ZERO;
use mz_repr::ScalarType;
use mz_sql::plan::PlanContext;

#[tokio::test]
async fn test_parameter_type_inference() -> Result<(), Box<dyn Error>> {
    let test_cases = vec![
        (
            "SELECT $1, $2, $3",
            vec![ScalarType::String, ScalarType::String, ScalarType::String],
        ),
        (
            "VALUES($1, $2, $3)",
            vec![ScalarType::String, ScalarType::String, ScalarType::String],
        ),
        (
            "SELECT 1 GROUP BY $1, $2, $3",
            vec![ScalarType::String, ScalarType::String, ScalarType::String],
        ),
        (
            "SELECT 1 ORDER BY $1, $2, $3",
            vec![ScalarType::String, ScalarType::String, ScalarType::String],
        ),
        (
            "SELECT ($1), (((($2))))",
            vec![ScalarType::String, ScalarType::String],
        ),
        ("SELECT $1::pg_catalog.int4", vec![ScalarType::Int32]),
        ("SELECT 1 WHERE $1", vec![ScalarType::Bool]),
        ("SELECT 1 HAVING $1", vec![ScalarType::Bool]),
        (
            "SELECT 1 FROM (VALUES (1)) a JOIN (VALUES (1)) b ON $1",
            vec![ScalarType::Bool],
        ),
        (
            "SELECT CASE WHEN $1 THEN 1 ELSE 0 END",
            vec![ScalarType::Bool],
        ),
        (
            "SELECT CASE WHEN true THEN $1 ELSE $2 END",
            vec![ScalarType::String, ScalarType::String],
        ),
        (
            "SELECT CASE WHEN true THEN $1 ELSE 1 END",
            vec![ScalarType::Int32],
        ),
        ("SELECT pg_catalog.abs($1)", vec![ScalarType::Float64]),
        ("SELECT pg_catalog.ascii($1)", vec![ScalarType::String]),
        (
            "SELECT coalesce($1, $2, $3)",
            vec![ScalarType::String, ScalarType::String, ScalarType::String],
        ),
        ("SELECT coalesce($1, 1)", vec![ScalarType::Int32]),
        (
            "SELECT pg_catalog.substr($1, $2)",
            vec![ScalarType::String, ScalarType::Int64],
        ),
        (
            "SELECT pg_catalog.substring($1, $2)",
            vec![ScalarType::String, ScalarType::Int64],
        ),
        (
            "SELECT $1 LIKE $2",
            vec![ScalarType::String, ScalarType::String],
        ),
        ("SELECT NOT $1", vec![ScalarType::Bool]),
        ("SELECT $1 AND $2", vec![ScalarType::Bool, ScalarType::Bool]),
        ("SELECT $1 OR $2", vec![ScalarType::Bool, ScalarType::Bool]),
        ("SELECT +$1", vec![ScalarType::Float64]),
        ("SELECT $1 < 1", vec![ScalarType::Int32]),
        (
            "SELECT $1 < $2",
            vec![ScalarType::String, ScalarType::String],
        ),
        ("SELECT $1 + 1", vec![ScalarType::Int32]),
        (
            "SELECT $1 + 1.0",
            vec![ScalarType::Numeric { max_scale: None }],
        ),
        (
            "SELECT '1970-01-01 00:00:00'::pg_catalog.timestamp + $1",
            vec![ScalarType::Interval],
        ),
        (
            "SELECT $1 + '1970-01-01 00:00:00'::pg_catalog.timestamp",
            vec![ScalarType::Interval],
        ),
        (
            "SELECT $1::pg_catalog.int4, $1 + $2",
            vec![ScalarType::Int32, ScalarType::Int32],
        ),
        (
            "SELECT '[0, 1, 2]'::pg_catalog.jsonb - $1",
            vec![ScalarType::String],
        ),
    ];

    let data_dir = TempDir::new()?;
    let catalog = Catalog::open_debug_sqlite(data_dir.path(), NOW_ZERO.clone()).await?;
    let catalog = catalog.for_system_session();
    for (sql, types) in test_cases {
        let stmt = mz_sql::parse::parse(sql)?.into_element();
        let desc = mz_sql::plan::describe(&PlanContext::zero(), &catalog, stmt, &[])?;
        assert_eq!(desc.param_types, types);
    }
    Ok(())
}
