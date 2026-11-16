// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The `mz_internal.mz_inferred_relationships` materialized view.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use mz_pgrepr::oid;
use mz_repr::namespaces::MZ_INTERNAL_SCHEMA;
use mz_repr::{RelationDesc, SqlScalarType};

use super::{BuiltinIndex, BuiltinMaterializedView, PUBLIC_SELECT};

/// Relationships between user relations that their views' definitions imply,
/// with no declared keys.
///
/// Evidence is of two kinds, both read by `parse_catalog_view_joins`:
///
/// 1. A join: two relations' columns a view equates. The referenced side is
///    a column that alone keys an index, then a column named `id`, then the
///    side not named `*_id`. A join neither settles is kept undirected.
/// 2. A passed-through column: a view's output column that is an input
///    column unchanged. An output that passes a key references that key.
///    An output that passes any other column holds the same values as it, so
///    a reference either has, the other has too. This runs to a fixed point,
///    through views over views.
///
/// The second is a claim about the columns' domain, not their data: a view
/// may filter, so its input column can hold values its output never shows.
pub static MZ_INFERRED_RELATIONSHIPS: LazyLock<BuiltinMaterializedView> = LazyLock::new(|| {
    BuiltinMaterializedView {
        name: "mz_inferred_relationships",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::MV_MZ_INFERRED_RELATIONSHIPS_OID,
        desc: RelationDesc::builder()
            .with_column("source_id", SqlScalarType::String.nullable(false))
            .with_column("source_column", SqlScalarType::String.nullable(false))
            .with_column("target_id", SqlScalarType::String.nullable(false))
            .with_column("target_column", SqlScalarType::String.nullable(false))
            .with_column("is_directed", SqlScalarType::Bool.nullable(false))
            .with_column(
                "inferred_from",
                SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(false),
            )
            .with_key(vec![0, 1, 2, 3])
            .finish(),
        column_comments: BTreeMap::from_iter([
            (
                "source_id",
                "The ID of the relation holding the referencing column. Corresponds to `mz_objects.id`.",
            ),
            ("source_column", "The referencing column."),
            (
                "target_id",
                "The ID of the relation holding the referenced column. Corresponds to `mz_objects.id`.",
            ),
            ("target_column", "The referenced column."),
            (
                "is_directed",
                "Whether a key decides which side references the other. When false, the two columns are only known to be joined, and their order is arbitrary.",
            ),
            (
                "inferred_from",
                "The IDs of the views and materialized views whose definitions imply the relationship.",
            ),
        ]),
        sql: "
IN CLUSTER mz_catalog_server
WITH (
    ASSERT NOT NULL source_id,
    ASSERT NOT NULL source_column,
    ASSERT NOT NULL target_id,
    ASSERT NOT NULL target_column,
    ASSERT NOT NULL is_directed,
    ASSERT NOT NULL inferred_from
) AS
WITH
    parsed AS (
        SELECT id AS view_id, mz_internal.parse_catalog_view_joins(create_sql) AS j
        FROM (
            SELECT id, create_sql FROM mz_catalog.mz_views WHERE id LIKE 'u%'
            UNION ALL
            SELECT id, create_sql FROM mz_catalog.mz_materialized_views WHERE id LIKE 'u%'
        )
    ),
    user_columns AS (
        SELECT id, name FROM mz_catalog.mz_columns WHERE id LIKE 'u%'
    ),
    -- Columns that alone key an index. Indexes do not enforce uniqueness, so
    -- this only hints at which side of a join is the key.
    index_keys AS (
        SELECT DISTINCT i.on_id AS id, c.name
        FROM mz_catalog.mz_indexes i
        JOIN mz_catalog.mz_index_columns ic ON ic.index_id = i.id
        JOIN mz_catalog.mz_columns c ON c.id = i.on_id AND c.position = ic.on_position
        WHERE i.on_id LIKE 'u%' AND i.id IN (
            SELECT index_id FROM mz_catalog.mz_index_columns
            GROUP BY index_id HAVING count(*) = 1
        )
    ),
    using_owners AS (
        SELECT u.view_id, u.right_id, u.name, u.candidate
        FROM (
            SELECT
                view_id,
                x.e->>'right_id' AS right_id,
                x.e->>'column' AS name,
                c.candidate
            FROM parsed,
                jsonb_array_elements(j->'using') AS x(e),
                jsonb_array_elements_text(x.e->'candidates') AS c(candidate)
        ) u
        JOIN user_columns c ON c.id = u.candidate AND c.name = u.name
    ),
    -- Pairs of equated columns, the left side of a USING join being the one
    -- earlier relation that has the column.
    equated AS (
        SELECT
            view_id,
            p.e->>'left_id' AS a_id,
            p.e->>'left_column' AS a_col,
            p.e->>'right_id' AS b_id,
            p.e->>'right_column' AS b_col
        FROM parsed, jsonb_array_elements(j->'joins') AS p(e)
        UNION ALL
        SELECT view_id, min(candidate), name, right_id, name
        FROM using_owners
        GROUP BY view_id, right_id, name
        HAVING count(DISTINCT candidate) = 1
    ),
    scored AS (
        SELECT
            e.*,
            CASE WHEN ka.id IS NOT NULL THEN 2 WHEN e.a_col = 'id' THEN 1 ELSE 0 END
                - CASE WHEN kb.id IS NOT NULL THEN 2 WHEN e.b_col = 'id' THEN 1 ELSE 0 END
                AS a_over_b,
            (e.b_col ~ '.+_id$')::int - (e.a_col ~ '.+_id$')::int AS a_by_name
        FROM equated e
        JOIN user_columns ca ON ca.id = e.a_id AND ca.name = e.a_col
        JOIN user_columns cb ON cb.id = e.b_id AND cb.name = e.b_col
        LEFT JOIN index_keys ka ON ka.id = e.a_id AND ka.name = e.a_col
        LEFT JOIN index_keys kb ON kb.id = e.b_id AND kb.name = e.b_col
        WHERE e.a_id <> e.b_id
    ),
    -- `a_first` is positive when `a` is the referenced side.
    oriented AS (
        SELECT
            view_id, a_id, a_col, b_id, b_col,
            CASE WHEN a_over_b <> 0 THEN a_over_b ELSE a_by_name END AS a_first
        FROM scored
    ),
    joins AS (
        SELECT
            view_id,
            CASE WHEN keep THEN b_id ELSE a_id END AS source_id,
            CASE WHEN keep THEN b_col ELSE a_col END AS source_column,
            CASE WHEN keep THEN a_id ELSE b_id END AS target_id,
            CASE WHEN keep THEN a_col ELSE b_col END AS target_column,
            a_first <> 0 AS is_directed
        FROM (
            SELECT
                *,
                -- Undirected pairs are ordered by name so each appears once.
                a_first > 0 OR (a_first = 0 AND a_id || '.' || a_col > b_id || '.' || b_col) AS keep
            FROM oriented
        )
    ),
    passed AS (
        SELECT
            view_id,
            o.e->>'column' AS output_column,
            o.e->>'source_id' AS source_id,
            o.e->>'source_column' AS source_column
        FROM parsed, jsonb_array_elements(j->'outputs') AS o(e)
    ),
    passed_keys AS (
        SELECT p.*
        FROM passed p
        JOIN user_columns c ON c.id = p.source_id AND c.name = p.source_column
        WHERE p.source_column = 'id' OR EXISTS (
            SELECT 1 FROM index_keys k
            WHERE k.id = p.source_id AND k.name = p.source_column
        )
    ),
    -- Passed columns that are not keys. A passed key references its source
    -- directly, and propagating through it too would add every key it
    -- reaches transitively, which grows with the square of a view stack's
    -- depth (a stack of 300 views passing `id` gave 45,150 rows).
    passed_references AS (
        SELECT p.*
        FROM passed p
        WHERE NOT EXISTS (
            SELECT 1 FROM passed_keys k
            WHERE k.view_id = p.view_id AND k.output_column = p.output_column
        )
    ),
    directed AS (
        WITH MUTUALLY RECURSIVE
            refs (
                view_id text,
                source_id text,
                source_column text,
                target_id text,
                target_column text
            ) AS (
                SELECT view_id, source_id, source_column, target_id, target_column
                FROM joins WHERE is_directed
                UNION
                SELECT view_id, view_id, output_column, source_id, source_column
                FROM passed_keys
                WHERE view_id <> source_id
                UNION
                SELECT p.view_id, p.view_id, p.output_column, r.target_id, r.target_column
                FROM passed_references p
                JOIN refs r ON r.source_id = p.source_id AND r.source_column = p.source_column
                WHERE p.view_id <> r.target_id
                UNION
                SELECT p.view_id, p.source_id, p.source_column, r.target_id, r.target_column
                FROM passed_references p
                JOIN refs r ON r.source_id = p.view_id AND r.source_column = p.output_column
                WHERE p.source_id <> r.target_id
            )
        SELECT * FROM refs
    ),
    undirected AS (
        SELECT j.view_id, j.source_id, j.source_column, j.target_id, j.target_column
        FROM joins j
        WHERE NOT j.is_directed AND NOT EXISTS (
            SELECT 1 FROM directed d
            WHERE (
                d.source_id = j.source_id AND d.source_column = j.source_column
                AND d.target_id = j.target_id AND d.target_column = j.target_column
            ) OR (
                d.source_id = j.target_id AND d.source_column = j.target_column
                AND d.target_id = j.source_id AND d.target_column = j.source_column
            )
        )
    ),
    evidence AS (
        SELECT DISTINCT *, true AS is_directed FROM directed
        UNION ALL
        SELECT DISTINCT *, false AS is_directed FROM undirected
    )
SELECT
    source_id,
    source_column,
    target_id,
    target_column,
    is_directed,
    array_agg(view_id ORDER BY view_id) AS inferred_from
FROM evidence
WHERE source_id LIKE 'u%' AND target_id LIKE 'u%'
GROUP BY source_id, source_column, target_id, target_column, is_directed",
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
});

pub const MZ_INFERRED_RELATIONSHIPS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_inferred_relationships_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_INFERRED_RELATIONSHIPS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_inferred_relationships (source_id)",
    is_retained_metrics_object: false,
};
