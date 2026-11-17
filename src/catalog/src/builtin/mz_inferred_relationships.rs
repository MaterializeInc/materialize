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

use super::{BuiltinIndex, BuiltinMaterializedView, BuiltinView, PUBLIC_SELECT};

/// Relationships between user relations that their views' definitions imply,
/// with no declared keys.
///
/// Every row comes from a single view's definition, read by
/// `parse_catalog_view_joins`, so the view stays linear in the number of
/// views:
///
/// - `join`: two relations' columns a view equates. The referenced side is a
///   column that alone keys an index, then a column named `id`, then the side
///   not named `*_id`. A join neither settles is undirected.
/// - `key`: a view's output column that is an input key unchanged, which
///   references that key.
/// - `passthrough`: a view's output column that is any other input column
///   unchanged. The two share a domain, not necessarily data, since the view
///   may filter, so this is no relationship by itself. `mz_mcp_relationships`
///   follows these rows to derive references.
///
/// Relationships across several views follow by chaining rows. Deriving them
/// here was measured at about three times the maintenance cost of
/// `mz_object_dependencies` per replaced view, nearly all of it the
/// fixpoint, for no information the chained rows lack.
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
            .with_column("kind", SqlScalarType::String.nullable(false))
            .with_column("is_directed", SqlScalarType::Bool.nullable(false))
            .with_column(
                "inferred_from",
                SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(false),
            )
            .with_key(vec![0, 1, 2, 3, 4])
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
                "kind",
                "`join` when a view joins the two columns, `key` when a view passes the target key through as the source column, and `passthrough` when a view passes any other target column through as the source column, which relates nothing by itself.",
            ),
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
    ASSERT NOT NULL kind,
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
    passed_columns AS (
        SELECT
            p.*,
            p.source_column = 'id' OR k.id IS NOT NULL AS is_key
        FROM passed p
        LEFT JOIN index_keys k ON k.id = p.source_id AND k.name = p.source_column
        WHERE p.view_id <> p.source_id
    ),
    -- Direction depends only on the two columns, so a pair of columns is
    -- directed in every view that joins it or in none, and appears once.
    evidence AS (
        SELECT view_id, source_id, source_column, target_id, target_column,
            'join' AS kind, is_directed
        FROM joins
        UNION ALL
        SELECT view_id, view_id, output_column, source_id, source_column,
            CASE WHEN is_key THEN 'key' ELSE 'passthrough' END, true
        FROM passed_columns
    )
SELECT
    source_id,
    source_column,
    target_id,
    target_column,
    kind,
    is_directed,
    array_agg(view_id ORDER BY view_id) AS inferred_from
FROM (SELECT DISTINCT * FROM evidence)
WHERE source_id LIKE 'u%' AND target_id LIKE 'u%'
GROUP BY source_id, source_column, target_id, target_column, kind, is_directed",
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

/// The relationships between relations the session's role can read, shaped
/// like an ontology table (`/docs/transform-data/patterns/ontology/`), for
/// agents.
///
/// Besides the `join` and `key` rows of `mz_inferred_relationships`, it lists
/// `derived` references: a column holding the same values as a referencing
/// column, through a chain of views passing it, references the same key. The
/// chains are walked here, on read, rather than maintained in
/// `mz_inferred_relationships`. Maintaining them was measured at 3 to 5.5
/// times that view's cost per replaced view on a one-worker catalog server,
/// against about 150 ms per read here with 4,000 views.
///
/// Rows are limited to relations the role holds SELECT on, so the view can be
/// read from a session with `restrict_to_user_objects` without describing
/// relations the role cannot see.
pub static MZ_MCP_RELATIONSHIPS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_mcp_relationships",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_MCP_RELATIONSHIPS_OID,
    desc: RelationDesc::builder()
        .with_column("table_name", SqlScalarType::String.nullable(false))
        .with_column(
            "columns",
            SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(false),
        )
        .with_column("referenced_table", SqlScalarType::String.nullable(false))
        .with_column(
            "referenced_columns",
            SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(false),
        )
        .with_column("kind", SqlScalarType::String.nullable(false))
        .with_column("is_directed", SqlScalarType::Bool.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "table_name",
            "Fully qualified name of the relation holding the referencing columns.",
        ),
        ("columns", "The referencing columns."),
        (
            "referenced_table",
            "Fully qualified name of the relation holding the referenced columns.",
        ),
        (
            "referenced_columns",
            "The referenced columns, paired by position with `columns`.",
        ),
        (
            "kind",
            "`join` when a view joins the columns, `key` when a view passes the referenced key through, and `derived` when the referencing column holds the same values as a column that references the key.",
        ),
        (
            "is_directed",
            "Whether a key decides which side references the other. When false, the columns are only known to be joined.",
        ),
    ]),
    sql: r#"
WITH MUTUALLY RECURSIVE
    -- Each passed-through column's root, by pointer doubling: a pointer jumps
    -- to where the pointer it lands on points.
    pointers (child_id text, child_column text, root_id text, root_column text) AS (
        SELECT
            s.child_id,
            s.child_column,
            COALESCE(jump.root_id, up.target_id, s.at_id),
            COALESCE(jump.root_column, up.target_column, s.at_column)
        FROM (
            SELECT
                e.source_id AS child_id,
                e.source_column AS child_column,
                COALESCE(p.root_id, e.target_id) AS at_id,
                COALESCE(p.root_column, e.target_column) AS at_column
            FROM mz_internal.mz_inferred_relationships e
            LEFT JOIN pointers p
                ON p.child_id = e.source_id AND p.child_column = e.source_column
            WHERE e.kind = 'passthrough'
        ) s
        LEFT JOIN pointers jump
            ON jump.child_id = s.at_id AND jump.child_column = s.at_column
        LEFT JOIN mz_internal.mz_inferred_relationships up
            ON up.kind = 'passthrough'
            AND up.source_id = s.at_id AND up.source_column = s.at_column
    ),
    members (id text, name text, root_id text, root_column text) AS (
        SELECT child_id, child_column, root_id, root_column FROM pointers
        UNION
        SELECT root_id, root_column, root_id, root_column FROM pointers
    )
SELECT
    src.qualified_name AS table_name,
    ARRAY[r.source_column] AS columns,
    tgt.qualified_name AS referenced_table,
    ARRAY[r.target_column] AS referenced_columns,
    r.kind,
    r.is_directed
FROM (
    SELECT source_id, source_column, target_id, target_column, kind, is_directed
    FROM mz_internal.mz_inferred_relationships
    WHERE kind IN ('join', 'key')
    UNION ALL
    SELECT DISTINCT m2.id, m2.name, r.target_id, r.target_column, 'derived', true
    FROM mz_internal.mz_inferred_relationships r
    JOIN members m1 ON m1.id = r.source_id AND m1.name = r.source_column
    JOIN members m2 ON m2.root_id = m1.root_id AND m2.root_column = m1.root_column
    WHERE r.kind IN ('join', 'key') AND r.is_directed
        AND NOT (m2.id = r.source_id AND m2.name = r.source_column)
        AND m2.id <> r.target_id
        AND NOT EXISTS (
            SELECT 1 FROM mz_internal.mz_inferred_relationships d
            WHERE d.kind IN ('join', 'key')
                AND d.source_id = m2.id AND d.source_column = m2.name
                AND d.target_id = r.target_id AND d.target_column = r.target_column
        )
) r
JOIN (
    SELECT DISTINCT
        o.id,
        '"' || op.database || '"."' || op.schema || '"."' || op.name || '"' AS qualified_name
    FROM mz_internal.mz_show_my_object_privileges op
    JOIN mz_objects o ON op.name = o.name AND op.object_type = o.type
    JOIN mz_schemas s ON s.name = op.schema AND s.id = o.schema_id
    JOIN mz_databases d ON d.name = op.database AND d.id = s.database_id
    WHERE op.privilege_type = 'SELECT'
) src ON src.id = r.source_id
JOIN (
    SELECT DISTINCT
        o.id,
        '"' || op.database || '"."' || op.schema || '"."' || op.name || '"' AS qualified_name
    FROM mz_internal.mz_show_my_object_privileges op
    JOIN mz_objects o ON op.name = o.name AND op.object_type = o.type
    JOIN mz_schemas s ON s.name = op.schema AND s.id = o.schema_id
    JOIN mz_databases d ON d.name = op.database AND d.id = s.database_id
    WHERE op.privilege_type = 'SELECT'
) tgt ON tgt.id = r.target_id
"#,
    access: vec![PUBLIC_SELECT],
    ontology: None,
});
