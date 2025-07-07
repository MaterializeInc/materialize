-- ############################################################
-- MCP Tools Definition Query
-- ############################################################
--
-- This SQL query dynamically discovers all user-accessible indexes in your Materialize instance
-- and translates each into a “tool” for the Model Context Protocol (MCP).
-- For each index, it:
--   1. Builds a unique tool name based on the database, schema, and index name.
--   2. Gathers privileges to ensure only SELECT- and USAGE-permitted objects are included.
--   3. Retrieves object and column comments to annotate the tool with human-readable descriptions.
--   4. Constructs a JSON Schema for the tool’s input parameters (key columns) with
--      appropriate types (number, string, boolean, date, etc.) and descriptions.
--   5. Lists the remaining indexed columns as the tool’s output values.
--
-- The final output is one row per index containing:
--   - name:          Unique MCP tool identifier
--   - database:      Origin database name
--   - schema:        Origin schema name
--   - object_name:   Underlying table or view name
--   - cluster:       Materialize cluster hosting the index
--   - description:   Object-level comment used as the tool description
--   - input_schema: JSON Schema object defining required input fields
--   - output_columns: Array of non-key columns returned by the tool
--
WITH tools AS (
    SELECT
        op.database || '_' || op.schema || '_' || i.name AS name,
        op.database,
        op.schema,
        op.name AS object_name,
        c.name AS cluster,
        cts.comment AS description,
        jsonb_build_object(
            'type', 'object',
            'required', jsonb_agg(distinct ccol.name) FILTER (WHERE ccol.position = ic.on_position),
            'properties', jsonb_strip_nulls(jsonb_object_agg(
                ccol.name,
                CASE
                    WHEN ccol.type IN (
                        'uint2', 'uint4','uint8', 'int', 'integer', 'smallint',
                        'double', 'double precision', 'bigint', 'float',
                        'numeric', 'real'
                    ) THEN jsonb_build_object(
                        'type', 'number',
                        'description', cts_col.comment
                    )
                    WHEN ccol.type = 'boolean' THEN jsonb_build_object(
                        'type', 'boolean',
                        'description', cts_col.comment
                    )
                    WHEN ccol.type = 'bytea' THEN jsonb_build_object(
                        'type', 'string',
                        'description', cts_col.comment,
                        'contentEncoding', 'base64',
                        'contentMediaType', 'application/octet-stream'
                    )
                    WHEN ccol.type = 'date' THEN jsonb_build_object(
                        'type', 'string',
                        'format', 'date',
                        'description', cts_col.comment
                    )
                    WHEN ccol.type = 'time' THEN jsonb_build_object(
                        'type', 'string',
                        'format', 'time',
                        'description', cts_col.comment
                    )
                    WHEN ccol.type ilike 'timestamp%%' THEN jsonb_build_object(
                        'type', 'string',
                        'format', 'date-time',
                        'description', cts_col.comment
                    )
                    WHEN ccol.type = 'jsonb' THEN jsonb_build_object(
                        'type', 'object',
                        'description', cts_col.comment
                    )
                    WHEN ccol.type = 'uuid' THEN jsonb_build_object(
                        'type', 'string',
                        'format', 'uuid',
                        'description', cts_col.comment
                    )
                    ELSE jsonb_build_object(
                        'type', 'string',
                        'description', cts_col.comment
                    )
                END
            ) FILTER (WHERE ccol.position = ic.on_position))
        ) AS input_schema,
        array_agg(distinct ccol.name) FILTER (WHERE ccol.position <> ic.on_position) AS output_columns
    FROM mz_internal.mz_show_my_object_privileges op
    JOIN mz_objects o ON op.name = o.name AND op.object_type = o.type
    JOIN mz_schemas s ON s.name = op.schema AND s.id = o.schema_id
    JOIN mz_databases d ON d.name = op.database AND d.id = s.database_id
    JOIN mz_indexes i ON i.on_id = o.id
    JOIN mz_index_columns ic ON i.id = ic.index_id
    JOIN mz_columns ccol ON ccol.id = o.id
    JOIN mz_clusters c ON c.id = i.cluster_id
    JOIN mz_internal.mz_show_my_cluster_privileges cp ON cp.name = c.name
    JOIN mz_internal.mz_comments cts ON cts.id = o.id AND cts.object_sub_id IS NULL
    LEFT JOIN mz_internal.mz_comments cts_col ON cts_col.id = o.id AND cts_col.object_sub_id = ccol.position
    WHERE op.privilege_type = 'SELECT'
      AND cp.privilege_type = 'USAGE'
    GROUP BY 1,2,3,4,5,6
)
SELECT * FROM tools
