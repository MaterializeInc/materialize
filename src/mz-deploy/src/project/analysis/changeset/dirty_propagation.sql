-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Dirty propagation for `mz-deploy stage`.
--
-- This query compares a compiled project with the deployment in production.
-- The query gives one jsonb object. The object tells a deploy which objects
-- to deploy again, which resources to make, and where to send each object.
--
-- Each binding below has a comment with the Datalog rule for that binding.
-- The rules are the specification. This header gives the data that you must
-- know before you read the rules.
--
-- Two names in the rules have no binding. Each of these names is a source
-- table without one column. ObjectInSchema is the table project_object
-- without the column `kind`. StmtUsesCluster is the table
-- project_stmt_cluster.
--
-- ## Object keys
--
-- Each relation has three text columns: `db`, `sch` and `obj`. The three
-- columns together are the key of an object. The three values are the raw
-- catalog names. The deployment history keeps the same names.
--
-- NOTE: do not replace the three columns with one column that holds the text
-- `db.sch.obj`. A name in one column must have quotation marks around a
-- component that is a keyword. It must also have quotation marks around a
-- component that contains a special character. The names in the deployment
-- history have no quotation marks. Therefore the two forms of a name do not
-- agree. This occurs, for example, for each object in a schema with the name
-- `select`. Each of these objects then looks deleted and added again at each
-- deploy.
--
-- An object that is not in a database has the empty text `''` in `db`. A
-- relation in the system catalog is such an object. This object can occur
-- only as the parent in the table project_depends_on. The empty text keeps
-- each join simple, because a join on NULL obeys different rules.
--
-- ## Input
--
-- The query reads temporary tables in the schema `mz_temp`. The Rust module
-- `facts.rs` makes these tables and fills them before this query starts. All
-- columns have the type text.
--
-- Materialize looks in `mz_temp` before it looks at the search path.
-- Therefore each binding uses the short name of the table.
--
-- This section gives the meaning of each column. The directory
-- `temp_tables/` holds one CREATE statement for each table.
--
--   project_object(db, sch, obj, kind)
--       Each object in the compiled project. The column `kind` has one of
--       these values: 'view', 'materialized_view', 'sink', 'table',
--       'table_from_source', 'source', 'secret', 'connection'.
--
--   project_depends_on(child_db, child_sch, child_obj,
--                      parent_db, parent_sch, parent_obj)
--       The query of the child refers to the parent. A parent that is not in
--       the project has no row in project_object. Such a parent has the empty
--       text in `parent_db`.
--
--   project_stmt_cluster(db, sch, obj, cluster)
--       The clause `IN CLUSTER` of the CREATE statement of the object.
--
--   project_index_cluster(db, sch, obj, idx, cluster)
--       The clause `IN CLUSTER` of a CREATE INDEX statement on the object.
--
--   replacement_schema(db, sch)
--       The schemas that use the replacement protocol for their materialized
--       views.
--
--   new_object(db, sch, obj, hash)
--       One content hash for each object in the project that you deploy. Two
--       objects have the same hash only if a deploy of the two objects gives
--       the same result. This table does not contain the kinds that `apply`
--       controls.
--
--   old_object(db, sch, obj, hash)
--       The same hashes for the deployment in production.
--
--   old_schema_kind(db, sch, kind)
--       The method that deployed each schema in production. The column `kind`
--       has one of these values: 'objects', 'replacement', 'sinks', 'tables'.
--       The rules use only the value 'replacement'.
--
--   forced_schema(db, sch)
--       The schemas that the caller deploys again at all times. The option
--       `--redeploy-schema` fills this table.
--
-- The tables `old_object` and `old_schema_kind` come from the deployment
-- history. The history keeps the three name components in three fields. The
-- load does not change these names. Therefore no side of the comparison
-- changes a name.
--
-- If the table `old_object` is empty, each object looks new. Therefore the
-- first deploy obeys the same rules as all other deploys.
--
-- You cannot read and write in one transaction in Materialize. Therefore the
-- COPY statements and this SELECT statement run in different transactions.
-- The transactions use the same session. The tables stay until the session
-- stops.
--
-- ## Output
--
-- The query gives one row with one jsonb column. The query gives this row
-- also when all of the sets are empty. The query sorts each array. Therefore
-- the output bytes stay the same for the same input.
--
-- An object in the output has the fields "database", "schema" and "object". A
-- schema in the output has the fields "database" and "schema". Therefore no
-- caller must divide a name into its components. The Rust type `ChangeSet` in
-- `types.rs` gives the name and the function of each field.
--
-- The fields `dirty_schemas` and `dirty_clusters` only show data to the user.
-- The rules do not read these two fields. A deploy reads the stage fields and
-- the two `_to_create` fields. The `_to_create` fields contain each object
-- that the deploy makes. Therefore they contain more objects than the dirty
-- fields.
--
-- The field `objects_to_deploy` also contains the objects that only the old
-- deployment has. These objects have no statement in the project. Therefore
-- the query does not put them in a stage field.
WITH MUTUALLY RECURSIVE
    -- IsSink(O) :- ProjectObject(Db, Sch, Obj, 'sink').
    is_sink (db text, sch text, obj text) AS (
        SELECT db, sch, obj FROM project_object WHERE kind = 'sink'
    ),

    -- IsApplyManaged(O) :- ProjectObject(Db, Sch, Obj, K), K IN (...).
    is_apply_managed (db text, sch text, obj text) AS (
        SELECT db, sch, obj
        FROM project_object
        WHERE kind IN ('table', 'table_from_source', 'source', 'secret', 'connection')
    ),

    -- IsReplacement(O) :- ProjectObject(Db, Sch, Obj, _), ReplacementSchema(Db, Sch).
    is_replacement (db text, sch text, obj text) AS (
        SELECT p.db, p.sch, p.obj
        FROM project_object p
        JOIN replacement_schema r ON r.db = p.db AND r.sch = p.sch
    ),

    -- UsesCluster(O, C) :- ProjectStmtCluster(Db, Sch, Obj, C).
    -- UsesCluster(O, C) :- ProjectIndexCluster(Db, Sch, Obj, _, C).
    uses_cluster (db text, sch text, obj text, cluster text) AS (
        SELECT db, sch, obj, cluster FROM project_stmt_cluster

        UNION

        SELECT db, sch, obj, cluster FROM project_index_cluster
    ),

    -- ChangedStmt(O) :- NewObject(O, H1), OldObject(O, H2), H1 != H2.
    -- ChangedStmt(O) :- NewObject(O, _), NOT OldObject(O, _).
    -- ChangedStmt(O) :- OldObject(O, _), NOT NewObject(O, _).
    changed_stmt (db text, sch text, obj text) AS (
        SELECT
            coalesce(n.db, o.db),
            coalesce(n.sch, o.sch),
            coalesce(n.obj, o.obj)
        FROM new_object n
        FULL OUTER JOIN old_object o
          ON o.db = n.db AND o.sch = n.sch AND o.obj = n.obj
        WHERE n.hash IS DISTINCT FROM o.hash
    ),

    -- DirtyStmt(O)           :- ChangedStmt(O).
    -- DirtyStmt(Db, Sch, O)  :- ForcedSchema(Db, Sch), ObjectInSchema(Db, Sch, O).
    -- DirtyStmt(O2)          :- ChangedStmt(O1), NOT IsSink(O1), UsesCluster(O1, C), StmtUsesCluster(O2, C).
    -- DirtyStmt(O)           :- DependsOn(O, P), DirtyStmt(P), NOT IsReplacement(P).
    -- DirtyStmt(Db, Sch, O2) :- DirtyStmt(Db, Sch, O1), NOT IsSink(O1), ObjectInSchema(Db, Sch, O2).
    --
    -- NOTE: the cluster rule reads ChangedStmt, not DirtyStmt. If this rule
    -- reads DirtyStmt, an object that a different rule made dirty sends its
    -- cluster again. One cluster in more than one schema then makes all of
    -- the project dirty at each deploy.
    --
    -- The two sides of the cluster rule read different relations. A change to
    -- an index makes the cluster of that index dirty. But only the cluster of
    -- the statement of an object makes that object dirty. You can make an
    -- index again without a new deploy of the statement below it.
    dirty_stmt (db text, sch text, obj text) AS (
        SELECT db, sch, obj FROM changed_stmt

        UNION

        SELECT p.db, p.sch, p.obj
        FROM project_object p
        JOIN forced_schema f ON f.db = p.db AND f.sch = p.sch

        UNION

        SELECT t.db, t.sch, t.obj
        FROM changed_stmt s
        JOIN uses_cluster u ON u.db = s.db AND u.sch = s.sch AND u.obj = s.obj
        JOIN project_stmt_cluster t ON t.cluster = u.cluster
        WHERE NOT EXISTS (
            SELECT 1 FROM is_sink k
            WHERE k.db = s.db AND k.sch = s.sch AND k.obj = s.obj
        )

        UNION

        SELECT dep.child_db, dep.child_sch, dep.child_obj
        FROM project_depends_on dep
        JOIN dirty_stmt p
          ON p.db = dep.parent_db AND p.sch = dep.parent_sch AND p.obj = dep.parent_obj
        WHERE NOT EXISTS (
            SELECT 1 FROM is_replacement r
            WHERE r.db = dep.parent_db
              AND r.sch = dep.parent_sch
              AND r.obj = dep.parent_obj
        )

        UNION

        SELECT p.db, p.sch, p.obj
        FROM dirty_stmt s
        JOIN project_object p ON p.db = s.db AND p.sch = s.sch
        WHERE NOT EXISTS (
            SELECT 1 FROM is_sink k
            WHERE k.db = s.db AND k.sch = s.sch AND k.obj = s.obj
        )
    ),

    -- DirtyCluster(C) :- ChangedStmt(O), UsesCluster(O, C), NOT IsSink(O).
    dirty_cluster (cluster text) AS (
        SELECT u.cluster
        FROM changed_stmt s
        JOIN uses_cluster u ON u.db = s.db AND u.sch = s.sch AND u.obj = s.obj
        WHERE NOT EXISTS (
            SELECT 1 FROM is_sink k
            WHERE k.db = s.db AND k.sch = s.sch AND k.obj = s.obj
        )
    ),

    -- DirtySchema(Db, Sch) :- DirtyStmt(Db, Sch, O), NOT IsSink(O).
    dirty_schema (db text, sch text) AS (
        SELECT DISTINCT s.db, s.sch
        FROM dirty_stmt s
        WHERE NOT EXISTS (
            SELECT 1 FROM is_sink k
            WHERE k.db = s.db AND k.sch = s.sch AND k.obj = s.obj
        )
    ),

    -- DeployStmt(O) :- DirtyStmt(O), ProjectObject(Db, Sch, Obj, K),
    --                  K IN ('view', 'materialized_view').
    deploy_stmt (db text, sch text, obj text) AS (
        SELECT s.db, s.sch, s.obj
        FROM dirty_stmt s
        JOIN project_object p ON p.db = s.db AND p.sch = s.sch AND p.obj = s.obj
        WHERE p.kind IN ('view', 'materialized_view')
    ),

    -- DeploySchema(Db, Sch) :- DeployStmt(Db, Sch, Obj).
    deploy_schema (db text, sch text) AS (
        SELECT DISTINCT db, sch FROM deploy_stmt
    ),

    -- DeployCluster(C) :- DeployStmt(O), UsesCluster(O, C).
    deploy_cluster (cluster text) AS (
        SELECT u.cluster
        FROM deploy_stmt d
        JOIN uses_cluster u ON u.db = d.db AND u.sch = d.sch AND u.obj = d.obj
    ),

    -- ReplacementChanged(O) :- DirtyStmt(Db, Sch, Obj), IsReplacement(O),
    --                          OldObject(O, _), OldSchemaKind(Db, Sch, 'replacement').
    replacement_changed (db text, sch text, obj text) AS (
        SELECT s.db, s.sch, s.obj
        FROM dirty_stmt s
        JOIN is_replacement r ON r.db = s.db AND r.sch = s.sch AND r.obj = s.obj
        JOIN old_object o ON o.db = s.db AND o.sch = s.sch AND o.obj = s.obj
        JOIN old_schema_kind k
          ON k.db = s.db AND k.sch = s.sch AND k.kind = 'replacement'
    ),

    -- ReplacementNew(O) :- DirtyStmt(O), IsReplacement(O),
    --                      NOT ReplacementChanged(O).
    replacement_new (db text, sch text, obj text) AS (
        SELECT s.db, s.sch, s.obj
        FROM dirty_stmt s
        JOIN is_replacement r ON r.db = s.db AND r.sch = s.sch AND r.obj = s.obj
        WHERE NOT EXISTS (
            SELECT 1 FROM replacement_changed c
            WHERE c.db = s.db AND c.sch = s.sch AND c.obj = s.obj
        )
    ),

    -- StageReplacementMv(O) :- DeployStmt(Db, Sch, Obj),
    --                          ProjectObject(Db, Sch, Obj, 'materialized_view'),
    --                          ReplacementChanged(O).
    stage_replacement_mv (db text, sch text, obj text) AS (
        SELECT d.db, d.sch, d.obj
        FROM deploy_stmt d
        JOIN project_object p
          ON p.db = d.db AND p.sch = d.sch AND p.obj = d.obj
         AND p.kind = 'materialized_view'
        JOIN replacement_changed c
          ON c.db = d.db AND c.sch = d.sch AND c.obj = d.obj
    ),

    -- StageObject(O) :- DeployStmt(O), NOT StageReplacementMv(O).
    stage_object (db text, sch text, obj text) AS (
        SELECT d.db, d.sch, d.obj
        FROM deploy_stmt d
        WHERE NOT EXISTS (
            SELECT 1 FROM stage_replacement_mv m
            WHERE m.db = d.db AND m.sch = d.sch AND m.obj = d.obj
        )
    ),

    -- StageSink(O) :- DirtyStmt(O), IsSink(O).
    stage_sink (db text, sch text, obj text) AS (
        SELECT s.db, s.sch, s.obj
        FROM dirty_stmt s
        JOIN is_sink k ON k.db = s.db AND k.sch = s.sch AND k.obj = s.obj
    ),

    -- StageApplyManaged(O) :- DirtyStmt(O), IsApplyManaged(O).
    stage_apply_managed (db text, sch text, obj text) AS (
        SELECT s.db, s.sch, s.obj
        FROM dirty_stmt s
        JOIN is_apply_managed a ON a.db = s.db AND a.sch = s.sch AND a.obj = s.obj
    )

SELECT jsonb_build_object(
    'changed_objects', (
        SELECT coalesce(
            jsonb_agg(
                jsonb_build_object('database', db, 'schema', sch, 'object', obj)
                ORDER BY db, sch, obj
            ),
            '[]'::jsonb
        )
        FROM changed_stmt
    ),
    'objects_to_deploy', (
        SELECT coalesce(
            jsonb_agg(
                jsonb_build_object('database', db, 'schema', sch, 'object', obj)
                ORDER BY db, sch, obj
            ),
            '[]'::jsonb
        )
        FROM dirty_stmt
    ),
    'dirty_schemas', (
        SELECT coalesce(
            jsonb_agg(
                jsonb_build_object('database', db, 'schema', sch)
                ORDER BY db, sch
            ),
            '[]'::jsonb
        )
        FROM dirty_schema
    ),
    'dirty_clusters', (
        SELECT coalesce(jsonb_agg(cluster ORDER BY cluster), '[]'::jsonb)
        FROM dirty_cluster
    ),
    'schemas_to_create', (
        SELECT coalesce(
            jsonb_agg(
                jsonb_build_object('database', db, 'schema', sch)
                ORDER BY db, sch
            ),
            '[]'::jsonb
        )
        FROM deploy_schema
    ),
    'clusters_to_create', (
        SELECT coalesce(jsonb_agg(cluster ORDER BY cluster), '[]'::jsonb)
        FROM deploy_cluster
    ),
    'stage_objects', (
        SELECT coalesce(
            jsonb_agg(
                jsonb_build_object('database', db, 'schema', sch, 'object', obj)
                ORDER BY db, sch, obj
            ),
            '[]'::jsonb
        )
        FROM stage_object
    ),
    'stage_replacement_mvs', (
        SELECT coalesce(
            jsonb_agg(
                jsonb_build_object('database', db, 'schema', sch, 'object', obj)
                ORDER BY db, sch, obj
            ),
            '[]'::jsonb
        )
        FROM stage_replacement_mv
    ),
    'stage_sinks', (
        SELECT coalesce(
            jsonb_agg(
                jsonb_build_object('database', db, 'schema', sch, 'object', obj)
                ORDER BY db, sch, obj
            ),
            '[]'::jsonb
        )
        FROM stage_sink
    ),
    'replacement_objects_new', (
        SELECT coalesce(
            jsonb_agg(
                jsonb_build_object('database', db, 'schema', sch, 'object', obj)
                ORDER BY db, sch, obj
            ),
            '[]'::jsonb
        )
        FROM replacement_new
    ),
    'apply_managed_count', (
        SELECT count(*) FROM stage_apply_managed
    )
);
