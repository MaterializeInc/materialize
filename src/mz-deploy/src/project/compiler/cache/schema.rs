// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Schema for the compiler cache database.
//!
//! All cached state is advisory. On schema-version mismatch every table is
//! dropped and recreated from [`CREATE_SQL`] — safe because nothing here is
//! authoritative.

pub(super) const SCHEMA_VERSION: i64 = 11;

pub(super) const DROP_SQL: &str = "
    DROP TABLE IF EXISTS meta;
    DROP TABLE IF EXISTS file_state;
    DROP TABLE IF EXISTS object_state;
    DROP TABLE IF EXISTS object_state_indexes;
    DROP TABLE IF EXISTS object_state_grants;
    DROP TABLE IF EXISTS object_state_comments;
    DROP TABLE IF EXISTS object_state_tests;
    DROP TABLE IF EXISTS typecheck_state;
    DROP TABLE IF EXISTS typecheck_columns;
    DROP TABLE IF EXISTS typecheck_objects;
    DROP TABLE IF EXISTS external_type_digest;
    DROP TABLE IF EXISTS project_aliases;
    DROP TABLE IF EXISTS project_databases;
    DROP TABLE IF EXISTS project_schemas;
    DROP TABLE IF EXISTS project_objects;
    DROP TABLE IF EXISTS project_dependencies;
    DROP TABLE IF EXISTS project_external_dependencies;
    DROP TABLE IF EXISTS project_cluster_dependencies;
    DROP TABLE IF EXISTS project_replacement_schemas;
    DROP TABLE IF EXISTS project_comments;
    DROP TABLE IF EXISTS project_indexes;
    DROP TABLE IF EXISTS project_grants;
    DROP TABLE IF EXISTS project_tests;
    DROP TABLE IF EXISTS project_infrastructure;
    DROP TABLE IF EXISTS project_infrastructure_properties;
    DROP TABLE IF EXISTS project_mod_statements;
";

pub(super) const CREATE_SQL: &str = "
    CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS file_state (
        path TEXT PRIMARY KEY,
        size INTEGER NOT NULL,
        mtime_ns INTEGER NOT NULL,
        content_hash TEXT NOT NULL,
        contents TEXT
    );
    CREATE TABLE IF NOT EXISTS object_state (
        object_key TEXT PRIMARY KEY,
        fingerprint TEXT NOT NULL,
        -- 'object' or 'skipped'. When 'skipped', every column below is NULL.
        kind TEXT NOT NULL,
        db_name TEXT,
        schema_name TEXT,
        file_path TEXT,
        stmt_sql TEXT
    );
    CREATE TABLE IF NOT EXISTS object_state_indexes (
        object_key TEXT NOT NULL,
        position INTEGER NOT NULL,
        sql_text TEXT NOT NULL,
        PRIMARY KEY (object_key, position)
    );
    CREATE TABLE IF NOT EXISTS object_state_grants (
        object_key TEXT NOT NULL,
        position INTEGER NOT NULL,
        sql_text TEXT NOT NULL,
        PRIMARY KEY (object_key, position)
    );
    CREATE TABLE IF NOT EXISTS object_state_comments (
        object_key TEXT NOT NULL,
        position INTEGER NOT NULL,
        sql_text TEXT NOT NULL,
        PRIMARY KEY (object_key, position)
    );
    CREATE TABLE IF NOT EXISTS object_state_tests (
        object_key TEXT NOT NULL,
        position INTEGER NOT NULL,
        sql_text TEXT NOT NULL,
        PRIMARY KEY (object_key, position)
    );
    CREATE TABLE IF NOT EXISTS typecheck_objects (
        object_key TEXT PRIMARY KEY,
        object_kind TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS typecheck_columns (
        object_key TEXT NOT NULL,
        column_name TEXT NOT NULL,
        column_type TEXT NOT NULL,
        nullable INTEGER NOT NULL,
        position INTEGER NOT NULL,
        PRIMARY KEY (object_key, column_name),
        FOREIGN KEY (object_key) REFERENCES typecheck_objects(object_key)
    );
    CREATE TABLE IF NOT EXISTS external_type_digest (
        object_key TEXT PRIMARY KEY,
        digest TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS project_databases (
        name TEXT PRIMARY KEY
    );
    CREATE TABLE IF NOT EXISTS project_schemas (
        database TEXT NOT NULL,
        name TEXT NOT NULL,
        schema_type TEXT NOT NULL,
        PRIMARY KEY (database, name)
    );
    CREATE TABLE IF NOT EXISTS project_objects (
        object_key TEXT PRIMARY KEY,
        database TEXT NOT NULL,
        schema TEXT NOT NULL,
        name TEXT NOT NULL,
        object_kind TEXT NOT NULL,
        cluster TEXT,
        file_path TEXT NOT NULL,
        sql_text TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_project_objects_file_path
        ON project_objects(file_path);
    CREATE INDEX IF NOT EXISTS idx_project_objects_db_schema
        ON project_objects(database, schema);
    CREATE TABLE IF NOT EXISTS project_dependencies (
        object_key TEXT NOT NULL,
        dependency_key TEXT NOT NULL,
        PRIMARY KEY (object_key, dependency_key)
    );
    CREATE INDEX IF NOT EXISTS idx_project_dependencies_dependency_key
        ON project_dependencies(dependency_key);
    CREATE TABLE IF NOT EXISTS project_external_dependencies (
        object_key TEXT NOT NULL PRIMARY KEY
    );
    CREATE TABLE IF NOT EXISTS project_cluster_dependencies (
        cluster_name TEXT NOT NULL PRIMARY KEY
    );
    CREATE TABLE IF NOT EXISTS project_replacement_schemas (
        database TEXT NOT NULL,
        schema TEXT NOT NULL,
        PRIMARY KEY (database, schema)
    );
    CREATE TABLE IF NOT EXISTS project_comments (
        object_key TEXT NOT NULL,
        comment_type TEXT NOT NULL,
        target_column TEXT,
        comment_text TEXT NOT NULL,
        sql_text TEXT NOT NULL,
        PRIMARY KEY (object_key, comment_type, target_column)
    );
    CREATE TABLE IF NOT EXISTS project_indexes (
        object_key TEXT NOT NULL,
        index_name TEXT,
        cluster TEXT,
        columns TEXT NOT NULL,
        sql_text TEXT NOT NULL,
        PRIMARY KEY (object_key, index_name)
    );
    CREATE TABLE IF NOT EXISTS project_grants (
        object_key TEXT NOT NULL,
        privilege TEXT NOT NULL,
        grantee TEXT NOT NULL,
        sql_text TEXT NOT NULL,
        PRIMARY KEY (object_key, privilege, grantee)
    );
    CREATE TABLE IF NOT EXISTS project_tests (
        object_key TEXT NOT NULL,
        test_name TEXT NOT NULL,
        sql_text TEXT NOT NULL,
        PRIMARY KEY (object_key, test_name)
    );
    CREATE TABLE IF NOT EXISTS project_infrastructure (
        object_key TEXT NOT NULL PRIMARY KEY,
        infra_type TEXT NOT NULL,
        connector_type TEXT,
        connection_ref TEXT,
        source_ref TEXT,
        external_reference TEXT
    );
    CREATE TABLE IF NOT EXISTS project_infrastructure_properties (
        object_key TEXT NOT NULL,
        property_key TEXT NOT NULL,
        property_value TEXT NOT NULL,
        secret_ref TEXT,
        object_ref TEXT,
        PRIMARY KEY (object_key, property_key)
    );
    CREATE TABLE IF NOT EXISTS project_aliases (
        object_key TEXT NOT NULL,
        alias TEXT NOT NULL,
        target_fqn TEXT NOT NULL,
        PRIMARY KEY (object_key, alias)
    );
    CREATE TABLE IF NOT EXISTS project_mod_statements (
        database TEXT NOT NULL,
        schema TEXT,
        position INTEGER NOT NULL,
        sql_text TEXT NOT NULL,
        PRIMARY KEY (database, schema, position)
    );
";
