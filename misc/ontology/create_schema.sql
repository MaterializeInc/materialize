-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Create the ontology schema and tables
-- Run first, then load data files in any order
-- Usage: psql -h localhost -p 6877 -U mz_system materialize -f create_schema.sql

CREATE SCHEMA IF NOT EXISTS misc_mz_ontology;

CREATE TABLE IF NOT EXISTS misc_mz_ontology.entity_types (
    name        text NOT NULL,
    relation    text NOT NULL,
    properties  jsonb,
    description text
);

CREATE TABLE IF NOT EXISTS misc_mz_ontology.semantic_types (
    name        text NOT NULL,
    sql_type    text NOT NULL,
    description text
);

CREATE TABLE IF NOT EXISTS misc_mz_ontology.properties (
    entity_type     text NOT NULL,
    column_name     text NOT NULL,
    semantic_type   text,
    description     text
);

CREATE TABLE IF NOT EXISTS misc_mz_ontology.link_types (
    name            text NOT NULL,
    source_entity   text NOT NULL,
    target_entity   text NOT NULL,
    properties      jsonb,
    description     text
);

-- Grant read access so MCP endpoints (which run as the authenticated user) can query ontology tables
GRANT USAGE ON SCHEMA misc_mz_ontology TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA misc_mz_ontology TO PUBLIC;
