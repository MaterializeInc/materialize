-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Materialize Catalog Ontology: Semantic Types

INSERT INTO misc_mz_ontology.semantic_types (name, sql_type, description) VALUES

('CatalogItemId', 'text', 'SQL-layer identifier for a catalog object (table, view, MV, source, sink, index, connection, secret, type, function). Format: s{n}/u{n}. This is the ID users see in mz_objects.id, mz_tables.id, etc. Rust: CatalogItemId in src/repr/src/catalog_item_id.rs'),
('GlobalId', 'text', 'Runtime identifier used by compute and storage layers. A single CatalogItemId can map to multiple GlobalIds over time (e.g., after ALTER). Format: s{n}/u{n}/si{n}. Appears in mz_internal introspection tables. Mapped via mz_object_global_ids. Rust: GlobalId in src/repr/src/global_id.rs'),
('ClusterId', 'text', 'Identifies a compute cluster. Format: s{n}/u{n}. Rust: ClusterId (alias for StorageInstanceId) in src/controller-types/src/lib.rs'),
('ReplicaId', 'text', 'Identifies a cluster replica. Format: s{n}/u{n}. Rust: ReplicaId in src/cluster-client/src/lib.rs'),
('SchemaId', 'text', 'Identifies a schema. Format: s{n}/u{n}. Rust: SchemaId in src/sql/src/names.rs'),
('DatabaseId', 'text', 'Identifies a database. Format: s{n}/u{n}. Rust: DatabaseId in src/sql/src/names.rs'),
('RoleId', 'text', 'Identifies a role or user. Format: s{n} (system), g{n} (predefined), u{n} (user), p (public). Rust: RoleId in src/repr/src/role_id.rs'),
('NetworkPolicyId', 'text', 'Identifies a network policy. Format: s{n}/u{n}. Rust: NetworkPolicyId in src/repr/src/network_policy_id.rs'),
('ShardId', 'text', 'Identifies a persist shard (durable storage unit). Format: s{uuid}. Rust: ShardId in src/persist-types/src/lib.rs'),
('OID', 'oid', 'Postgres-compatible object identifier. Numeric. Used for compatibility with PostgreSQL tooling and drivers. Not the primary ID in Materialize.'),
('ObjectType', 'text', 'The type of a catalog object: table, source, view, materialized-view, sink, index, connection, secret, type, function. Used in mz_objects.type, mz_audit_events.object_type'),
('ConnectionType', 'text', 'The type of a connection: kafka, postgres, aws, ssh-tunnel, etc. Used in mz_connections.type'),
('SourceType', 'text', 'The type of a source: kafka, postgres, load-generator, etc. Used in mz_sources.type'),
('MzTimestamp', 'mz_timestamp', 'Internal logical timestamp used by Materialize for consistency. A uint64. Different from wall clock timestamps. Used in frontier tracking.'),
('WallclockTimestamp', 'timestamp with time zone', 'A wall clock timestamp. Used for audit events, storage usage timestamps, etc.'),
('ByteCount', 'uint8', 'A count of bytes. Used for storage sizes, memory usage, arrangement sizes.'),
('RecordCount', 'uint8', 'A count of records/rows. Used for statistics and arrangement info.'),
('CreditRate', 'numeric', 'Credits consumed per hour. Used in mz_cluster_replica_sizes.credits_per_hour.'),
('SqlDefinition', 'text', 'A SQL CREATE statement. Used in create_sql columns across object types.'),
('RedactedSqlDefinition', 'text', 'A SQL CREATE statement with sensitive values redacted. Used in redacted_create_sql columns.');
