// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * Object types that can have privileges granted in Materialize
 * Based on: https://materialize.com/docs/sql/grant-privilege/
 */
export const OBJECT_TYPES = [
  { id: "cluster", name: "Cluster" },
  { id: "connection", name: "Connection" },
  { id: "database", name: "Database" },
  { id: "materialized-view", name: "Materialized View" },
  { id: "schema", name: "Schema" },
  { id: "secret", name: "Secret" },
  { id: "source", name: "Source" },
  { id: "system", name: "System" },
  { id: "table", name: "Table" },
  { id: "type", name: "Type" },
  { id: "view", name: "View" },
] as const;

export type ObjectTypeId = (typeof OBJECT_TYPES)[number]["id"];
export type ObjectType = (typeof OBJECT_TYPES)[number];

/**
 * Privileges available for each object type
 * Based on: https://materialize.com/docs/sql/grant-privilege/
 */
export const PRIVILEGES_BY_OBJECT_TYPE: Record<ObjectTypeId, string[]> = {
  cluster: ["CREATE", "USAGE"],
  connection: ["USAGE"],
  database: ["CREATE", "USAGE"],
  "materialized-view": ["SELECT"],
  schema: ["CREATE", "USAGE"],
  secret: ["USAGE"],
  source: ["SELECT"],
  system: ["CREATECLUSTER", "CREATEDB", "CREATENETWORKPOLICY", "CREATEROLE"],
  table: ["DELETE", "INSERT", "SELECT", "UPDATE"],
  type: ["USAGE"],
  view: ["SELECT"],
};

/**
 * A privilege grant for a specific object
 */
export type PrivilegeGrant = {
  objectType: ObjectTypeId;
  objectId: string;
  objectName: string;
  databaseName?: string;
  schemaName?: string;
  privileges: string[];
};

/**
 * Map of object type ID to uppercase display name
 */
export const OBJECT_TYPE_SQL_NAMES: Record<ObjectTypeId, string> =
  Object.fromEntries(
    OBJECT_TYPES.map((t) => [t.id, t.name.toUpperCase()]),
  ) as Record<ObjectTypeId, string>;

/**
 * Map of object type ID to SQL keyword for GRANT/REVOKE statements.
 * Note: Views and materialized views use TABLE keyword in GRANT syntax.
 */
export const GRANT_SQL_OBJECT_TYPES: Record<ObjectTypeId, string> = {
  cluster: "CLUSTER",
  connection: "CONNECTION",
  database: "DATABASE",
  "materialized-view": "TABLE",
  schema: "SCHEMA",
  secret: "SECRET",
  source: "SOURCE",
  system: "SYSTEM",
  table: "TABLE",
  type: "TYPE",
  view: "TABLE",
};
