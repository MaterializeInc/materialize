// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  generateCreateRoleSql,
  generateTerraformCode,
} from "./generateRoleCodePreview";
import { CreateRoleFormState } from "./types";

// Helper to create minimal state
const createState = (
  overrides: Partial<CreateRoleFormState> = {},
): CreateRoleFormState => ({
  name: "",
  inheritedRoles: [],
  privileges: [],
  ...overrides,
});

describe("generateCreateRoleSql", () => {
  it("returns placeholder when name is empty or whitespace", () => {
    expect(generateCreateRoleSql(createState({ name: "" }))).toBe(
      "-- Enter a role name to see the SQL",
    );
    expect(generateCreateRoleSql(createState({ name: "   " }))).toBe(
      "-- Enter a role name to see the SQL",
    );
  });

  it("generates basic CREATE ROLE statement", () => {
    const result = generateCreateRoleSql(createState({ name: "analyst" }));

    expect(result).toBe('CREATE ROLE "analyst";');
  });

  it("generates inherited role grants", () => {
    const result = generateCreateRoleSql(
      createState({
        name: "analyst",
        inheritedRoles: [
          { id: "u1", name: "readonly" },
          { id: "u2", name: "writer" },
        ],
      }),
    );

    expect(result).toContain('CREATE ROLE "analyst"');
    expect(result).toContain('GRANT "readonly" TO "analyst"');
    expect(result).toContain('GRANT "writer" TO "analyst"');
  });

  it("generates privilege grants on objects", () => {
    const result = generateCreateRoleSql(
      createState({
        name: "reader",
        privileges: [
          {
            objectType: "cluster",
            objectId: "c1",
            objectName: "prod",
            privileges: ["USAGE", "CREATE"],
          },
        ],
      }),
    );

    expect(result).toContain('GRANT USAGE, CREATE ON CLUSTER "prod"');
  });

  it("generates system privilege grants", () => {
    const result = generateCreateRoleSql(
      createState({
        name: "admin",
        privileges: [
          {
            objectType: "system",
            objectId: "system",
            objectName: "system",
            privileges: ["CREATEROLE"],
          },
        ],
      }),
    );

    expect(result).toContain("GRANT CREATEROLE ON SYSTEM");
  });

  it("quotes identifiers correctly (special chars and qualified names)", () => {
    const result = generateCreateRoleSql(
      createState({
        name: "my-role",
        privileges: [
          {
            objectType: "table",
            objectId: "t1",
            objectName: "table",
            databaseName: "db",
            schemaName: "schema",
            privileges: ["SELECT"],
          },
          {
            objectType: "database",
            objectId: "d1",
            objectName: "my-db",
            privileges: ["USAGE"],
          },
        ],
      }),
    );

    expect(result).toContain('"my-role"');
    expect(result).toContain('"db"."schema"."table"');
    expect(result).toContain('"my-db"');
  });

  it("skips privileges with empty array", () => {
    const result = generateCreateRoleSql(
      createState({
        name: "test",
        privileges: [
          {
            objectType: "cluster",
            objectId: "c1",
            objectName: "prod",
            privileges: [],
          },
          {
            objectType: "database",
            objectId: "d1",
            objectName: "analytics",
            privileges: ["USAGE"],
          },
        ],
      }),
    );

    expect(result).not.toContain("CLUSTER");
    expect(result).toContain('GRANT USAGE ON DATABASE "analytics"');
  });

  it("handles object names containing periods", () => {
    const result = generateCreateRoleSql(
      createState({
        name: "test",
        privileges: [
          {
            objectType: "table",
            objectId: "t1",
            objectName: "wee..woo",
            databaseName: "db",
            schemaName: "schema",
            privileges: ["SELECT"],
          },
        ],
      }),
    );

    expect(result).toContain('"db"."schema"."wee..woo"');
  });

  it("orders statements correctly: CREATE ROLE, then GRANT roles, then GRANT privileges", () => {
    const result = generateCreateRoleSql(
      createState({
        name: "test",
        inheritedRoles: [{ id: "u1", name: "parent" }],
        privileges: [
          {
            objectType: "cluster",
            objectId: "c1",
            objectName: "prod",
            privileges: ["USAGE"],
          },
        ],
      }),
    );

    const statements = result.split("\n\n");
    expect(statements[0]).toMatch(/^CREATE ROLE/);
    expect(statements[1]).toMatch(/^GRANT "parent" TO/);
    expect(statements[2]).toMatch(/^GRANT USAGE ON CLUSTER/);
  });

  it("handles different object types", () => {
    const result = generateCreateRoleSql(
      createState({
        name: "test",
        privileges: [
          {
            objectType: "schema",
            objectId: "s1",
            objectName: "public",
            privileges: ["USAGE"],
          },
          {
            objectType: "materialized-view",
            objectId: "mv1",
            objectName: "view",
            databaseName: "db",
            schemaName: "schema",
            privileges: ["SELECT"],
          },
          {
            objectType: "connection",
            objectId: "conn1",
            objectName: "kafka",
            privileges: ["USAGE"],
          },
        ],
      }),
    );

    expect(result).toContain('ON SCHEMA "public"');
    // Views and materialized views use TABLE keyword in GRANT syntax
    expect(result).toContain("ON TABLE");
    expect(result).toContain('ON CONNECTION "kafka"');
  });
});

describe("generateTerraformCode", () => {
  it("returns placeholder when name is empty or whitespace", () => {
    expect(generateTerraformCode(createState({ name: "" }))).toBe(
      "# Enter a role name to see the Terraform configuration",
    );
    expect(generateTerraformCode(createState({ name: "   " }))).toBe(
      "# Enter a role name to see the Terraform configuration",
    );
  });

  it("generates basic role resource", () => {
    const result = generateTerraformCode(createState({ name: "analyst" }));

    expect(result).toContain('resource "materialize_role" "analyst"');
    expect(result).toMatch(/name\s*=\s*"analyst"/);
  });

  it("generates role grant resources for inherited roles", () => {
    const result = generateTerraformCode(
      createState({
        name: "analyst",
        inheritedRoles: [
          { id: "u1", name: "readonly" },
          { id: "u2", name: "viewer" },
        ],
      }),
    );

    expect(result).toContain("materialize_grant_role");
    expect(result).toMatch(/role_name\s*=\s*"readonly"/);
    expect(result).toMatch(/member_name\s*=\s*materialize_role\.analyst\.name/);
  });

  it("generates privilege grant resources", () => {
    const result = generateTerraformCode(
      createState({
        name: "user",
        privileges: [
          {
            objectType: "cluster",
            objectId: "c1",
            objectName: "prod",
            privileges: ["USAGE"],
          },
        ],
      }),
    );

    expect(result).toContain("materialize_grant_privilege");
    expect(result).toMatch(/privilege\s*=\s*"USAGE"/);
    expect(result).toMatch(/object_type\s*=\s*"CLUSTER"/);
    expect(result).toMatch(/object_name\s*=\s*"prod"/);
  });

  it("generates system privilege resources", () => {
    const result = generateTerraformCode(
      createState({
        name: "admin",
        privileges: [
          {
            objectType: "system",
            objectId: "system",
            objectName: "system",
            privileges: ["CREATEROLE", "CREATEDB"],
          },
        ],
      }),
    );

    expect(result).toContain("materialize_grant_system_privilege");
    expect(result).toMatch(/privilege\s*=\s*"CREATEROLE"/);
    expect(result).toMatch(/privilege\s*=\s*"CREATEDB"/);
    expect(result).not.toContain("object_type");
  });

  it("sanitizes resource IDs (replaces special chars with underscores)", () => {
    const result = generateTerraformCode(
      createState({ name: "my-special-role" }),
    );

    expect(result).toContain('"my_special_role"');
  });

  it("uses object attributes based on object type", () => {
    // Cluster: only object_name
    let result = generateTerraformCode(
      createState({
        name: "test",
        privileges: [
          {
            objectType: "cluster",
            objectId: "c1",
            objectName: "prod",
            privileges: ["USAGE"],
          },
        ],
      }),
    );
    expect(result).toMatch(/object_name\s*=\s*"prod"/);
    expect(result).not.toContain("database_name");

    // Schema: database_name + schema_name
    result = generateTerraformCode(
      createState({
        name: "test",
        privileges: [
          {
            objectType: "schema",
            objectId: "s1",
            objectName: "schema",
            databaseName: "db",
            privileges: ["USAGE"],
          },
        ],
      }),
    );
    expect(result).toMatch(/database_name\s*=\s*"db"/);
    expect(result).toMatch(/schema_name\s*=\s*"schema"/);

    // Table: database_name + schema_name + object_name
    result = generateTerraformCode(
      createState({
        name: "test",
        privileges: [
          {
            objectType: "table",
            objectId: "t1",
            objectName: "table",
            databaseName: "db",
            schemaName: "schema",
            privileges: ["SELECT"],
          },
        ],
      }),
    );
    expect(result).toMatch(/database_name\s*=\s*"db"/);
    expect(result).toMatch(/schema_name\s*=\s*"schema"/);
    expect(result).toMatch(/object_name\s*=\s*"table"/);
  });

  it("creates separate resources for multiple privileges on same object", () => {
    const result = generateTerraformCode(
      createState({
        name: "test",
        privileges: [
          {
            objectType: "cluster",
            objectId: "c1",
            objectName: "prod",
            privileges: ["USAGE", "CREATE"],
          },
        ],
      }),
    );

    expect(result).toContain("_cluster_usage_");
    expect(result).toContain("_cluster_create_");
  });

  it("handles object names containing periods", () => {
    const result = generateTerraformCode(
      createState({
        name: "test",
        privileges: [
          {
            objectType: "table",
            objectId: "t1",
            objectName: "wee..woo",
            databaseName: "db",
            schemaName: "schema",
            privileges: ["SELECT"],
          },
        ],
      }),
    );

    expect(result).toMatch(/database_name\s*=\s*"db"/);
    expect(result).toMatch(/schema_name\s*=\s*"schema"/);
    expect(result).toMatch(/object_name\s*=\s*"wee\.\.woo"/);
  });

  it("skips privileges with empty array", () => {
    const result = generateTerraformCode(
      createState({
        name: "test",
        privileges: [
          {
            objectType: "cluster",
            objectId: "c1",
            objectName: "prod",
            privileges: [],
          },
          {
            objectType: "database",
            objectId: "d1",
            objectName: "db",
            privileges: ["USAGE"],
          },
        ],
      }),
    );

    expect(result).not.toContain("cluster");
    expect(result).toContain("database_usage");
  });
});
