// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { calculateInheritedRoleDiff, calculatePrivilegeDiff } from "./editRole";

describe("calculateInheritedRoleDiff", () => {
  it("returns roles to grant and revoke", () => {
    const existing = [
      { id: "1", name: "role_a" },
      { id: "2", name: "role_b" },
    ];
    const updated = [
      { id: "2", name: "role_b" },
      { id: "3", name: "role_c" },
    ];

    const result = calculateInheritedRoleDiff(existing, updated);

    expect(result.toGrant).toEqual([{ id: "3", name: "role_c" }]);
    expect(result.toRevoke).toEqual([{ id: "1", name: "role_a" }]);
  });

  it("returns empty arrays when no changes", () => {
    const roles = [{ id: "1", name: "role_a" }];

    const result = calculateInheritedRoleDiff(roles, roles);

    expect(result.toGrant).toEqual([]);
    expect(result.toRevoke).toEqual([]);
  });
});

describe("calculatePrivilegeDiff", () => {
  it("returns privileges to grant and revoke", () => {
    const existing = [
      {
        objectType: "database" as const,
        objectId: "d1",
        objectName: "db_a",
        privileges: ["USAGE"],
      },
    ];
    const updated = [
      {
        objectType: "database" as const,
        objectId: "d2",
        objectName: "db_b",
        privileges: ["CREATE"],
      },
    ];

    const result = calculatePrivilegeDiff(existing, updated);

    expect(result.toGrant).toEqual([
      {
        objectType: "database",
        objectId: "d2",
        objectName: "db_b",
        privileges: ["CREATE"],
      },
    ]);
    expect(result.toRevoke).toEqual([
      {
        objectType: "database",
        objectId: "d1",
        objectName: "db_a",
        privileges: ["USAGE"],
      },
    ]);
  });

  it("handles partial privilege changes on the same object", () => {
    const existing = [
      {
        objectType: "table" as const,
        objectId: "t1",
        objectName: "my_table",
        databaseName: "db",
        schemaName: "public",
        privileges: ["SELECT", "INSERT"],
      },
    ];
    const updated = [
      {
        objectType: "table" as const,
        objectId: "t1",
        objectName: "my_table",
        databaseName: "db",
        schemaName: "public",
        privileges: ["SELECT", "UPDATE"],
      },
    ];

    const result = calculatePrivilegeDiff(existing, updated);

    // Only UPDATE should be granted (SELECT already exists)
    expect(result.toGrant).toEqual([
      {
        objectType: "table",
        objectId: "t1",
        objectName: "my_table",
        databaseName: "db",
        schemaName: "public",
        privileges: ["UPDATE"],
      },
    ]);
    // Only INSERT should be revoked (SELECT is kept)
    expect(result.toRevoke).toEqual([
      {
        objectType: "table",
        objectId: "t1",
        objectName: "my_table",
        databaseName: "db",
        schemaName: "public",
        privileges: ["INSERT"],
      },
    ]);
  });
});
