// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { sql } from "kysely";

import { buildFullyQualifiedSchemaIdentifier } from "~/api/materialize";
import { queryBuilder } from "~/api/materialize/db";
import { executeSqlV2 } from "~/api/materialize/executeSqlV2";
import {
  GRANT_SQL_OBJECT_TYPES,
  ObjectTypeId,
  PrivilegeGrant,
} from "~/platform/roles/create/constants";

import {
  buildInheritedRoleGrantQuery,
  buildPrivilegeGrantQuery,
} from "./createRole";
import { processResults } from "./roleUtils";

export type ExistingRoleState = {
  inheritedRoles: { id: string; name: string }[];
  privileges: PrivilegeGrant[];
};

export type EditRoleFormState = {
  name: string;
  inheritedRoles: { id: string; name: string }[];
  privileges: PrivilegeGrant[];
};

/**
 * Build a compiled query for revoking an inherited role
 */
export function buildRevokeInheritedRoleQuery({
  inheritedRoleName,
  roleName,
}: {
  inheritedRoleName: string;
  roleName: string;
}) {
  return sql`REVOKE ${sql.id(inheritedRoleName)} FROM ${sql.id(roleName)}`.compile(
    queryBuilder,
  );
}

/**
 * Build a compiled query for revoking privileges on an object from a role
 */
export function buildRevokePrivilegeQuery({
  grant,
  roleName,
}: {
  grant: PrivilegeGrant;
  roleName: string;
}) {
  const privilegeList = grant.privileges.join(", ");
  const objectTypeName =
    GRANT_SQL_OBJECT_TYPES[grant.objectType as ObjectTypeId];

  const objectNameSql = grant.schemaName
    ? buildFullyQualifiedSchemaIdentifier({
        databaseName: grant.databaseName,
        schemaName: grant.schemaName,
        name: grant.objectName,
      })
    : sql.id(grant.objectName);

  return sql`REVOKE ${sql.raw(privilegeList)} ON ${sql.raw(objectTypeName)} ${objectNameSql} FROM ${sql.id(roleName)}`.compile(
    queryBuilder,
  );
}

/**
 * Calculate differences between existing and new inherited roles
 */
export function calculateInheritedRoleDiff(
  existing: { id: string; name: string }[],
  updated: { id: string; name: string }[],
): {
  toGrant: { id: string; name: string }[];
  toRevoke: { id: string; name: string }[];
} {
  const existingNames = new Set(existing.map((r) => r.name));
  const updatedNames = new Set(updated.map((r) => r.name));

  const namesToGrant = updatedNames.difference(existingNames);
  const namesToRevoke = existingNames.difference(updatedNames);

  const toGrant = updated.filter((r) => namesToGrant.has(r.name));
  const toRevoke = existing.filter((r) => namesToRevoke.has(r.name));

  return { toGrant, toRevoke };
}

/** Create a unique key for a privilege grant based on the object it targets */
export function createPrivilegeKey(grant: PrivilegeGrant): string {
  return `${grant.objectType}:${grant.databaseName ?? ""}:${grant.schemaName ?? ""}:${grant.objectName}`;
}

/** Build a Map from grants keyed by their object identifier */
function buildGrantMap(grants: PrivilegeGrant[]): Map<string, PrivilegeGrant> {
  return new Map(grants.map((g) => [createPrivilegeKey(g), g]));
}

/** Find privileges in `from` that don't exist in `against` */
function diffPrivileges(from: string[], against: string[]): string[] {
  return [...new Set(from).difference(new Set(against))];
}

/**
 * Calculate differences between existing and new privileges
 */
export function calculatePrivilegeDiff(
  existing: PrivilegeGrant[],
  updated: PrivilegeGrant[],
): {
  toGrant: PrivilegeGrant[];
  toRevoke: PrivilegeGrant[];
} {
  const existingByKey = buildGrantMap(existing);
  const updatedByKey = buildGrantMap(updated);

  const toGrant = [...updatedByKey.entries()].reduce<PrivilegeGrant[]>(
    (acc, [key, grant]) => {
      const diff = diffPrivileges(
        grant.privileges,
        existingByKey.get(key)?.privileges ?? [],
      );
      return diff.length > 0 ? [...acc, { ...grant, privileges: diff }] : acc;
    },
    [],
  );

  const toRevoke = [...existingByKey.entries()].reduce<PrivilegeGrant[]>(
    (acc, [key, grant]) => {
      const diff = diffPrivileges(
        grant.privileges,
        updatedByKey.get(key)?.privileges ?? [],
      );
      return diff.length > 0 ? [...acc, { ...grant, privileges: diff }] : acc;
    },
    [],
  );

  return { toGrant, toRevoke };
}

export type EditRoleQueryKeys = {
  grantInheritedRole: (role: { id: string; name: string }) => QueryKey;
  revokeInheritedRole: (role: { id: string; name: string }) => QueryKey;
  grantPrivilege: (privilege: PrivilegeGrant) => QueryKey;
  revokePrivilege: (privilege: PrivilegeGrant) => QueryKey;
};

/**
 * Edit a role by applying differential changes.
 * Compares existing state with new state and executes GRANT/REVOKE operations.
 */
export async function editRole({
  roleName,
  existingState,
  newState,
  queryKeys,
  requestOptions,
}: {
  roleName: string;
  existingState: ExistingRoleState;
  newState: EditRoleFormState;
  queryKeys: EditRoleQueryKeys;
  requestOptions?: RequestInit;
}) {
  const inheritedRoleDiff = calculateInheritedRoleDiff(
    existingState.inheritedRoles,
    newState.inheritedRoles,
  );

  const privilegeDiff = calculatePrivilegeDiff(
    existingState.privileges,
    newState.privileges,
  );

  // Execute all operations in parallel
  const [
    grantInheritedResults,
    revokeInheritedResults,
    grantPrivilegeResults,
    revokePrivilegeResults,
  ] = await Promise.all([
    // Grant new inherited roles
    Promise.allSettled(
      inheritedRoleDiff.toGrant.map((role) => {
        const query = buildInheritedRoleGrantQuery({
          inheritedRoleName: role.name,
          roleName,
        });
        return executeSqlV2({
          queries: query,
          queryKey: queryKeys.grantInheritedRole(role),
          requestOptions,
        });
      }),
    ),
    // Revoke removed inherited roles
    Promise.allSettled(
      inheritedRoleDiff.toRevoke.map((role) => {
        const query = buildRevokeInheritedRoleQuery({
          inheritedRoleName: role.name,
          roleName,
        });
        return executeSqlV2({
          queries: query,
          queryKey: queryKeys.revokeInheritedRole(role),
          requestOptions,
        });
      }),
    ),
    // Grant new privileges
    Promise.allSettled(
      privilegeDiff.toGrant.map((grant) => {
        const query = buildPrivilegeGrantQuery({ grant, roleName });
        return executeSqlV2({
          queries: query,
          queryKey: queryKeys.grantPrivilege(grant),
          requestOptions,
        });
      }),
    ),
    // Revoke removed privileges
    Promise.allSettled(
      privilegeDiff.toRevoke.map((grant) => {
        const query = buildRevokePrivilegeQuery({ grant, roleName });
        return executeSqlV2({
          queries: query,
          queryKey: queryKeys.revokePrivilege(grant),
          requestOptions,
        });
      }),
    ),
  ]);

  const grantInheritedProcessed = processResults(
    grantInheritedResults,
    inheritedRoleDiff.toGrant,
    (role) =>
      buildInheritedRoleGrantQuery({ inheritedRoleName: role.name, roleName })
        .sql,
  );

  const revokeInheritedProcessed = processResults(
    revokeInheritedResults,
    inheritedRoleDiff.toRevoke,
    (role) =>
      buildRevokeInheritedRoleQuery({ inheritedRoleName: role.name, roleName })
        .sql,
  );

  const grantPrivilegeProcessed = processResults(
    grantPrivilegeResults,
    privilegeDiff.toGrant,
    (grant) => buildPrivilegeGrantQuery({ grant, roleName }).sql,
  );

  const revokePrivilegeProcessed = processResults(
    revokePrivilegeResults,
    privilegeDiff.toRevoke,
    (grant) => buildRevokePrivilegeQuery({ grant, roleName }).sql,
  );

  return {
    succeeded: [
      ...grantInheritedProcessed.succeeded,
      ...revokeInheritedProcessed.succeeded,
      ...grantPrivilegeProcessed.succeeded,
      ...revokePrivilegeProcessed.succeeded,
    ],
    failed: [
      ...grantInheritedProcessed.failed,
      ...revokeInheritedProcessed.failed,
      ...grantPrivilegeProcessed.failed,
      ...revokePrivilegeProcessed.failed,
    ],
  };
}
