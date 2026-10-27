// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { CompiledQuery, sql } from "kysely";

import { buildFullyQualifiedSchemaIdentifier } from "~/api/materialize";
import { queryBuilder } from "~/api/materialize/db";
import { executeSqlV2 } from "~/api/materialize/executeSqlV2";
import {
  GRANT_SQL_OBJECT_TYPES,
  ObjectTypeId,
} from "~/platform/roles/create/constants";
import { CreateRoleFormState } from "~/platform/roles/create/types";

import { processResults } from "./roleUtils";

/**
 * Build a compiled query for creating a role
 */
export function buildCreateRoleQuery(roleName: string): CompiledQuery {
  return sql`CREATE ROLE ${sql.id(roleName)}`.compile(queryBuilder);
}

/**
 * Build a compiled query for granting an inherited role
 */
export function buildInheritedRoleGrantQuery({
  inheritedRoleName,
  roleName,
}: {
  inheritedRoleName: string;
  roleName: string;
}): CompiledQuery {
  return sql`GRANT ${sql.id(inheritedRoleName)} TO ${sql.id(roleName)}`.compile(
    queryBuilder,
  );
}

/**
 * Build a compiled query for granting privileges on an object to a role
 */
export function buildPrivilegeGrantQuery({
  grant,
  roleName,
}: {
  grant: CreateRoleFormState["privileges"][0];
  roleName: string;
}): CompiledQuery {
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

  return sql`GRANT ${sql.raw(privilegeList)} ON ${sql.raw(objectTypeName)} ${objectNameSql} TO ${sql.id(roleName)}`.compile(
    queryBuilder,
  );
}

/**
 * Build all SQL statements needed to create a role with privileges
 */
export function buildCreateRoleStatements(
  state: CreateRoleFormState,
): CompiledQuery[] {
  const statements: CompiledQuery[] = [];

  // CREATE ROLE
  statements.push(buildCreateRoleQuery(state.name));

  // GRANT inherited roles
  for (const role of state.inheritedRoles) {
    statements.push(
      buildInheritedRoleGrantQuery({
        inheritedRoleName: role.name,
        roleName: state.name,
      }),
    );
  }

  // GRANT privileges on objects
  for (const grant of state.privileges) {
    if (grant.privileges.length === 0) continue;

    statements.push(buildPrivilegeGrantQuery({ grant, roleName: state.name }));
  }

  return statements;
}

export type CreateRoleQueryKeys = {
  createRole: QueryKey;
  grantInheritedRole: (
    inheritedRole: CreateRoleFormState["inheritedRoles"][0],
  ) => QueryKey;
  grantPrivilege: (privilege: CreateRoleFormState["privileges"][0]) => QueryKey;
};

/**
 * Create a role with partial failure handling.
 * First creates the role, then attempts all GRANT operations in parallel.
 * Returns success/failure information for each operation.
 */
export async function createRole({
  variables,
  queryKeys,
  requestOptions,
}: {
  variables: CreateRoleFormState;
  queryKeys: CreateRoleQueryKeys;
  requestOptions?: RequestInit;
}) {
  // CREATE ROLE (must succeed or throw)
  const createRoleQuery = buildCreateRoleQuery(variables.name);
  await executeSqlV2({
    queries: createRoleQuery,
    queryKey: queryKeys.createRole,
    requestOptions,
  });

  // Execute all grants in parallel
  const privilegeGrants = variables.privileges.filter(
    (grant) => grant.privileges.length > 0,
  );

  const [inheritedRoleResults, privilegeResults] = await Promise.all([
    Promise.allSettled(
      variables.inheritedRoles.map((role) => {
        const grantQuery = buildInheritedRoleGrantQuery({
          inheritedRoleName: role.name,
          roleName: variables.name,
        });
        return executeSqlV2({
          queries: grantQuery,
          queryKey: queryKeys.grantInheritedRole(role),
          requestOptions,
        });
      }),
    ),
    Promise.allSettled(
      privilegeGrants.map((grant) => {
        const grantQuery = buildPrivilegeGrantQuery({
          grant,
          roleName: variables.name,
        });
        return executeSqlV2({
          queries: grantQuery,
          queryKey: queryKeys.grantPrivilege(grant),
          requestOptions,
        });
      }),
    ),
  ]);

  const inheritedRolesProcessed = processResults(
    inheritedRoleResults,
    variables.inheritedRoles,
    (role) =>
      buildInheritedRoleGrantQuery({
        inheritedRoleName: role.name,
        roleName: variables.name,
      }).sql,
  );

  const privilegesProcessed = processResults(
    privilegeResults,
    privilegeGrants,
    (grant) =>
      buildPrivilegeGrantQuery({ grant, roleName: variables.name }).sql,
  );

  return {
    roleCreated: true,
    succeeded: [
      ...inheritedRolesProcessed.succeeded,
      ...privilegesProcessed.succeeded,
    ],
    failed: [...inheritedRolesProcessed.failed, ...privilegesProcessed.failed],
  };
}
