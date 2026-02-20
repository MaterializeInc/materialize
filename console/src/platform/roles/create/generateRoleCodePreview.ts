// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  buildCreateRoleQuery,
  buildInheritedRoleGrantQuery,
  buildPrivilegeGrantQuery,
} from "~/api/materialize/roles/createRole";

import {
  OBJECT_TYPE_SQL_NAMES,
  ObjectTypeId,
  PrivilegeGrant,
} from "./constants";
import { CreateRoleFormState } from "./types";

/** Convert a name to a valid Terraform resource identifier */
export function toTerraformResourceId(name: string): string {
  return name.toLowerCase().replace(/[^a-z0-9_]/g, "_");
}

/** Build Terraform object attributes based on object type and grant fields */
export function buildObjectAttrs(grant: {
  objectType: string;
  objectName: string;
  databaseName?: string;
  schemaName?: string;
}): string {
  const attrs: string[] = [];

  if (grant.objectType === "schema") {
    if (grant.databaseName) {
      attrs.push(`  database_name = "${grant.databaseName}"`);
    }
    attrs.push(`  schema_name = "${grant.objectName}"`);
  } else {
    if (grant.databaseName) {
      attrs.push(`  database_name = "${grant.databaseName}"`);
    }
    if (grant.schemaName) {
      attrs.push(`  schema_name = "${grant.schemaName}"`);
    }
    attrs.push(`  object_name = "${grant.objectName}"`);
  }

  return attrs.join("\n");
}

/** Generate Terraform block for a role resource */
export function generateTerraformRoleBlock(roleName: string): string {
  const roleId = toTerraformResourceId(roleName);
  return `resource "materialize_role" "${roleId}" {
  name = "${roleName}"
}`;
}

/** Generate Terraform block for a role membership grant */
export function generateTerraformGrantRoleBlock(
  roleName: string,
  inheritedRoleName: string,
  useRoleRef = true,
): string {
  const roleId = toTerraformResourceId(roleName);
  const grantId = toTerraformResourceId(
    `${roleName}_member_${inheritedRoleName}`,
  );
  const memberName = useRoleRef
    ? `materialize_role.${roleId}.name`
    : `"${roleName}"`;

  return `resource "materialize_grant_role" "${grantId}" {
  role_name   = "${inheritedRoleName}"
  member_name = ${memberName}
}`;
}

/** Generate Terraform block for a privilege grant */
export function generateTerraformPrivilegeBlock(
  roleName: string,
  grant: PrivilegeGrant,
  privilege: string,
  useRoleRef = true,
): string {
  const roleId = toTerraformResourceId(roleName);
  const objectType = OBJECT_TYPE_SQL_NAMES[grant.objectType as ObjectTypeId];
  const lastPart = grant.objectName.split(".").pop();
  const grantId = toTerraformResourceId(
    `${roleName}_${grant.objectType}_${privilege}_${lastPart}`,
  );
  const granteeName = useRoleRef
    ? `materialize_role.${roleId}.name`
    : `"${roleName}"`;

  if (grant.objectType === "system") {
    return `resource "materialize_grant_system_privilege" "${grantId}" {
  grantee_name = ${granteeName}
  privilege    = "${privilege}"
}`;
  }

  return `resource "materialize_grant_privilege" "${grantId}" {
  grantee_name = ${granteeName}
  privilege = "${privilege}"
  object_type = "${objectType}"
${buildObjectAttrs(grant)}
}`;
}

/**
 * Generates SQL statements for creating a role with privileges
 */
export function generateCreateRoleSql(state: CreateRoleFormState): string {
  if (!state.name.trim()) {
    return "-- Enter a role name to see the SQL";
  }

  const statements: string[] = [];

  // CREATE ROLE statement
  statements.push(buildCreateRoleQuery(state.name).sql + ";");

  // GRANT inherited roles
  for (const role of state.inheritedRoles) {
    statements.push(
      buildInheritedRoleGrantQuery({
        inheritedRoleName: role.name,
        roleName: state.name,
      }).sql + ";",
    );
  }

  // GRANT privileges on objects
  for (const grant of state.privileges) {
    if (grant.privileges.length === 0) continue;

    statements.push(
      buildPrivilegeGrantQuery({ grant, roleName: state.name }).sql + ";",
    );
  }

  return statements.join("\n\n");
}

/**
 * Generates Terraform configuration for creating a role with privileges
 */
export function generateTerraformCode(state: CreateRoleFormState): string {
  if (!state.name.trim()) {
    return "# Enter a role name to see the Terraform configuration";
  }

  const blocks: string[] = [];

  // Role resource
  blocks.push(generateTerraformRoleBlock(state.name));

  // Grant role memberships
  for (const role of state.inheritedRoles) {
    blocks.push(generateTerraformGrantRoleBlock(state.name, role.name));
  }

  // Grant privileges on objects
  for (const grant of state.privileges) {
    if (grant.privileges.length === 0) continue;

    for (const privilege of grant.privileges) {
      blocks.push(
        generateTerraformPrivilegeBlock(state.name, grant, privilege),
      );
    }
  }

  return blocks.join("\n\n");
}
