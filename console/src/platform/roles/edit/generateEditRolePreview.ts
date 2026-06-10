// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  buildInheritedRoleGrantQuery,
  buildPrivilegeGrantQuery,
} from "~/api/materialize/roles/createRole";
import {
  buildRevokeInheritedRoleQuery,
  buildRevokePrivilegeQuery,
  calculateInheritedRoleDiff,
  calculatePrivilegeDiff,
  createPrivilegeKey,
  ExistingRoleState,
} from "~/api/materialize/roles/editRole";
import {
  generateCreateRoleSql,
  generateTerraformCode,
  generateTerraformGrantRoleBlock,
  generateTerraformPrivilegeBlock,
  generateTerraformRoleBlock,
} from "~/platform/roles/create/generateRoleCodePreview";
import { CreateRoleFormState } from "~/platform/roles/create/types";

type RoleDiffs = {
  inheritedRoles: ReturnType<typeof calculateInheritedRoleDiff>;
  privileges: ReturnType<typeof calculatePrivilegeDiff>;
  hasChanges: boolean;
};

function computeDiffs(
  existingState: ExistingRoleState,
  newState: CreateRoleFormState,
): RoleDiffs {
  const inheritedRoles = calculateInheritedRoleDiff(
    existingState.inheritedRoles,
    newState.inheritedRoles,
  );

  const privileges = calculatePrivilegeDiff(
    existingState.privileges,
    newState.privileges,
  );

  const hasChanges =
    inheritedRoles.toGrant.length > 0 ||
    inheritedRoles.toRevoke.length > 0 ||
    privileges.toGrant.length > 0 ||
    privileges.toRevoke.length > 0;

  return { inheritedRoles, privileges, hasChanges };
}

function getUnchangedRoles(
  existingRoles: ExistingRoleState["inheritedRoles"],
  toRevoke: ExistingRoleState["inheritedRoles"],
) {
  const revokeNames = new Set(toRevoke.map((r) => r.name));
  return existingRoles.filter((r) => !revokeNames.has(r.name));
}

/**
 * Generates SQL statements showing the operations that will be executed.
 * When no changes: shows the current role state.
 * When changes: shows GRANT/REVOKE statements that will run.
 */
export function generateEditRoleSql(
  roleName: string,
  existingState: ExistingRoleState | null,
  newState: CreateRoleFormState,
): string {
  if (!existingState) {
    return "-- Loading role data...";
  }

  const diffs = computeDiffs(existingState, newState);

  // No changes - show current state
  if (!diffs.hasChanges) {
    return generateCreateRoleSql({
      name: roleName,
      inheritedRoles: existingState.inheritedRoles,
      privileges: existingState.privileges,
    });
  }

  // Has changes - show full state with changes marked
  const statements: string[] = [];

  // Start with CREATE ROLE
  statements.push(`CREATE ROLE ${JSON.stringify(roleName)};`);

  // Unchanged inherited roles
  for (const role of getUnchangedRoles(
    existingState.inheritedRoles,
    diffs.inheritedRoles.toRevoke,
  )) {
    statements.push(
      buildInheritedRoleGrantQuery({
        inheritedRoleName: role.name,
        roleName,
      }).sql + ";",
    );
  }

  // New inherited roles (to be granted)
  for (const role of diffs.inheritedRoles.toGrant) {
    statements.push(
      "-- NEW:\n" +
        buildInheritedRoleGrantQuery({
          inheritedRoleName: role.name,
          roleName,
        }).sql +
        ";",
    );
  }

  // Revoked inherited roles
  for (const role of diffs.inheritedRoles.toRevoke) {
    statements.push(
      "-- REMOVE:\n" +
        buildRevokeInheritedRoleQuery({
          inheritedRoleName: role.name,
          roleName,
        }).sql +
        ";",
    );
  }

  // Unchanged privileges
  const revokeKeys = new Set(diffs.privileges.toRevoke.map(createPrivilegeKey));
  for (const grant of existingState.privileges) {
    if (revokeKeys.has(createPrivilegeKey(grant))) continue;
    if (grant.privileges.length === 0) continue;
    statements.push(buildPrivilegeGrantQuery({ grant, roleName }).sql + ";");
  }

  // New privileges (to be granted)
  for (const grant of diffs.privileges.toGrant) {
    if (grant.privileges.length === 0) continue;
    statements.push(
      "-- NEW:\n" + buildPrivilegeGrantQuery({ grant, roleName }).sql + ";",
    );
  }

  // Revoked privileges
  for (const grant of diffs.privileges.toRevoke) {
    if (grant.privileges.length === 0) continue;
    statements.push(
      "-- REMOVE:\n" + buildRevokePrivilegeQuery({ grant, roleName }).sql + ";",
    );
  }

  return statements.join("\n\n");
}

/**
 * Generates Terraform configuration showing the operations needed.
 * When no changes: shows the current role state.
 * When changes: shows resources to ADD and REMOVE.
 */
export function generateEditRoleTerraform(
  roleName: string,
  existingState: ExistingRoleState | null,
  newState: CreateRoleFormState,
): string {
  if (!existingState) {
    return "# Loading role data...";
  }

  const diffs = computeDiffs(existingState, newState);

  // No changes - show current state
  if (!diffs.hasChanges) {
    return generateTerraformCode({
      name: roleName,
      inheritedRoles: existingState.inheritedRoles,
      privileges: existingState.privileges,
    });
  }

  // Has changes - show full state with changes marked
  const blocks: string[] = [];

  // Role resource (always present)
  blocks.push(generateTerraformRoleBlock(roleName));

  // Unchanged inherited roles
  for (const role of getUnchangedRoles(
    existingState.inheritedRoles,
    diffs.inheritedRoles.toRevoke,
  )) {
    blocks.push(generateTerraformGrantRoleBlock(roleName, role.name, false));
  }

  // New inherited roles
  for (const role of diffs.inheritedRoles.toGrant) {
    const block = generateTerraformGrantRoleBlock(roleName, role.name, false);
    blocks.push(`# NEW:\n${block}`);
  }

  // Build set of privilege keys being revoked for quick lookup
  const revokePrivilegeKeys = new Set<string>();
  for (const grant of diffs.privileges.toRevoke) {
    for (const privilege of grant.privileges) {
      revokePrivilegeKeys.add(`${createPrivilegeKey(grant)}:${privilege}`);
    }
  }

  // Unchanged privileges
  for (const grant of existingState.privileges) {
    for (const privilege of grant.privileges) {
      const key = `${createPrivilegeKey(grant)}:${privilege}`;
      if (revokePrivilegeKeys.has(key)) continue;
      blocks.push(
        generateTerraformPrivilegeBlock(roleName, grant, privilege, false),
      );
    }
  }

  // New privileges
  for (const grant of diffs.privileges.toGrant) {
    if (grant.privileges.length === 0) continue;
    for (const privilege of grant.privileges) {
      const block = generateTerraformPrivilegeBlock(
        roleName,
        grant,
        privilege,
        false,
      );
      blocks.push(`# NEW:\n${block}`);
    }
  }

  return blocks.join("\n\n");
}
