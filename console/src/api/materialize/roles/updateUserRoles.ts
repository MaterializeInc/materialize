// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";

import { grantRoleMember } from "./grantRoleMember";
import { revokeRoleMember } from "./revokeRoleMember";

export type UpdateUserRolesError = {
  roleName: string;
  operation: "grant" | "revoke";
  error: { errorMessage: string };
};

export type UpdateUserRolesSuccess = {
  roleName: string;
  operation: "grant" | "revoke";
};

export type UpdateUserRolesInput = {
  memberName: string;
  rolesToGrant: string[];
  rolesToRevoke: string[];
};

function getErrorMessage(reason: unknown): string {
  return reason instanceof Error ? reason.message : "Unknown error";
}

function processResults<T extends "grant" | "revoke">(
  results: PromiseSettledResult<unknown>[],
  roleNames: string[],
  operation: T,
): {
  succeeded: UpdateUserRolesSuccess[];
  failed: UpdateUserRolesError[];
} {
  const succeeded: UpdateUserRolesSuccess[] = [];
  const failed: UpdateUserRolesError[] = [];

  results.forEach((result, i) => {
    const roleName = roleNames[i];
    if (result.status === "fulfilled") {
      succeeded.push({ roleName, operation });
    } else {
      failed.push({
        roleName,
        operation,
        error: { errorMessage: getErrorMessage(result.reason) },
      });
    }
  });

  return { succeeded, failed };
}

export async function updateUserRoles({
  variables,
  requestOptions,
}: {
  variables: UpdateUserRolesInput;
  requestOptions?: RequestInit;
}) {
  const { memberName, rolesToGrant, rolesToRevoke } = variables;

  const rolesBase = buildRegionQueryKey("roles");
  const grantQueryKey = [
    ...rolesBase,
    buildQueryKeyPart("grantRoleMember"),
  ] as const;
  const revokeQueryKey = [
    ...rolesBase,
    buildQueryKeyPart("revokeRoleMember"),
  ] as const;

  // Execute all operations in parallel
  const [grantResults, revokeResults] = await Promise.all([
    Promise.allSettled(
      rolesToGrant.map((roleName) =>
        grantRoleMember({
          variables: { roleName, memberName },
          queryKey: grantQueryKey,
          requestOptions,
        }),
      ),
    ),
    Promise.allSettled(
      rolesToRevoke.map((roleName) =>
        revokeRoleMember({
          variables: { roleName, memberName },
          queryKey: revokeQueryKey,
          requestOptions,
        }),
      ),
    ),
  ]);

  // Process results
  const grantProcessed = processResults(grantResults, rolesToGrant, "grant");
  const revokeProcessed = processResults(
    revokeResults,
    rolesToRevoke,
    "revoke",
  );

  return {
    succeeded: [...grantProcessed.succeeded, ...revokeProcessed.succeeded],
    failed: [...grantProcessed.failed, ...revokeProcessed.failed],
  };
}

export default updateUserRoles;
