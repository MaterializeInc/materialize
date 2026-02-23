// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import { createRole } from "~/api/materialize/roles/createRole";
import { dropRole, DropRoleVariables } from "~/api/materialize/roles/dropRole";
import {
  editRole,
  EditRoleFormState,
  ExistingRoleState,
} from "~/api/materialize/roles/editRole";
import {
  grantRoleMember,
  GrantRoleMemberVariables,
} from "~/api/materialize/roles/grantRoleMember";
import {
  revokeRoleMember,
  RevokeRoleMemberVariables,
} from "~/api/materialize/roles/revokeRoleMember";
import {
  fetchRoleDetails,
  RoleDetailsParameters,
} from "~/api/materialize/roles/roleDetails";
import { fetchRoleGraphEdges } from "~/api/materialize/roles/roleGraph";
import {
  fetchRoleMembers,
  fetchRoleMembership,
  RoleMembersParameters,
} from "~/api/materialize/roles/roleMembers";
import {
  fetchRolePrivileges,
  RolePrivilegesParameters,
} from "~/api/materialize/roles/rolePrivileges";
import { fetchRolesList } from "~/api/materialize/roles/rolesList";
import {
  updateUserRoles,
  UpdateUserRolesInput,
} from "~/api/materialize/roles/updateUserRoles";
import { fetchUsersList } from "~/api/materialize/roles/usersList";
import { PrivilegeGrant } from "~/platform/roles/create/constants";

import { CreateRoleFormState } from "./create/types";

export const roleQueryKeys = {
  all: () => buildRegionQueryKey("roles"),
  list: () => [...roleQueryKeys.all(), buildQueryKeyPart("list", {})] as const,
  edges: () =>
    [...roleQueryKeys.all(), buildQueryKeyPart("edges", {})] as const,
  details: (params: RoleDetailsParameters) =>
    [...roleQueryKeys.all(), buildQueryKeyPart("details", params)] as const,
  privileges: (params: RolePrivilegesParameters) =>
    [...roleQueryKeys.all(), buildQueryKeyPart("privileges", params)] as const,
  members: (params: RoleMembersParameters) =>
    [...roleQueryKeys.all(), buildQueryKeyPart("members", params)] as const,
  grantedRoles: (params: { roleName: string }) =>
    [
      ...roleQueryKeys.all(),
      buildQueryKeyPart("grantedRoles", params),
    ] as const,
  revokeRoleMember: () =>
    [...roleQueryKeys.all(), buildQueryKeyPart("revokeRoleMember")] as const,
  grantRoleMember: () =>
    [...roleQueryKeys.all(), buildQueryKeyPart("grantRoleMember")] as const,
  updateUserRoles: () =>
    [...roleQueryKeys.all(), buildQueryKeyPart("updateUserRoles")] as const,
  createRole: (params: { name: CreateRoleFormState["name"] }) =>
    [...roleQueryKeys.all(), buildQueryKeyPart("createRole", params)] as const,
  grantInheritedRole: (params: {
    name: CreateRoleFormState["name"];
    inheritedRole: CreateRoleFormState["inheritedRoles"][0];
  }) =>
    [
      ...roleQueryKeys.all(),
      buildQueryKeyPart("grantInheritedRole", params),
    ] as const,
  grantPrivilege: (params: {
    name: CreateRoleFormState["name"];
    privilege: CreateRoleFormState["privileges"][0];
  }) =>
    [
      ...roleQueryKeys.all(),
      buildQueryKeyPart("grantPrivilege", params),
    ] as const,
  // Edit role query keys
  editRole: (params: { name: string }) =>
    [...roleQueryKeys.all(), buildQueryKeyPart("editRole", params)] as const,
  editGrantInheritedRole: (params: {
    name: string;
    inheritedRole: { id: string; name: string };
  }) =>
    [
      ...roleQueryKeys.all(),
      buildQueryKeyPart("editGrantInheritedRole", params),
    ] as const,
  editRevokeInheritedRole: (params: {
    name: string;
    inheritedRole: { id: string; name: string };
  }) =>
    [
      ...roleQueryKeys.all(),
      buildQueryKeyPart("editRevokeInheritedRole", params),
    ] as const,
  editGrantPrivilege: (params: { name: string; privilege: PrivilegeGrant }) =>
    [
      ...roleQueryKeys.all(),
      buildQueryKeyPart("editGrantPrivilege", params),
    ] as const,
  editRevokePrivilege: (params: { name: string; privilege: PrivilegeGrant }) =>
    [
      ...roleQueryKeys.all(),
      buildQueryKeyPart("editRevokePrivilege", params),
    ] as const,
  dropRole: (params: { name: string }) =>
    [...roleQueryKeys.all(), buildQueryKeyPart("dropRole", params)] as const,
};

export const userQueryKeys = {
  all: () => buildRegionQueryKey("users"),
  list: () => [...userQueryKeys.all(), buildQueryKeyPart("list", {})] as const,
};

export function useRolesList() {
  return useQuery({
    queryKey: roleQueryKeys.list(),
    queryFn: ({ queryKey, signal }) => {
      return fetchRolesList({
        queryKey,
        requestOptions: { signal },
      });
    },
  });
}

export function useUsersList() {
  return useQuery({
    queryKey: userQueryKeys.list(),
    queryFn: ({ queryKey, signal }) => {
      return fetchUsersList({
        queryKey,
        requestOptions: { signal },
      });
    },
    refetchInterval: 5000,
  });
}

export function useRoleGraphEdges() {
  return useQuery({
    queryKey: roleQueryKeys.edges(),
    queryFn: ({ queryKey, signal }) => {
      return fetchRoleGraphEdges({
        queryKey,
        requestOptions: { signal },
      });
    },
  });
}

export function useRoleDetails(params: RoleDetailsParameters) {
  return useQuery({
    queryKey: roleQueryKeys.details(params),
    queryFn: ({ queryKey, signal }) => {
      const [, parameters] = queryKey;
      return fetchRoleDetails({
        parameters,
        queryKey,
        requestOptions: { signal },
      });
    },
  });
}

export function useRolePrivileges(params: RolePrivilegesParameters) {
  return useQuery({
    queryKey: roleQueryKeys.privileges(params),
    queryFn: ({ queryKey, signal }) => {
      const [, parameters] = queryKey;
      return fetchRolePrivileges({
        parameters,
        queryKey,
        requestOptions: { signal },
      });
    },
  });
}

export function useRoleMembers(
  params: RoleMembersParameters & { enabled?: boolean },
) {
  const { enabled = true, ...queryParams } = params;
  return useQuery({
    queryKey: roleQueryKeys.members(queryParams),
    queryFn: ({ queryKey, signal }) => {
      const [, parameters] = queryKey;
      return fetchRoleMembers({
        parameters,
        queryKey,
        requestOptions: { signal },
      });
    },
    enabled,
  });
}

export function useGrantedRoles(params: { roleName: string }) {
  return useQuery({
    queryKey: roleQueryKeys.grantedRoles(params),
    queryFn: ({ queryKey, signal }) => {
      return fetchRoleMembership({
        filters: { childRoleName: params.roleName },
        queryKey,
        requestOptions: { signal },
      });
    },
  });
}

export function useRevokeRoleMember() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationKey: roleQueryKeys.revokeRoleMember(),
    mutationFn: (variables: RevokeRoleMemberVariables) => {
      return revokeRoleMember({
        variables,
        queryKey: roleQueryKeys.revokeRoleMember(),
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: roleQueryKeys.all(),
      });
    },
  });
}

export function useGrantRoleMember() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationKey: roleQueryKeys.grantRoleMember(),
    mutationFn: (variables: GrantRoleMemberVariables) => {
      return grantRoleMember({
        variables,
        queryKey: roleQueryKeys.grantRoleMember(),
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: roleQueryKeys.all(),
      });
      queryClient.invalidateQueries({
        queryKey: userQueryKeys.all(),
      });
    },
  });
}

export function useUpdateUserRoles() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationKey: roleQueryKeys.updateUserRoles(),
    mutationFn: (variables: UpdateUserRolesInput) => {
      return updateUserRoles({ variables });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: roleQueryKeys.all(),
      });
      queryClient.invalidateQueries({
        queryKey: userQueryKeys.all(),
      });
    },
  });
}

export function useCreateRole() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (variables: CreateRoleFormState) => {
      return createRole({
        variables,
        queryKeys: {
          createRole: roleQueryKeys.createRole({ name: variables.name }),
          grantInheritedRole: (inheritedRole) =>
            roleQueryKeys.grantInheritedRole({
              name: variables.name,
              inheritedRole,
            }),
          grantPrivilege: (privilege) =>
            roleQueryKeys.grantPrivilege({ name: variables.name, privilege }),
        },
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: roleQueryKeys.all(),
      });
    },
  });
}

export function useEditRole() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({
      roleName,
      existingState,
      newState,
    }: {
      roleName: string;
      existingState: ExistingRoleState;
      newState: EditRoleFormState;
    }) => {
      return editRole({
        roleName,
        existingState,
        newState,
        queryKeys: {
          grantInheritedRole: (role) =>
            roleQueryKeys.editGrantInheritedRole({
              name: roleName,
              inheritedRole: role,
            }),
          revokeInheritedRole: (role) =>
            roleQueryKeys.editRevokeInheritedRole({
              name: roleName,
              inheritedRole: role,
            }),
          grantPrivilege: (privilege) =>
            roleQueryKeys.editGrantPrivilege({ name: roleName, privilege }),
          revokePrivilege: (privilege) =>
            roleQueryKeys.editRevokePrivilege({ name: roleName, privilege }),
        },
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: roleQueryKeys.all(),
      });
    },
  });
}

export function useDropRole() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationKey: roleQueryKeys.dropRole({ name: "" }),
    mutationFn: (variables: DropRoleVariables) => {
      return dropRole({
        variables,
        queryKey: roleQueryKeys.dropRole({ name: variables.roleName }),
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: roleQueryKeys.all(),
      });
    },
  });
}
