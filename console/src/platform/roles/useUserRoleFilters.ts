// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import { useAllRoles } from "~/store/allRoles";
import { useQueryStringState } from "~/useQueryString";

import { ALL_ROLES_FILTER_ID, RoleFilterOption } from "./RoleFilter";

export function useUserRoleFilters() {
  const [nameValue, setNameValue] = useQueryStringState("user");
  const [roleValue, setRoleValue] = useQueryStringState("userRole");
  const { data: roles } = useAllRoles();

  const roleOptions: RoleFilterOption[] = React.useMemo(
    () => [
      { id: ALL_ROLES_FILTER_ID, label: "All roles" },
      ...(roles?.map((r) => ({ id: r.roleName, label: r.roleName })) ?? []),
    ],
    [roles],
  );

  const selectedRole = React.useMemo(
    () => roleOptions.find((o) => o.id === roleValue) ?? roleOptions[0],
    [roleOptions, roleValue],
  );

  return {
    nameFilter: {
      name: nameValue ?? "",
      setName: setNameValue,
    },
    roleFilter: {
      roles,
      roleOptions,
      selected: selectedRole,
      setSelected: setRoleValue,
    },
  };
}

export type UseUserRoleFilters = ReturnType<typeof useUserRoleFilters>;
export type RoleFilterState = UseUserRoleFilters["roleFilter"];
export type NameFilterState = UseUserRoleFilters["nameFilter"];
