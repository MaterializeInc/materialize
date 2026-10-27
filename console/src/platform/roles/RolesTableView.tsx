// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Spinner } from "@chakra-ui/react";
import React, { useMemo } from "react";
import { useNavigate } from "react-router-dom";

import SearchInput from "~/components/SearchInput";
import { MainContentContainer } from "~/layouts/BaseLayout";
import {
  RolesEmptyFilteredState,
  RolesEmptyState,
  RolesList,
} from "~/platform/roles/RolesList";
import { useRoleFilters } from "~/platform/roles/useRoleFilters";
import { useAllRoles } from "~/store/allRoles";

import { RolesViewHeader } from "./RolesViewHeader";

export const RolesTableView = () => {
  const { data: roles, isLoading } = useAllRoles();
  const roleFilter = useRoleFilters();
  const navigate = useNavigate();

  const handleCreateOpen = () => {
    navigate("new");
  };

  const filteredRoles = useMemo(() => {
    if (!roles) return [];
    if (!roleFilter.name) return roles;
    return roles.filter((role) =>
      role.roleName.toLowerCase().includes(roleFilter.name.toLowerCase()),
    );
  }, [roles, roleFilter.name]);

  if (isLoading) {
    return (
      <MainContentContainer>
        <Spinner data-testid="loading-spinner" />
      </MainContentContainer>
    );
  }

  return (
    <MainContentContainer>
      {!roles || roles.length === 0 ? (
        <RolesEmptyState onCreateRole={handleCreateOpen} />
      ) : (
        <>
          <RolesViewHeader
            onCreateRole={handleCreateOpen}
            searchInput={
              <SearchInput
                name="role"
                value={roleFilter.name}
                onChange={(e) => {
                  roleFilter.setName(e.target.value);
                }}
              />
            }
          />
          {filteredRoles.length === 0 ? (
            <RolesEmptyFilteredState
              clearFilters={() => roleFilter.setName("")}
            />
          ) : (
            <RolesList roles={filteredRoles} />
          )}
        </>
      )}
    </MainContentContainer>
  );
};

export default RolesTableView;
