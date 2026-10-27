// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Spinner } from "@chakra-ui/react";
import React from "react";
import { useNavigate } from "react-router-dom";

import { MainContentContainer } from "~/layouts/BaseLayout";
import RoleGraph from "~/platform/roles/RoleGraph";
import { RolesEmptyState } from "~/platform/roles/RolesList";
import { useAllRoles } from "~/store/allRoles";

import { RolesViewHeader } from "./RolesViewHeader";

export const RolesGraphView = () => {
  const { data: roles, isLoading } = useAllRoles();
  const navigate = useNavigate();

  const handleCreateOpen = () => {
    navigate("/roles/new", { state: { previousPage: "/roles/graph" } });
  };

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
          <RolesViewHeader onCreateRole={handleCreateOpen} />
          <RoleGraph />
        </>
      )}
    </MainContentContainer>
  );
};

export default RolesGraphView;
