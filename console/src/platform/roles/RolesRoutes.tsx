// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, Text, useTheme, VStack } from "@chakra-ui/react";
import React from "react";
import { Navigate, Outlet, Route } from "react-router-dom";

import { PageHeader, PageTabStrip, Tab } from "~/layouts/BaseLayout";
import { SentryRoutes } from "~/sentry";
import { useSubscribeToAllRoles } from "~/store/allRoles";
import { MaterializeTheme } from "~/theme";

import { CreateRoleModal } from "./create";
import { EditRoleModal } from "./edit/EditRoleModal";
import { RolesGraphView } from "./RolesGraphView";
import { RolesTableView } from "./RolesTableView";
import { UsersListPage } from "./UsersListPage";

const NAV_ITEMS: Tab[] = [
  { label: "Roles", href: "/roles", end: true },
  { label: "Users", href: "/roles/users", end: true },
];

const RolesLayout = () => {
  const { colors } = useTheme<MaterializeTheme>();
  useSubscribeToAllRoles();

  return (
    <>
      <PageHeader variant="compact" sticky>
        <VStack spacing={0} alignItems="flex-start" width="100%">
          <HStack
            borderBottom="solid 1px"
            borderColor={colors.border.primary}
            justifyContent="space-between"
            width="100%"
            py="4"
            px="7"
          >
            <Text textStyle="heading-md">Roles And Users</Text>
          </HStack>

          <PageTabStrip tabData={NAV_ITEMS} />
        </VStack>
      </PageHeader>
      <Outlet />
    </>
  );
};

const RolesRoutes = () => {
  return (
    <SentryRoutes>
      <Route element={<RolesLayout />}>
        <Route index element={<RolesTableView />} />
        <Route path="new" element={<CreateRoleModal />} />
        <Route path="edit/:roleName" element={<EditRoleModal />} />
        <Route path="graph" element={<RolesGraphView />} />
        <Route path="users" element={<UsersListPage />} />
        <Route path="*" element={<Navigate to="." replace />} />
      </Route>
    </SentryRoutes>
  );
};

export default RolesRoutes;
