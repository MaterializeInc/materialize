// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Tab, TabList, TabPanel, TabPanels, Tabs } from "@chakra-ui/react";
import React from "react";

import { GrantedRolesList } from "./GrantedRolesList";
import { GrantedToRolesList } from "./GrantedToRolesList";
import { PrivilegesList } from "./PrivilegesList";
import { RoleDetailsCard } from "./RoleDetailsCard";
import { UsersList } from "./UsersList";

export interface RoleDetailsProps {
  roleName: string;
}

export const RoleDetails = ({ roleName }: RoleDetailsProps) => {
  return (
    <Box p={4}>
      <RoleDetailsCard roleName={roleName} />
      <Tabs mt={6}>
        <TabList mb={6}>
          <Tab>Privileges</Tab>
          <Tab>Users</Tab>
          <Tab>Granted to roles</Tab>
          <Tab>Granted roles</Tab>
        </TabList>
        <TabPanels>
          <TabPanel>
            <PrivilegesList roleName={roleName} />
          </TabPanel>
          <TabPanel>
            <UsersList roleName={roleName} />
          </TabPanel>
          <TabPanel>
            <GrantedToRolesList roleName={roleName} />
          </TabPanel>
          <TabPanel>
            <GrantedRolesList roleName={roleName} />
          </TabPanel>
        </TabPanels>
      </Tabs>
    </Box>
  );
};

export default RoleDetails;
