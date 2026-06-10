// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Flex,
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  useTheme,
} from "@chakra-ui/react";
import React from "react";

import Alert from "~/components/Alert";
import { LoadingContainer } from "~/components/LoadingContainer";
import { MaterializeTheme } from "~/theme";

import { useGrantedRoles } from "./queries";

export interface GrantedRolesListProps {
  roleName: string;
}

export const GrantedRolesList = ({ roleName }: GrantedRolesListProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { data, isLoading, isError } = useGrantedRoles({ roleName });

  if (isLoading) {
    return <LoadingContainer />;
  }

  if (isError) {
    return (
      <Flex width="100%" alignItems="center" justifyContent="center">
        <Alert
          variant="error"
          message="Failed to load granted roles. Please try refreshing the page."
        />
      </Flex>
    );
  }

  const roles = data?.rows ?? [];

  if (roles.length === 0) {
    return (
      <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
        This role does not inherit from any other roles.
      </Text>
    );
  }

  return (
    <Table variant="standalone" data-testid="granted-roles-table">
      <Thead>
        <Tr>
          <Th>Role</Th>
        </Tr>
      </Thead>
      <Tbody>
        {roles.map((role) => (
          <Tr key={role.id}>
            <Td>
              <Text textStyle="text-ui-med">{role.roleName}</Text>
            </Td>
          </Tr>
        ))}
      </Tbody>
    </Table>
  );
};

export default GrantedRolesList;
