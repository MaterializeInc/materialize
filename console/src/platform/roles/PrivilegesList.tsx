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
import { capitalizeSentence } from "~/util";

import { useRolePrivileges } from "./queries";

export interface PrivilegesListProps {
  roleName: string;
}

export const PrivilegesList = ({ roleName }: PrivilegesListProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { data, isLoading, isError } = useRolePrivileges({ roleName });

  if (isLoading) {
    return <LoadingContainer />;
  }

  if (isError) {
    return (
      <Flex width="100%" alignItems="center" justifyContent="center">
        <Alert
          variant="error"
          message="Failed to load privileges. Please try refreshing the page."
        />
      </Flex>
    );
  }

  const privileges = (data?.rows ?? [])
    .filter((row) => row.grantee !== "PUBLIC")
    .map((row) => ({
      object: row.name,
      type: capitalizeSentence(row.object_type, false),
      privilege: row.privilege_type,
      inheritedFrom: row.grantee === roleName ? "Direct" : row.grantee,
    }));

  if (privileges.length === 0) {
    return (
      <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
        No privileges assigned to this role.
      </Text>
    );
  }

  return (
    <Table variant="standalone" data-testid="privileges-table">
      <Thead>
        <Tr>
          <Th>Object</Th>
          <Th>Type</Th>
          <Th>Privileges</Th>
          <Th>Inherited From</Th>
        </Tr>
      </Thead>
      <Tbody>
        {privileges.map((privilege) => (
          <Tr
            key={`${privilege.object}-${privilege.type}-${privilege.privilege}-${privilege.inheritedFrom}`}
          >
            <Td>
              <Text textStyle="text-ui-med">{privilege.object}</Text>
            </Td>
            <Td>
              <Text textStyle="text-ui-reg">{privilege.type}</Text>
            </Td>
            <Td>
              <Text textStyle="text-ui-reg">{privilege.privilege}</Text>
            </Td>
            <Td>
              <Text textStyle="text-ui-reg">{privilege.inheritedFrom}</Text>
            </Td>
          </Tr>
        ))}
      </Tbody>
    </Table>
  );
};

export default PrivilegesList;
