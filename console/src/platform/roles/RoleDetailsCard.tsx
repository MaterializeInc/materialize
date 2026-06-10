// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Card,
  Flex,
  HStack,
  Spinner,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import Alert from "~/components/Alert";
import { MaterializeTheme } from "~/theme";

import { useRoleDetails } from "./queries";

export interface RoleDetailsCardProps {
  roleName: string;
}

export const RoleDetailsCard = ({ roleName }: RoleDetailsCardProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { data, isLoading, isError } = useRoleDetails({ roleName });

  const roleDetails = data?.rows[0];
  const userCount = roleDetails?.users?.length ?? 0;
  const grantedToRolesCount = roleDetails?.grantedToRoles?.length ?? 0;
  const grantedRolesCount = roleDetails?.grantedRoles?.length ?? 0;

  return (
    <Card
      p={6}
      width="100%"
      borderRadius="md"
      border="1px"
      borderColor={colors.border.primary}
    >
      {isError ? (
        <Alert
          variant="error"
          message="Failed to load role details. Please try refreshing the page."
          width="100%"
        />
      ) : isLoading ? (
        <Flex justify="center" align="center" minHeight="200px">
          <Spinner size="lg" color={colors.accent.brightPurple} />
        </Flex>
      ) : (
        <VStack align="start" spacing={4}>
          <Text textStyle="heading-lg">Role Details</Text>
          <VStack align="start" spacing={3} width="100%">
            <HStack justify="space-between" width="100%">
              <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
                Role name
              </Text>
              <Text textStyle="text-ui-reg">{roleDetails?.name}</Text>
            </HStack>
            <HStack justify="space-between" width="100%">
              <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
                Number of users
              </Text>
              <Text textStyle="text-ui-reg">{userCount}</Text>
            </HStack>
            <HStack justify="space-between" width="100%">
              <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
                Granted to roles
              </Text>
              <Text textStyle="text-ui-reg">{grantedToRolesCount}</Text>
            </HStack>
            <HStack justify="space-between" width="100%">
              <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
                Granted roles
              </Text>
              <Text textStyle="text-ui-reg">{grantedRolesCount}</Text>
            </HStack>
          </VStack>
        </VStack>
      )}
    </Card>
  );
};

export default RoleDetailsCard;
