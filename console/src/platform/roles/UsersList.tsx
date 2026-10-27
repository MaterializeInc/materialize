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
  IconButton,
  Menu,
  MenuButton,
  MenuItem,
  MenuList,
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  useDisclosure,
  useTheme,
} from "@chakra-ui/react";
import React, { useState } from "react";

import Alert from "~/components/Alert";
import { LoadingContainer } from "~/components/LoadingContainer";
import OverflowMenuIcon from "~/svg/OverflowMenuIcon";
import { MaterializeTheme } from "~/theme";

import { useRoleMembers } from "./queries";
import RemoveUserModal from "./RemoveUserModal";

export interface UsersListProps {
  roleName: string;
}

export const UsersList = ({ roleName }: UsersListProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { data, isLoading, isError } = useRoleMembers({
    roleName,
    memberType: "users",
  });
  const { isOpen, onOpen, onClose } = useDisclosure();
  const [selectedMember, setSelectedMember] = useState<string | null>(null);

  const handleRemoveClick = (memberName: string) => {
    setSelectedMember(memberName);
    onOpen();
  };

  const handleModalClose = () => {
    onClose();
    setSelectedMember(null);
  };

  if (isLoading) {
    return <LoadingContainer />;
  }

  if (isError) {
    return (
      <Flex width="100%" alignItems="center" justifyContent="center">
        <Alert
          variant="error"
          message="Failed to load users. Please try refreshing the page."
        />
      </Flex>
    );
  }

  const users = data?.rows ?? [];

  if (users.length === 0) {
    return (
      <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
        No users assigned to this role.
      </Text>
    );
  }

  return (
    <>
      <Table variant="standalone" data-testid="users-table">
        <Thead>
          <Tr>
            <Th>User</Th>
            <Th width="50px"></Th>
          </Tr>
        </Thead>
        <Tbody>
          {users.map((user) => (
            <Tr key={user.id}>
              <Td>
                <Text textStyle="text-ui-med">{user.memberName}</Text>
              </Td>
              {/* Using direct Chakra Menu instead of OverflowMenu because
              OverflowMenu's Portal unmounts on menu close, preventing the
              modal from rendering inside a SideDrawer. */}
              <Td>
                <Menu>
                  <MenuButton
                    as={IconButton}
                    aria-label="More actions"
                    icon={<OverflowMenuIcon />}
                    variant="ghost"
                    size="sm"
                  />
                  <MenuList>
                    <MenuItem
                      onClick={() => handleRemoveClick(user.memberName)}
                      color={colors.accent.red}
                    >
                      Remove user
                    </MenuItem>
                  </MenuList>
                </Menu>
              </Td>
            </Tr>
          ))}
        </Tbody>
      </Table>
      {selectedMember && (
        <RemoveUserModal
          isOpen={isOpen}
          onClose={handleModalClose}
          roleName={roleName}
          memberName={selectedMember}
        />
      )}
    </>
  );
};

export default UsersList;
