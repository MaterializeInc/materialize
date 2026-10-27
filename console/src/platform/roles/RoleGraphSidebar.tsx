// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Box,
  chakra,
  Flex,
  HStack,
  List,
  ListItem,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useCombobox } from "downshift";
import React from "react";

import { RoleItem } from "~/api/materialize/roles/rolesList";
import SearchInput from "~/components/SearchInput";
import {
  SidebarHeaderContainer,
  SidebarItem,
  SidebarItemValue,
  SidebarSection,
} from "~/components/WorkflowGraph/Sidebar";
import { AdminShieldIcon } from "~/svg/AdminIcon";
import { MaterializeTheme } from "~/theme";

export const ROLE_SIDEBAR_WIDTH = 300;

export interface RoleGraphSidebarProps {
  selectedRole: RoleItem | undefined;
  inheritsFrom: RoleItem[];
  grantedTo: RoleItem[];
  onRoleClick: (roleName: string) => void;
  filterValue: string;
  onFilterChange: (value: string) => void;
  filteredRoles: RoleItem[];
  onRoleSelect: (roleName: string) => void;
}

const RoleGraphSidebar = ({
  selectedRole,
  inheritsFrom,
  grantedTo,
  onRoleClick,
  filterValue,
  onFilterChange,
  filteredRoles,
  onRoleSelect,
}: RoleGraphSidebarProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  const {
    isOpen,
    getMenuProps,
    getInputProps,
    getItemProps,
    highlightedIndex,
  } = useCombobox({
    items: filteredRoles,
    inputValue: filterValue,
    itemToString: (item) => item?.roleName ?? "",
    onInputValueChange: ({ inputValue }) => {
      onFilterChange(inputValue ?? "");
    },
    onSelectedItemChange: ({ selectedItem }) => {
      if (selectedItem) {
        onRoleSelect(selectedItem.roleName);
      }
    },
    stateReducer: (_state, actionAndChanges) => {
      const { changes, type } = actionAndChanges;
      switch (type) {
        case useCombobox.stateChangeTypes.InputKeyDownEnter:
        case useCombobox.stateChangeTypes.ItemClick:
          return {
            ...changes,
            inputValue: "",
          };
        default:
          return changes;
      }
    },
  });

  const showDropdown = isOpen && filterValue && filteredRoles.length > 0;

  return (
    <Flex
      width={ROLE_SIDEBAR_WIDTH}
      height="100%"
      position="absolute"
      top="0"
      right="0"
      backgroundColor={colors.background.primary}
      borderColor={colors.border.secondary}
      borderLeftWidth="1px"
      direction="column"
      overflow="auto"
    >
      <Box
        px="4"
        py="3"
        borderBottom="1px solid"
        borderColor={colors.border.primary}
      >
        <Box position="relative">
          <SearchInput
            {...getInputProps()}
            placeholder="Filter roles..."
            minWidth="unset"
            width="100%"
          />
          <List
            {...getMenuProps()}
            position="absolute"
            top="100%"
            left="0"
            right="0"
            mt="1"
            bg={colors.background.primary}
            border={showDropdown ? "1px solid" : "none"}
            borderColor={colors.border.secondary}
            borderRadius="md"
            boxShadow={showDropdown ? "md" : "none"}
            maxHeight="200px"
            overflowY="auto"
            zIndex={10}
            py={showDropdown ? "1" : "0"}
          >
            {showDropdown &&
              filteredRoles.map((role, index) => (
                <ListItem
                  key={role.roleName}
                  {...getItemProps({ item: role, index })}
                  px="3"
                  py="2"
                  cursor="pointer"
                  bg={
                    highlightedIndex === index
                      ? colors.background.secondary
                      : "transparent"
                  }
                  _hover={{ bg: colors.background.secondary }}
                >
                  <RoleLabel roleName={role.roleName} />
                </ListItem>
              ))}
          </List>
        </Box>
      </Box>

      {selectedRole && (
        <>
          <SidebarHeaderContainer>
            <Text
              textStyle="text-ui-med"
              noOfLines={1}
              title={selectedRole.roleName}
            >
              {selectedRole.roleName}
            </Text>
          </SidebarHeaderContainer>

          <SidebarSection title="Inherits from">
            <VStack spacing="2" align="stretch">
              {inheritsFrom.map((role) => (
                <RelatedRole
                  key={role.roleName}
                  role={role}
                  onRoleClick={onRoleClick}
                />
              ))}
              {inheritsFrom.length === 0 && (
                <SidebarItem>
                  <SidebarItemValue color={colors.foreground.tertiary}>
                    No inherited roles
                  </SidebarItemValue>
                </SidebarItem>
              )}
            </VStack>
          </SidebarSection>

          <SidebarSection title="Granted to">
            <VStack spacing="2" align="stretch">
              {grantedTo.map((role) => (
                <RelatedRole
                  key={role.roleName}
                  role={role}
                  onRoleClick={onRoleClick}
                />
              ))}
              {grantedTo.length === 0 && (
                <SidebarItem>
                  <SidebarItemValue color={colors.foreground.tertiary}>
                    No roles inherit this role
                  </SidebarItemValue>
                </SidebarItem>
              )}
            </VStack>
          </SidebarSection>
        </>
      )}

      {!selectedRole && (
        <Flex px="4" py="6" justify="center">
          <Text textStyle="text-small" color={colors.foreground.tertiary}>
            Select a role to view details
          </Text>
        </Flex>
      )}
    </Flex>
  );
};

interface RoleLabelProps {
  roleName: string;
}

const RoleLabel = ({ roleName }: RoleLabelProps) => {
  return (
    <HStack>
      <AdminShieldIcon width="16px" height="16px" />
      <Text textStyle="text-small" noOfLines={1} title={roleName}>
        {roleName}
      </Text>
    </HStack>
  );
};

interface RelatedRoleProps {
  role: RoleItem;
  onRoleClick: (roleName: string) => void;
}

const RelatedRole = ({ role, onRoleClick }: RelatedRoleProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <chakra.button onClick={() => onRoleClick(role.roleName)} width="100%">
      <SidebarItem _hover={{ backgroundColor: colors.background.secondary }}>
        <RoleLabel roleName={role.roleName} />
      </SidebarItem>
    </chakra.button>
  );
};

export default RoleGraphSidebar;
