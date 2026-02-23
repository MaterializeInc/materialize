// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  chakra,
  Checkbox,
  HStack,
  IconButton,
  Menu,
  MenuButton,
  MenuList,
  Portal,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import SearchInput from "~/components/SearchInput";
import { FilterIcon } from "~/svg/FilterIcon";
import { MaterializeTheme } from "~/theme";
import { kebabToTitleCase } from "~/util";

import { SUPPORTED_OBJECT_TYPES } from "./ObjectExplorerNode";
import { ObjectTypeFilterUpdateFn } from "./objectExplorerState";

export interface FiltersProps {
  nameFilter: string;
  objectTypeFilter: string[] | undefined;
  setNameFilter: (value: string) => void;
  setObjectTypeFilter: (updater: ObjectTypeFilterUpdateFn) => void;
}

export const Filters = (props: FiltersProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <HStack gap="2">
      <SearchInput
        minWidth="100%"
        name="nameFilter"
        value={props.nameFilter}
        onChange={(e) => {
          props.setNameFilter(e.target.value);
        }}
        width="256px"
      />
      <Menu gutter={2} placement="bottom-start">
        <MenuButton
          aria-label="Filter"
          as={IconButton}
          icon={
            <FilterIcon
              color={
                props.objectTypeFilter
                  ? colors.accent.brightPurple
                  : colors.foreground.secondary
              }
            />
          }
          size="sm"
          variant="outline"
        />
        <Portal>
          <MenuList p="2">
            <VStack px="2" align="left" spacing="1">
              <Text textStyle="text-small-heavy" py="2">
                Filter by object type
              </Text>
              {SUPPORTED_OBJECT_TYPES.map((type) => (
                <HStack key={type}>
                  <Checkbox
                    id={type}
                    isChecked={props.objectTypeFilter?.includes(type) ?? false}
                    onChange={(e) => {
                      if (e.currentTarget.checked) {
                        props.setObjectTypeFilter((prev) =>
                          prev ? [type, ...prev] : [type],
                        );
                      } else {
                        props.setObjectTypeFilter((prev) => {
                          if (!prev) return undefined;
                          const newFilters = prev.filter((v) => v !== type);
                          // if the set is empty, we return undefined to signify no
                          // selection, rather than the empty array, which means nothing
                          // selected.
                          return newFilters.length === 0
                            ? undefined
                            : newFilters;
                        });
                      }
                    }}
                  />
                  <chakra.label htmlFor={type}>
                    {kebabToTitleCase(type)}
                  </chakra.label>
                </HStack>
              ))}
            </VStack>
          </MenuList>
        </Portal>
      </Menu>
    </HStack>
  );
};
