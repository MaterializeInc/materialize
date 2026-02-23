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
  BoxProps,
  Flex,
  forwardRef,
  HStack,
  Image,
  StackProps,
  Text,
  useTheme,
} from "@chakra-ui/react";
import React from "react";
import { DropdownIndicatorProps, GroupBase, OptionProps } from "react-select";

import plus from "~/img/plus.svg";
import CheckmarkIcon from "~/svg/CheckmarkIcon";
import ChevronDownIcon from "~/svg/ChevronDownIcon";
import { MaterializeTheme } from "~/theme";

export const DropdownIndicator = <
  Option,
  IsMulti extends boolean,
  Group extends GroupBase<Option> = GroupBase<Option>,
>(
  _props: React.PropsWithChildren<
    DropdownIndicatorProps<Option, IsMulti, Group>
  >,
) => {
  return <ChevronDownIcon mr="8px" />;
};

export const OptionBase = forwardRef(
  (
    {
      isHighlighted,
      isSelected,
      children,
      containerProps,
    }: {
      isHighlighted?: boolean;
      isSelected?: boolean;
      children?: React.ReactNode;
      containerProps?: BoxProps;
    },
    ref,
  ) => {
    const { colors } = useTheme<MaterializeTheme>();
    return (
      <Box
        ref={ref}
        backgroundColor={
          isHighlighted ? colors.background.secondary : undefined
        }
        py="1"
        pr="4"
        width="100%"
        cursor="pointer"
        {...containerProps}
      >
        <HStack spacing="0" alignItems="center" justifyContent="start">
          <Flex justifyContent="center" width="40px" flexShrink="0">
            {isSelected && <CheckmarkIcon color={colors.accent.brightPurple} />}
          </Flex>
          <Text
            noOfLines={1}
            textStyle="text-base"
            userSelect="none"
            wordBreak="break-all"
          >
            {children}
          </Text>
        </HStack>
      </Box>
    );
  },
);

export const Option = <Option, IsMulti extends boolean>(
  props: React.PropsWithChildren<
    OptionProps<Option, IsMulti, GroupBase<Option>>
  >,
) => {
  const { isFocused, innerRef, innerProps, isSelected, label } = props;

  return (
    <OptionBase
      isHighlighted={isFocused}
      isSelected={isSelected}
      containerProps={{ title: label, ...innerProps }}
      ref={innerRef}
    >
      {props.children}
    </OptionBase>
  );
};

export const AddNewItemMenuOption = ({
  addNewItemLabel,
  onAddNewItem,
  closeMenu,
  containerProps,
}: {
  addNewItemLabel?: string;
  onAddNewItem: () => void;
  closeMenu: () => void;
  containerProps?: StackProps;
}) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Flex
      background={colors.background.secondary}
      borderColor={colors.border.secondary}
      borderTopWidth="1px"
      color={colors.accent.brightPurple}
      cursor="pointer"
      p="3"
      textStyle="text-ui-reg"
      onClick={() => {
        onAddNewItem();
        closeMenu();
      }}
      {...containerProps}
    >
      <Image alt="Plus icon" src={plus} mr="2" />
      {addNewItemLabel ?? "Add New Item"}
    </Flex>
  );
};
