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
  HStack,
  Text,
  TextProps,
  useTheme,
} from "@chakra-ui/react";
import React from "react";
import { Link, LinkProps } from "react-router-dom";

import ChevronRightIcon from "~/svg/ChevronRightIcon";
import { MaterializeTheme } from "~/theme";

export type SetNodeExpanded = (
  nodeKey: string,
  set: (previousState: boolean) => boolean,
) => void;

export const Tree = (props: BoxProps) => {
  return <Box as="ul" width="100%" {...props} />;
};

export interface TreeNodeProps<T> {
  childNodeIndent?: string | number;
  children?(data: T[]): React.ReactNode;
  data?: T[];
  href?: string;
  icon?: React.ReactNode;
  iconRight?: React.ReactNode;
  indent?: string | number;
  isExpanded: boolean;
  isSelected: boolean;
  label: string;
  nodeKey: string;
  setNodeExpanded: SetNodeExpanded;
  textProps?: TextProps;
  toggleExpandNode: (nodeKey: string) => void;
}

type MaybeLinkProps = LinkProps | Optional<LinkProps, "to">;

const LinkOrButton = (props: MaybeLinkProps & BoxProps) => {
  return (
    <Flex
      as={props.to ? Link : "button"}
      cursor="pointer"
      gap={2}
      noOfLines={1}
      // noOfLines sets display -webkit-inline-box, override it back to flex
      display="flex"
      {...props}
    />
  );
};

export const TreeNode = <T,>(props: TreeNodeProps<T>) => {
  const { colors } = useTheme<MaterializeTheme>();
  const hasChildren = (props.data?.length ?? 0) > 0;

  return (
    <Box
      as="li"
      className="tree-node"
      display="block"
      position="relative"
      sx={{
        ".tree-node &": {
          marginLeft: props.childNodeIndent ?? "1.5rem",
        },
      }}
    >
      <HStack
        alignItems="flex-start"
        borderRadius="lg"
        background={props.isSelected ? colors.background.accent : undefined}
        spacing="0"
      >
        <Flex
          as="button"
          aria-label={`${props.isExpanded ? "Collapse" : "Expand"} ${props.label}`}
          cursor={hasChildren ? "pointer" : "auto"}
          flexShrink="0"
          w="8"
          h="8"
          p="2"
          onClick={() => {
            props.toggleExpandNode(props.nodeKey);
          }}
        >
          {hasChildren && (
            <ChevronRightIcon
              aria-hidden
              flexShrink="0"
              className="node-icon"
              transform={props.isExpanded ? "rotate(90deg)" : undefined}
            />
          )}
        </Flex>
        <LinkOrButton
          cursor={hasChildren || props.href ? "pointer" : "auto"}
          p="2"
          pl="0"
          width="100%"
          to={props.href}
          onClick={() => {
            // Clicking an expanded node should select it, but not collapse it
            if (!props.href || props.isSelected) {
              // If the node has no href, it's not selectable, so we can always toggle
              props.toggleExpandNode(props.nodeKey);
            } else {
              props.setNodeExpanded(props.nodeKey, () => true);
            }
          }}
        >
          {props.icon}
          <Text
            textStyle="text-small-heavy"
            noOfLines={1}
            {...props.textProps}
            title={props.label}
          >
            {props.label}
          </Text>
          {props.iconRight}
        </LinkOrButton>
      </HStack>
      {props.data &&
        props.children &&
        props.isExpanded &&
        props.children(props.data)}
    </Box>
  );
};
