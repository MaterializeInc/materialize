// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Button,
  Menu,
  MenuButton,
  MenuList,
  MenuProps,
  Portal,
  useTheme,
} from "@chakra-ui/react";
import React from "react";

import OverflowMenuIcon from "~/svg/OverflowMenuIcon";
import { MaterializeTheme } from "~/theme";

export const OVERFLOW_BUTTON_WIDTH = 8;

export interface OverflowMenuProps extends Omit<MenuProps, "children"> {
  items?: Array<{
    visible: boolean | null | undefined;
    render: () => React.ReactNode;
  }>;
}
const OverflowMenu = (props: OverflowMenuProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { items, ...menuProps } = props;
  const visibleItems = items && items.filter((i) => i.visible);
  if (visibleItems && visibleItems.length === 0) {
    return null;
  }
  const children = visibleItems?.map((i, index) => (
    <React.Fragment key={index}>{i.render()}</React.Fragment>
  ));
  return (
    <Menu gutter={2} placement="bottom-start" {...menuProps}>
      <MenuButton
        aria-label="More actions"
        variant="none"
        as={Button}
        width={OVERFLOW_BUTTON_WIDTH}
        height={OVERFLOW_BUTTON_WIDTH}
        p="0"
        minWidth="auto"
        onClick={(e) => e.stopPropagation()}
        _hover={{
          background: colors.background.tertiary,
        }}
      >
        <OverflowMenuIcon />
      </MenuButton>
      {/* The portal prevents hover state from bubbling up to the parent table row */}
      <Portal>
        <MenuList>{children}</MenuList>
      </Portal>
    </Menu>
  );
};

export default OverflowMenu;
