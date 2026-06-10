// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Button, ButtonProps, useTheme, VStack } from "@chakra-ui/react";
import React from "react";

import LeftArrowIcon from "~/svg/LeftArrowIcon";
import { MaterializeTheme } from "~/theme";

export const SortFilterMenuBottom = ({
  ascendingButtonProps,
  descendingButtonProps,
}: {
  ascendingButtonProps?: ButtonProps;
  descendingButtonProps?: ButtonProps;
}) => {
  const { colors } = useTheme<MaterializeTheme>();

  const commonButtonProps = {
    variant: "borderless",
    width: "100%",
    size: "sm",
    py: "2",
    px: "4",
    borderRadius: "0",
    justifyContent: "flex-start",
    color: colors.foreground.primary,
  };

  return (
    <VStack
      width="100%"
      alignItems="flex-start"
      borderTopWidth="1px"
      spacing="0"
      borderColor={colors.border.secondary}
      py="2"
    >
      <Button
        {...commonButtonProps}
        leftIcon={
          <LeftArrowIcon transform="rotate(90deg)" color="currentcolor" />
        }
        {...ascendingButtonProps}
      >
        Ascending (A &rarr; Z)
      </Button>
      <Button
        {...commonButtonProps}
        leftIcon={
          <LeftArrowIcon transform="rotate(-90deg)" color="currentcolor" />
        }
        {...descendingButtonProps}
      >
        Descending (Z &rarr; A)
      </Button>
    </VStack>
  );
};

export default SortFilterMenuBottom;
