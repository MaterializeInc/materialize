// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * @module
 * A reusable "card" component.
 */

import { BoxProps, HeadingProps } from "@chakra-ui/layout";
import { Box, Heading, HStack, StackProps, useTheme } from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";

export const CARD_PADDING = 4;

/**
 * A "card" UI element with a header, body.
 React.PropsWithChildren
 * Example usage:
 *
 * ```
 * <Card>
 *   <CardHeader>Card title</CardHeader>
 *   <CardContent>
 *     Some content
 *   </CardContent>
 * </Card>
 * ```
 */
export const Card = React.forwardRef(
  (props: BoxProps, ref: React.LegacyRef<HTMLDivElement> | undefined) => {
    const { colors, shadows } = useTheme<MaterializeTheme>();

    return (
      <Box
        ref={ref}
        bg={colors.background.primary}
        shadow={shadows.level2}
        border="1px solid"
        borderColor={colors.border.primary}
        width="100%"
        borderRadius="xl"
        {...props}
      >
        {props.children}
      </Box>
    );
  },
);

export interface CardTitleProps extends HeadingProps {
  children: React.ReactNode;
}

/* A title for a `Card`. Used standalone in `CardHeader.` */
export const CardTitle = (props: CardTitleProps) => {
  return <Heading fontSize="lg" fontWeight="600" p={CARD_PADDING} {...props} />;
};

/** A header for a `Card`. */
export const CardHeader = (props: CardTitleProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <CardTitle
      borderBottomWidth="1px"
      borderBottomColor={colors.border.primary}
      {...props}
    />
  );
};

/** The container of the body content for a `Card`. */
export const CardContent = ({ children, ...props }: BoxProps) => {
  return (
    <Box p={CARD_PADDING} {...props}>
      {children}
    </Box>
  );
};

export const CardFooter = ({ children, ...props }: StackProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <HStack
      background={colors.background.secondary}
      borderTopWidth="1px"
      borderColor={colors.border.primary}
      justifyContent="space-between"
      width="100%"
      p="4"
      {...props}
    >
      {children}
    </HStack>
  );
};
