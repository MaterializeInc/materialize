// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Button, ButtonProps, Text } from "@chakra-ui/react";
import React from "react";
import { Link, LinkProps } from "react-router-dom";

export type IconNavLinkProps = LinkProps &
  ButtonProps & {
    icon?: React.ReactNode;
    label?: string | React.ReactNode;
  };

const IconNavLink = ({ icon, label, ...props }: IconNavLinkProps) => {
  return (
    <Button
      as={Link}
      variant="outline"
      p="4"
      height="auto"
      justifyContent="left"
      {...props}
    >
      {icon && (
        <Box mr="4" flexShrink="0">
          {icon}
        </Box>
      )}
      {typeof label === "string" ? (
        <Text textStyle="text-ui-med" noOfLines={1}>
          {label}
        </Text>
      ) : (
        label
      )}
    </Button>
  );
};

export default IconNavLink;
