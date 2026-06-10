// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { forwardRef, Link, LinkProps, useTheme } from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";

const TextLink = forwardRef<LinkProps, "a">((props: LinkProps, ref) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Link
      ref={ref}
      color={colors.accent.brightPurple}
      textDecoration="none"
      _hover={{ textDecoration: "underline" }}
      {...props}
    />
  );
});

export default TextLink;
