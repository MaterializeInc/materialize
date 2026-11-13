// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { forwardRef, Icon, IconProps, useTheme } from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";

/** Three linked nodes, for the ontology explorer. */
export const OntologyIcon = forwardRef<IconProps, "svg">((props, ref) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Icon
      height="4"
      width="4"
      viewBox="0 0 16 16"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      ref={ref}
      color={colors.foreground.tertiary}
      {...props}
    >
      <circle
        cx="3.5"
        cy="4"
        r="2"
        stroke="currentColor"
        strokeWidth="1.33333"
      />
      <circle
        cx="12.5"
        cy="4"
        r="2"
        stroke="currentColor"
        strokeWidth="1.33333"
      />
      <circle
        cx="8"
        cy="12.5"
        r="2"
        stroke="currentColor"
        strokeWidth="1.33333"
      />
      <path
        d="M5.5 4H10.5M4.6 5.8L6.9 10.7M11.4 5.8L9.1 10.7"
        stroke="currentColor"
        strokeWidth="1.33333"
        strokeLinecap="round"
      />
    </Icon>
  );
});
