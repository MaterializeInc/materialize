// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useTheme } from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";

export const IFrameWrapper = ({
  ...rest
}: React.DetailedHTMLProps<
  React.IframeHTMLAttributes<HTMLIFrameElement>,
  HTMLIFrameElement
>) => {
  const { colors, shadows, radii } = useTheme<MaterializeTheme>();
  return (
    <iframe
      style={{
        border: `1px solid ${colors.border.secondary}`,
        borderRadius: radii.md,
        boxShadow: shadows.levelInset1,
      }}
      {...rest}
    />
  );
};

export default IFrameWrapper;
