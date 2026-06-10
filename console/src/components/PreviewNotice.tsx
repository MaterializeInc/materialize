// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { BoxProps, Text, Tooltip, useTheme } from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";

const PreviewNotice = (props: BoxProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Tooltip label="This is a preview feature and may undergo changes.">
      <Text
        textStyle="text-small"
        fontWeight="500"
        py={1}
        px={3}
        ml={2}
        borderRadius="full"
        border="1px solid"
        borderColor={colors.border.info}
        background={colors.background.info}
        {...props}
      >
        Preview
      </Text>
    </Tooltip>
  );
};

export default PreviewNotice;
