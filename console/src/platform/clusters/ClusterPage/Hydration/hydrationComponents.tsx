// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Tooltip, useTheme } from "@chakra-ui/react";
import React from "react";

import { InfoIcon } from "~/icons";
import { MaterializeTheme } from "~/theme";

export interface InfoTipProps {
  /** What the tip explains, which names it for screen readers. */
  subject: string;
  label: string;
}

/** An info icon that shows `label` on hover or keyboard focus. */
export const InfoTip = ({ subject, label }: InfoTipProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Tooltip label={label} lineHeight={1.2}>
      <Box
        as="span"
        role="img"
        aria-label={`About ${subject}`}
        tabIndex={0}
        display="inline-flex"
        alignItems="center"
        borderRadius="full"
        _focusVisible={{ boxShadow: "outline" }}
      >
        <InfoIcon color={colors.foreground.secondary} />
      </Box>
    </Tooltip>
  );
};
