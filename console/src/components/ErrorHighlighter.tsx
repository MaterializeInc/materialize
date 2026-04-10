// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Mark, useTheme } from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";

import { ErrorHighlightSegments } from "./errorHighlighting";

interface ErrorHighlighterProps {
  text: string;
  segments: ErrorHighlightSegments;
  lineNumber: number;
}

export const ErrorHighlighter = ({
  text,
  segments,
  lineNumber,
}: ErrorHighlighterProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const linePrefix = `LINE ${lineNumber}: `;

  return (
    <>
      {linePrefix}
      {segments.preHighlight}
      <Mark
        bg={colors.background.error}
        color={colors.accent.red}
        fontWeight="semibold"
        textDecoration="underline"
        px="1px"
      >
        {segments.highlightChar}
      </Mark>
      {segments.postHighlight}
    </>
  );
};
