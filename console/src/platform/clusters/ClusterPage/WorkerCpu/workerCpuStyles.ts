// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useTheme } from "@chakra-ui/react";
import { color } from "d3";
import React from "react";

import { MaterializeTheme } from "~/theme";

import { EVEN_RATIO, SKEW_RATIO } from "./workerCpuModel";

const withOpacity = (value: string, opacity: number) => {
  const parsed = color(value);
  if (!parsed) return value;
  parsed.opacity = opacity;
  return parsed.formatRgb();
};

/** Worker bars stop at this many times an even share. */
export const BAR_CEILING_RATIO = 3;

/**
 * A worker bar's color by its time on an object against an even share:
 * neutral up to about even, orange once uneven, red at the bars' ceiling.
 */
export function useWorkerBarColor() {
  const { colors } = useTheme<MaterializeTheme>();
  return React.useMemo(() => {
    const uneven = withOpacity(colors.accent.orange, 0.5);
    return (ratio: number) =>
      ratio >= BAR_CEILING_RATIO
        ? colors.accent.red
        : ratio >= SKEW_RATIO
          ? colors.accent.orange
          : ratio >= EVEN_RATIO
            ? uneven
            : colors.foreground.tertiary;
  }, [colors]);
}

/** The shade between two lines, such as the busiest and average worker. */
export const gapFill = (base: string) => withOpacity(base, 0.16);
