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

const withOpacity = (value: string, opacity: number) => {
  const parsed = color(value);
  if (!parsed) return value;
  parsed.opacity = opacity;
  return parsed.formatRgb();
};

/**
 * The worker bars' colors: a picked object's part of each bar, the rest of
 * each worker's time, and both again for an overloaded worker. Each pair
 * differs in lightness, so it reads without telling hues apart.
 */
export function useWorkerBarColors() {
  const { colors } = useTheme<MaterializeTheme>();
  return React.useMemo(
    () => ({
      picked: colors.accent.brightPurple,
      rest: withOpacity(colors.accent.brightPurple, 0.25),
      hotPicked: colors.accent.red,
      hotRest: withOpacity(colors.accent.red, 0.35),
    }),
    [colors],
  );
}

/** The shade between two lines, such as replica CPU and the workers' time. */
export const gapFill = (base: string) => withOpacity(base, 0.16);
