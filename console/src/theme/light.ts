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
 * Light theme colors.
 *
 */

import { BasePalette, ComponentOverrides, ThemeColors, ThemeShadows } from ".";
import colors from "./colors";

const base: BasePalette = {
  accent: {
    purple: colors.purple[600],
    brightPurple: colors.purple[500],
    darkGreen: colors.green[450],
    indigo: colors.indigo[50],
    green: colors.green[500],
    orange: colors.orange[350],
    red: colors.red[500],
    darkYellow: colors.yellow[500],
    blue: colors.blue[500],
  },
  foreground: {
    primary: colors.gray[900],
    secondary: colors.gray[500],
    tertiary: "#949197",
    inverse: colors.gray[50],
    primaryButtonLabel: colors.white,
  },
  background: {
    // robinclowers: I added this based on Parker's design for the new navigation hover
    // style. There may be a better / more idomatic way to fit it into our color system.
    accent: "rgba(90, 52, 203, 0.08)",
    primary: colors.white,
    secondary: colors.gray[50],
    tertiary: colors.gray[100],
    shellTutorial: colors.gray[50],
    error: "#FBECEC",
    info: "#DAEEFD",
    warn: "#FEFABF",
    inverse: colors.gray[700],
  },
  border: {
    primary: colors.gray[200],
    secondary: colors.gray[300],
    error: "#F3C0BF",
    info: "#9AC6F2",
    warn: "#EED879",
  },
  lineGraph: [
    colors.purple[700],
    colors.turquoise[600],
    colors.blue[500],
    colors.yellow[700],
    colors.green[500],
    colors.purple[200],
    colors.turquoise[400],
    colors.blue[200],
    colors.yellow[300],
    colors.green[300],
  ],
};

const components: ComponentOverrides = {
  card: {
    background: base.background.primary,
  },
};

export const lightColors: ThemeColors = {
  ...base,
  components,
};

export const lightShadows: ThemeShadows = {
  levelInset1: `
    0.5px 1px 1px -0.5px rgba(0, 0, 0, 0.08) inset,
    0.5px 1.5px 1.5px -0.75px rgba(0, 0, 0, 0.08) inset
  `,
  level1: `
    0px 2px 2px -1px rgba(0, 0, 0, 0.08),
    0px 0.5px 0.5px -0.25px rgba(0, 0, 0, 0.08),
    0px 0px 0px 1px rgba(0, 0, 0, 0.08)
  `,
  level2: `
    0px 4px 4px -2px rgba(0, 0, 0, 0.06),
    0px 2px 2px -1px rgba(0, 0, 0, 0.06),
    0px 1px 1px 0.5px rgba(0, 0, 0, 0.06),
    0px 0px 0px 1px rgba(0, 0, 0, 0.06)
  `,
  level3: `
    0px 12px 12px -6px rgba(0, 0, 0, 0.06),
    0px 8px 8px -4px rgba(0, 0, 0, 0.06),
    0px 4px 4px -2px rgba(0, 0, 0, 0.06),
    0px 1px 1px 0.5px rgba(0, 0, 0, 0.06),
    0px 0px 0px 1px rgba(0, 0, 0, 0.06)
  `,
  level4: `
    0px 1px 1px 0.5px rgba(0, 0, 0, 0.08),
    0px 4px 4px -2px rgba(0, 0, 0, 0.06),
    0px 8px 8px -4px rgba(0, 0, 0, 0.06),
    0px 16px 16px -8px rgba(0, 0, 0, 0.06),
    0px 40px 40px -20px rgba(0, 0, 0, 0.06),
    0px 64px 64px -32px rgba(0, 0, 0, 0.06)
  `,
  input: {
    error: "0px 0px 0px 2px hsla(343, 95%, 46%, 0.24)",
    focus: "0px 0px 0px 2px hsla(257, 100%, 65%, 0.24)",
  },
};
