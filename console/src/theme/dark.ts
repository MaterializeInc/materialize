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
 * Dark theme colors.
 *
 */

import { BasePalette, ComponentOverrides, ThemeColors, ThemeShadows } from ".";
import colors from "./colors";

const base: BasePalette = {
  accent: {
    purple: colors.purple[400],
    brightPurple: colors.purple[300],
    green: colors.green[400],
    darkGreen: colors.green[450],
    indigo: colors.indigo[50],
    orange: colors.orange[350],
    red: colors.red[400],
    darkYellow: colors.yellow[500],
    blue: colors.blue[400],
  },
  foreground: {
    primary: colors.gray[50],
    secondary: colors.gray[400],
    tertiary: "#807B84",
    inverse: colors.gray[900],
    primaryButtonLabel: colors.white,
  },
  background: {
    // robinclowers: I added this based on Parker's design for the new navigation hover
    // style. There may be a better / more idomatic way to fit it into our color system.
    accent: "rgba(181, 154, 255, 0.08)",
    primary: colors.gray[900],
    secondary: colors.gray[800],
    tertiary: colors.gray[700],
    shellTutorial: "#18181B",
    error: "#562525",
    info: "#2F324C",
    warn: "#354E04",
    inverse: colors.gray[200],
  },
  border: {
    primary: colors.gray[700],
    secondary: colors.gray[600],
    error: "#813737",
    info: "#4E547E",
    warn: "#82840A",
  },
  lineGraph: [
    colors.purple[200],
    colors.turquoise[400],
    colors.blue[200],
    colors.yellow[300],
    colors.green[300],
    colors.purple[700],
    colors.turquoise[600],
    colors.blue[500],
    colors.yellow[700],
    colors.green[500],
  ],
};

const components: ComponentOverrides = {
  card: {
    background: base.background.secondary,
  },
};

export const darkColors: ThemeColors = {
  ...base,
  components,
};

export const darkShadows: ThemeShadows = {
  levelInset1: `
    0.5px 1px 1px -0.5px rgba(255, 255, 255, 0.08) inset,
    0.5px 1.5px 1.5px -0.75px rgba(255, 255, 255, 0.08) inset
  `,
  level1: `
    0px 2px 2px -1px rgba(0, 0, 0, 0.24),
    0px 0.5px 0.5px -0.25px rgba(0, 0, 0, 0.24),
    0px 0px 0px 1px rgba(0, 0, 0, 0.24),
    0px 0px 0px 1px rgba(255, 255, 255, 0.08) inset,
    0px 1px 0px 0px rgba(255, 255, 255, 0.08) inset`,
  level2: `
    0px 4px 4px -2px rgba(0, 0, 0, 0.24),
    0px 2px 2px -1px rgba(0, 0, 0, 0.24),
    0px 1px 1px -0.5px rgba(0, 0, 0, 0.24),
    0px 0px 0px 1px rgba(0, 0, 0, 0.24),
    0px 0px 0px 1px rgba(255, 255, 255, 0.08) inset,
    0px 1px 0px 0px rgba(255, 255, 255, 0.08) inset`,
  level3: `
    0px 12px 12px -6px rgba(0, 0, 0, 0.24),
    0px 8px 8px -4px rgba(0, 0, 0, 0.24),
    0px 4px 4px -2px rgba(0, 0, 0, 0.24),
    0px 1px 1px -0.5px rgba(0, 0, 0, 0.24),
    0px 0px 0px 1px rgba(0, 0, 0, 0.24),
    0px 1px 0px 0px rgba(255, 255, 255, 0.08) inset,
    0px 0px 0px 1px rgba(255, 255, 255, 0.08) inset`,
  level4: `
    0px 1px 1px 0.5px rgba(0, 0, 0, 0.24),
    0px 4px 4px -2px rgba(0, 0, 0, 0.24),
    0px 8px 8px -4px rgba(0, 0, 0, 0.24),
    0px 16px 16px -8px rgba(0, 0, 0, 0.24),
    0px 40px 40px -20px rgba(0, 0, 0, 0.24),
    0px 64px 64px -32px rgba(0, 0, 0, 0.24),
    0px 0px 0px 1px rgba(255, 255, 255, 0.08) inset,
    0px 1px 0px 0px rgba(255, 255, 255, 0.08) inset`,
  input: {
    error: "0px 0px 0px 2px hsla(343, 95%, 46%, 0.24)",
    focus: "0px 0px 0px 2px rgba(181, 154, 255, 0.40)",
  },
};
