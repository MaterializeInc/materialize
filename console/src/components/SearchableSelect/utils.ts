// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { GroupBase, mergeStyles, StylesConfig } from "react-select";

import {
  buildReactSelectFilterStyles,
  ThemeColors,
  ThemeShadows,
} from "~/theme";

export const buildSearchableSelectFilterStyles = <
  Option = unknown,
  IsMulti extends boolean = boolean,
  Group extends GroupBase<Option> = GroupBase<Option>,
>(
  {
    colors: themeColors,
    shadows,
    isError,
    containerWidth,
    menuWidth,
    usePortal,
  }: {
    colors: ThemeColors;
    shadows: ThemeShadows;
    isError?: boolean;
    containerWidth?: string;
    menuWidth?: string;
    usePortal?: boolean;
  },
  overrides: StylesConfig<Option, IsMulti, Group> = {},
): StylesConfig<Option, IsMulti, Group> =>
  mergeStyles(
    mergeStyles(
      buildReactSelectFilterStyles({ colors: themeColors, shadows }),
      {
        control: (base) => {
          return {
            ...base,
            outline: "none",
            borderColor: "transparent",
            boxShadow: "none",
          };
        },
        container: (base, state) => ({
          ...base,
          boxSizing: "border-box",
          width: containerWidth ?? base.width,
          borderRadius: "8px",
          borderWidth: "1px",
          transition: "all 0.2s ease-in-out",
          borderColor: isError
            ? themeColors.accent.red
            : state.isFocused
              ? themeColors.accent.brightPurple
              : themeColors.border.secondary,
          boxShadow: isError
            ? shadows.input.error
            : state.isFocused
              ? shadows.input.focus
              : "0px 0px 0.5px rgba(0, 0, 0, 0.16), 0px 0.5px 2px rgba(0, 0, 0, 0.12);",
        }),
        menu: (base) => ({
          ...base,
          minWidth: menuWidth ?? base.minWidth,
          width: menuWidth ?? base.width,
          boxShadow: shadows.level3,
        }),
        menuPortal: (base) => ({
          ...base,
          zIndex: usePortal ? 1500 : base.zIndex,
        }),
      },
    ),
    overrides,
  );
