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
 * Theme configuration.
 *
 * NOTE(benesch): this configuration was thrown together quickly to make the few
 * UI elements in use match the "Open Beta" Figma. Over time, we should make
 * the Chakra UI theme the source of truth and ensure it is comprehensive enough
 * to cover the full set of UI elements. Frontegg makes this a bit complicated,
 * because the important colors need to be plumbed through both Chakra and
 * Frontegg.
 */

import {
  ChakraTheme,
  extendTheme,
  Flex,
  SystemStyleInterpolation,
} from "@chakra-ui/react";
import { mode, StyleFunctionProps } from "@chakra-ui/theme-tools";
import React from "react";
import { GroupBase, mergeStyles, StylesConfig } from "react-select";

import { type FronteggThemeOptions } from "~/external-library-wrappers/frontegg";
import materializeLogo from "~/img/materialize-logo-white.svg";
import { SELECT_MENU_Z_INDEX } from "~/layouts/zIndex";
import colors, { gradients } from "~/theme/colors";
import * as components from "~/theme/components";

import { darkColors, darkShadows } from "./dark";
import { lightColors, lightShadows } from "./light";
import type { TextStyles } from "./typography";
import { typographySystem } from "./typography";

export interface BasePalette {
  accent: {
    purple: string;
    brightPurple: string;
    green: string;
    darkGreen: string;
    darkYellow: string;
    indigo: string;
    orange: string;
    red: string;
    blue: string;
  };
  foreground: {
    primary: string;
    secondary: string;
    tertiary: string;
    inverse: string;
    primaryButtonLabel: string;
  };
  background: {
    accent: string;
    primary: string;
    secondary: string;
    tertiary: string;
    shellTutorial: string;
    error: string;
    info: string;
    warn: string;
    inverse: string;
  };
  border: {
    primary: string;
    secondary: string;
    error: string;
    info: string;
    warn: string;
  };
  lineGraph: string[];
}

export type ComponentOverrides = {
  card: {
    background: string;
  };
};

export interface ThemeColors extends BasePalette {
  components: ComponentOverrides;
}

export interface ThemeShadows {
  levelInset1: string;
  level1: string;
  level2: string;
  level3: string;
  level4: string;
  input: {
    error: string;
    focus: string;
  };
}

const fontDefault = "Inter, Arial, sans-serif";

export const initialColorMode = "system";
export const config: ChakraTheme["config"] = {
  cssVarPrefix: "ck",
  initialColorMode,
  useSystemColorMode: true,
};

export interface MaterializeTheme extends ChakraTheme {
  // Because we don't use Chakra's built in dark / light handling, and because
  // the shell forces dark mode, useColorMode doesn't work as we would expect.
  // Instead, we can get the current theme and look at this value.
  colorMode: "light" | "dark";
  colors: ChakraTheme["colors"] & ThemeColors & typeof colors;
  textStyles: ChakraTheme["textStyles"] & TextStyles;
  fonts: {
    heading: string;
    body: string;
    mono: string;
  };
  shadows: ChakraTheme["shadows"] & ThemeShadows;
  radii: ChakraTheme["radii"] & {
    none: "0";
    sm: "0.125rem"; // 2px
    base: "0.25rem"; // 4px
    md: "0.375rem"; // 6px
    lg: "0.5rem"; // 8px
    xl: "0.75rem"; // 12px
    "2xl": "1rem"; // 16px
    "3xl": "1.5rem"; // 24px
    full: "9999px";
  };
  space: {
    px: "1px";
    0.5: "0.125rem";
    1: "0.25rem";
    1.5: "0.375rem";
    2: "0.5rem";
    2.5: "0.625rem";
    3: "0.75rem";
    3.5: "0.875rem";
    4: "1rem";
    5: "1.25rem";
    6: "1.5rem";
    7: "1.75rem";
    8: "2rem";
    9: "2.25rem";
    10: "2.5rem";
    12: "3rem";
    14: "3.5rem";
    16: "4rem";
    20: "5rem";
    24: "6rem";
    28: "7rem";
    32: "8rem";
    36: "9rem";
    40: "10rem";
    44: "11rem";
    48: "12rem";
    52: "13rem";
    56: "14rem";
    60: "15rem";
    64: "16rem";
    72: "18rem";
    80: "20rem";
    96: "24rem";
  };
}

/**
 * Removes the ugly and unconfigurable dropdown from input components with type "time" .
 *
 * Source: https://stackoverflow.com/questions/70881842/disable-datepicker-for-html-5-input-type-date
 */
const removeTimePickerGlobalStyles: SystemStyleInterpolation = {
  'input[type="time"]::-webkit-calendar-picker-indicator': {
    display: "none",
  },
};

const collapseOverflowOverrides: SystemStyleInterpolation = {
  '.chakra-collapse[style*="height: auto"]': {
    overflow: "initial !important",
  },
};

const devtoolOverrides: SystemStyleInterpolation = {
  ".jotai-devtools-trigger-button": {
    position: "initial !important",
  },
  // React query devtools button
  ".tsqd-open-btn-container": {
    height: "64px",
    width: "64px",
  },
};

export const baseTheme: Partial<ChakraTheme> = {
  breakpoints: {
    sm: "30em", // 480px
    md: "48em", // 768px
    lg: "62em", // 992px
    xl: "80em", // 1280px
    "2xl": "96em", // 1536px
    "3xl": "120em", // 1920px
    "4xl": "160em", // 2560px
  },
  textStyles: typographySystem,
  components: {
    // Something about the ESM module format breaks Chakra styles unless we use the spread operator here.
    // The spread operator removes the __esModule property among other things, which makes this work.
    ...components,
  },
  colors: {
    ...colors,
    teal: colors.turquoise,
    pink: colors.orchid,
    // our "blue" is closer to cyan, and we have "cobalt" also, hence remapping
    cyan: colors.blue,
    blue: colors.cobalt,
    // nb: style guide color palettes without a standard chakra equivalent: indigo, lavender
  },
  fonts: {
    heading: fontDefault,
    body: fontDefault,
    mono: "'Roboto Mono', Menlo, monospace",
  },
  styles: {
    global: (props: StyleFunctionProps) => ({
      html: {
        /**
         * Fix base font size to 16px to prevent different browser font size preferences
         * breaking layout based on rem units.
         */
        fontSize: "16px",
      },
      body: {
        bg: mode(colors.white, colors.gray[900])(props),
        // Prevents menu content from blowing out the body and showing a scrollbar
        overflow: "hidden",
        ...typographySystem["text-base"],
      },
      "*": {
        fontVariantLigatures: "none",
      },
      iframe: {
        // Prevents background color issue with statuspage.io iframes
        colorScheme: "light",
      },
      ...removeTimePickerGlobalStyles,
      ...collapseOverflowOverrides,
      ...devtoolOverrides,
    }),
  },
  radii: {
    none: "0",
    sm: "0.125rem", // 2px
    base: "0.25rem", // 4px
    md: "0.375rem", // 6px
    lg: "0.5rem", // 8px
    xl: "0.75rem", // 12px
    "2xl": "1rem", // 16px
    "3xl": "1.5rem", // 24px
    full: "9999px",
  } as MaterializeTheme["radii"],
  config,
};

export const darkTheme = extendTheme(baseTheme, {
  colorMode: "dark",
  colors: darkColors,
  semanticTokens: {
    colors: {
      "chakra-placeholder-color": darkColors.foreground.secondary,
      "chakra-body-text": darkColors.foreground.primary,
    },
  },
  shadows: darkShadows,
}) as MaterializeTheme;

export const lightTheme = extendTheme(baseTheme, {
  colorMode: "light",
  colors: lightColors,
  semanticTokens: {
    colors: {
      "chakra-placeholder-color": lightColors.foreground.secondary,
      "chakra-body-text": lightColors.foreground.primary,
    },
  },
  shadows: lightShadows,
}) as MaterializeTheme;

// Extracted from Figma.
export const fronteggAuthPageBackground = `
  ${gradients.primary.gradient},
  ${gradients.primary.fallback}
`;
const buildFronteggTheme: ({
  requiresExternalRegistration,
}: {
  requiresExternalRegistration: boolean;
}) => FronteggThemeOptions = ({ requiresExternalRegistration }) => ({
  loginBox: {
    boxFooter: () => (
      <p style={{ textAlign: "center", marginTop: "2em" }}>
        Need help?{" "}
        <a
          target="_blank"
          rel="noreferrer"
          href="https://materialize.com/support/"
          style={{ color: colors.purple[400], fontWeight: "600" }}
        >
          Contact support
        </a>
        .
      </p>
    ),
    boxStyle: {
      backgroundColor: "white",
    },
    login: {
      signupMessage: requiresExternalRegistration
        ? () => {
            return (
              <p style={{ textAlign: "center" }}>
                Don&apos;t have an account?{" "}
                <a
                  href="https://materialize.com/register"
                  style={{ color: colors.purple[400], fontWeight: "600" }}
                >
                  Sign up
                </a>
                .
              </p>
            );
          }
        : undefined,
    },
    signup: {
      // This is a bit of a hack to redirect to the external registration form
      // when Frontegg tries to render the stock registration form. It's not a
      // big deal, though, since the only way the user could trigger this is
      // by directly navigating to /account/sign-up, which shouldn't be linked
      // to from anywhere.
      loginMessage: requiresExternalRegistration
        ? () => {
            window.location.href = "https://materialize.com/register";
          }
        : undefined,
      hideSignUpForm: requiresExternalRegistration,
    },
    logo: {
      image: () => (
        <Flex w="100%" justifyContent="center">
          <img src={materializeLogo} />
        </Flex>
      ),
      placement: "page",
    },
  },
});

export const buildReactSelectFilterStyles = <
  Option = unknown,
  IsMulti extends boolean = boolean,
  Group extends GroupBase<Option> = GroupBase<Option>,
>(
  {
    colors: themeColors,
    shadows,
  }: {
    colors: ThemeColors;
    shadows: ThemeShadows;
  },
  overrides: StylesConfig<Option, IsMulti, Group> = {},
): StylesConfig<Option, IsMulti, Group> => {
  return mergeStyles(
    {
      menu: (base) => ({
        ...base,
        position: "absolute",
        marginTop: "2px",
        minWidth: "240px",
        width: "fit-content",
        background: themeColors.background.primary,
        border: "1px solid",
        borderColor: themeColors.border.primary,
        shadow: shadows.level2,
        borderRadius: "8px",
        overflow: "hidden",
        zIndex: SELECT_MENU_Z_INDEX,
      }),
      control: (base, state) => ({
        ...base,
        cursor: "pointer",
        fontSize: "14px",
        lineHeight: "16px",
        minHeight: "32px",
        padding: "0px",
        borderRadius: "8px",
        overflow: "hidden",
        borderWidth: "0",
        background: state.isFocused
          ? themeColors.background.secondary
          : themeColors.background.primary,
        "&:hover": {
          // disable the default hover style
        },
        '&[aria-disabled="true"]': {
          opacity: 0.4,
          cursor: "not-allowed",
        },
      }),
      dropdownIndicator: (base) => ({
        ...base,
        color: themeColors.foreground.secondary,
      }),
      groupHeading: (base) => ({
        ...base,
        color: themeColors.foreground.tertiary,
        fontSize: "14px",
        fontWeight: "500",
        lineHeight: "16px",
        overflow: "hidden",
        padding: "0px 8px",
        marginBottom: "8px",
        textTransform: "none",
      }),
      option: (base) => ({
        ...base,
        userSelect: "none",
        cursor: "pointer",
      }),
      input: (base) => ({
        ...base,
        color: themeColors.foreground.primary,
      }),
      indicatorSeparator: (base) => ({
        ...base,
        display: "none",
      }),
      valueContainer: (base) => ({
        ...base,
        paddingRight: "2px",
      }),
      singleValue: (base) => ({
        ...base,
        padding: 0,
        color: themeColors.foreground.primary,
      }),
    },
    overrides,
  );
};

export const getFronteggTheme = ({
  requiresExternalRegistration,
}: {
  requiresExternalRegistration: boolean;
}): FronteggThemeOptions => ({
  ...buildFronteggTheme({ requiresExternalRegistration }),
  palette: {
    error: {
      main: colors.red[500],
    },
    primary: {
      main: colors.purple[600],
      light: colors.purple[200],
      active: colors.purple[800],
      hover: colors.purple[800],
    },
    background: {
      default: colors.white,
    },
  },
});
