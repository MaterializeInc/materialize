// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { fileURLToPath } from "node:url";

import { fixupPluginRules, includeIgnoreFile } from "@eslint/compat";
import js from "@eslint/js";
import pluginQuery from "@tanstack/eslint-plugin-query";
import { defineConfig } from "eslint/config";
import jestFormatting from "eslint-plugin-jest-formatting";
import prettierRecommended from "eslint-plugin-prettier/recommended";
import react from "eslint-plugin-react";
import reactHooks from "eslint-plugin-react-hooks";
import reactRefresh from "eslint-plugin-react-refresh";
import simpleImportSort from "eslint-plugin-simple-import-sort";
import unicorn from "eslint-plugin-unicorn";
import globals from "globals";
import tseslint from "typescript-eslint";

const subscribeOptionsMessage =
  "Declare subscribe options at module scope; an inline object restarts the SUBSCRIBE session on every render.";

export default defineConfig([
  includeIgnoreFile(
    fileURLToPath(new URL(".gitignore", import.meta.url)),
    "Imported .gitignore patterns",
  ),
  {
    ignores: [
      "src/api/schemas",
      "types/materialize.d.ts",
      "vendor",
      // The jest-formatting file patterns below would otherwise pull vitest
      // snapshots into the lint set.
      "**/*.snap",
    ],
  },
  {
    // Matches the ESLint 8 default, where unused directives went unreported.
    linterOptions: {
      reportUnusedDisableDirectives: "off",
    },
  },
  js.configs.recommended,
  pluginQuery.configs["flat/recommended"],
  tseslint.configs.recommended,
  prettierRecommended,
  reactHooks.configs.flat.recommended,
  {
    ...react.configs.flat.recommended,
    // eslint-plugin-react still calls context methods that ESLint 10 removed
    // (e.g. `getFilename` during React version detection).
    plugins: { react: fixupPluginRules(react) },
  },
  {
    languageOptions: {
      globals: {
        ...globals.node,
        ...globals.es2024,
      },
      parserOptions: {
        ecmaFeatures: {
          jsx: true,
        },
      },
    },
    plugins: {
      "react-refresh": reactRefresh,
      "simple-import-sort": simpleImportSort,
      unicorn,
    },
    settings: {
      react: {
        version: "detect",
      },
    },
    rules: {
      "unicorn/prefer-module": "error",
      "unicorn/prefer-node-protocol": "error",
      "prettier/prettier": "error",
      "simple-import-sort/imports": "error",
      "simple-import-sort/exports": "error",
      "no-restricted-syntax": [
        "error",
        {
          selector:
            "CallExpression[callee.name='useGlobalUpsertSubscribe'] > ObjectExpression",
          message: subscribeOptionsMessage,
        },
        {
          selector:
            "CallExpression[callee.name='useGlobalSubscribeCollection'] > ObjectExpression",
          message: subscribeOptionsMessage,
        },
      ],
      "@typescript-eslint/no-shadow": "error",
      "@typescript-eslint/ban-ts-comment": "off",
      "@typescript-eslint/explicit-module-boundary-types": "off",
      "@typescript-eslint/no-explicit-any": "off",
      "@typescript-eslint/no-non-null-assertion": "off",
      "@typescript-eslint/no-unused-expressions": [
        "error",
        {
          allowShortCircuit: true,
        },
      ],
      "@typescript-eslint/no-unused-vars": [
        "error",
        {
          args: "none",
          argsIgnorePattern: "^_",
          caughtErrors: "none",
          destructuredArrayIgnorePattern: "^_",
          varsIgnorePattern: "^_",
        },
      ],
      "react-hooks/exhaustive-deps": "error",
      // TODO: Enable once the existing violations are fixed. These rules are
      // new in @eslint/js 10 and in the React Compiler diagnostics of
      // eslint-plugin-react-hooks 7.
      "no-useless-assignment": "off",
      "preserve-caught-error": "off",
      "react-hooks/immutability": "off",
      "react-hooks/incompatible-library": "off",
      "react-hooks/preserve-manual-memoization": "off",
      "react-hooks/purity": "off",
      "react-hooks/refs": "off",
      "react-hooks/set-state-in-effect": "off",
      "react-hooks/use-memo": "off",
      "react-refresh/only-export-components": [
        "warn",
        {
          allowConstantExport: true,
        },
      ],
      "react/display-name": "off",
      "react/function-component-definition": [
        2,
        {
          namedComponents: "arrow-function",
        },
      ],
      "react/prop-types": "off",
      "react/jsx-curly-brace-presence": [
        1,
        {
          props: "never",
          children: "never",
          propElementValues: "always",
        },
      ],
      "no-restricted-globals": [
        "error",
        {
          name: "assert",
          message: "Use 'assert' from '~/util' instead.",
        },
      ],
      "no-restricted-imports": [
        "error",
        {
          paths: [
            {
              name: "@chakra-ui/react",
              importNames: ["Modal"],
              message: "Use ~/components/Modal instead.",
            },
            {
              name: "date-fns",
              importNames: ["format"],
              message: "Use formatDate from ~/utils/dateFormat instead.",
            },
            {
              name: "lodash",
              message:
                "Use the separate packages instead from e.g. lodash.debounce.",
            },
          ],
          patterns: [
            {
              group: ["*.svg?react"],
              message: "Use a named import from `~/icons` instead.",
            },
            {
              group: ["@frontegg/*"],
              message:
                "Import from src/external-library-wrappers/frontegg.ts instead. This is to control mocking of the Frontegg library.",
            },
          ],
        },
      ],
    },
  },
  {
    // eslint-plugin-jest-formatting only ships an eslintrc config, so its
    // `recommended` preset is spelled out here.
    files: [
      "**/*.test.*",
      "**/*_test.*",
      "**/*Test.*",
      "**/*.spec.*",
      "**/*_spec.*",
      "**/*Spec.*",
      "**/__tests__/*",
    ],
    plugins: {
      "jest-formatting": fixupPluginRules(jestFormatting),
    },
    rules: {
      "jest-formatting/padding-around-after-all-blocks": "error",
      "jest-formatting/padding-around-after-each-blocks": "error",
      "jest-formatting/padding-around-before-all-blocks": "error",
      "jest-formatting/padding-around-before-each-blocks": "error",
      "jest-formatting/padding-around-describe-blocks": "error",
      "jest-formatting/padding-around-test-blocks": "error",
    },
  },
  {
    files: ["src/test/*", "*/.test.ts[x]", ".storybook/*"],
    rules: {
      "react-refresh/only-export-components": "off",
    },
  },
]);
