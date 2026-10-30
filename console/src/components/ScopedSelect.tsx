// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, HStack, Select, useTheme } from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";

export interface ScopedSelectProps<Scope extends string> {
  /** The scope menu's accessible name. */
  scopeLabel: string;
  scope: Scope;
  scopes: { value: Scope; label: string }[];
  onScopeChange: (scope: Scope) => void;
  /**
   * The select for an item of the chosen scope. Render it `frameless`: this
   * component draws the border and focus ring around both.
   */
  children: React.ReactNode;
}

/**
 * A menu choosing what kind of thing to pick, joined to the select that picks
 * one: [Cluster ▾ | quickstart ▾].
 *
 * The scope menu is styled as the sidebar's links are, accent at rest and
 * stronger on hover, so it reads as a control beside the static labels of
 * `LabeledSelect` and `SearchableSelect`.
 */
const ScopedSelect = <Scope extends string>({
  scopeLabel,
  scope,
  scopes,
  onScopeChange,
  children,
}: ScopedSelectProps<Scope>) => {
  const { colors, shadows } = useTheme<MaterializeTheme>();
  return (
    <HStack
      spacing={0}
      alignItems="stretch"
      border={`1px solid ${colors.border.secondary}`}
      borderRadius="lg"
      boxShadow="0px 0px 0.5px rgba(0, 0, 0, 0.16), 0px 0.5px 2px rgba(0, 0, 0, 0.12)"
      transition="all 0.2s ease-in-out"
      _focusWithin={{
        borderColor: colors.accent.brightPurple,
        boxShadow: shadows.input.focus,
      }}
    >
      {/* The fill and divider are on this wrapper, which holds the select
          and its chevron alike: on the select itself they stop short of the
          chevron, which Chakra positions outside the field. */}
      <Box
        position="relative"
        display="flex"
        flexShrink={0}
        bg="background.accent"
        borderLeftRadius="7px"
        borderRight={`1px solid ${colors.border.secondary}`}
        transition="background 0.2s"
        // The sidebar's current-page fill (`NavItem`).
        _hover={{ bg: "rgba(90, 52, 203, 0.2)" }}
      >
        <Select
          aria-label={scopeLabel}
          value={scope}
          onChange={(e) => onScopeChange(e.target.value as Scope)}
          variant="unstyled"
          // Fills the purple area, chevron included, so all of it opens the
          // menu. Padding and width go through `sx` because Chakra's Select
          // passes `pl`, `pr` and `width` to its wrapper, which leaves the
          // edges and the chevron outside the <select>.
          rootProps={{ width: "100%", height: "100%" }}
          // Square corners: the wrapper draws the rounding, and a rounded
          // <select> does not take clicks in its corners.
          sx={{
            width: "100%",
            paddingLeft: "12px",
            paddingRight: "32px",
            borderRadius: 0,
          }}
          height="100%"
          minHeight="32px"
          fontSize="14px"
          fontWeight={500}
          lineHeight="16px"
          color="accent.brightPurple"
          iconColor="accent.brightPurple"
          cursor="pointer"
          _focusVisible={{ boxShadow: "none" }}
        >
          {scopes.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </Select>
      </Box>
      {children}
    </HStack>
  );
};

export default ScopedSelect;
