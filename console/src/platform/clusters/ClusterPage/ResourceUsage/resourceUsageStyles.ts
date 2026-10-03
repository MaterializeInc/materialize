// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useTheme } from "@chakra-ui/react";

import { MaterializeTheme } from "~/theme";

import { ReplicaState } from "./resourceUsageModel";

/** Shared by every chart so their plot areas line up on one time axis. */
export const CHART_MARGIN = { top: 24, right: 16, bottom: 8, left: 56 };

export const PERCENT_TICKS = [0, 25, 50, 75, 100];

/** A memory bar's narrowest slot: an 8px bar plus the 2px gap to its neighbor. */
export const MIN_BAR_SLOT_PX = 10;

export const useStatusStyles = () => {
  const { colors } = useTheme<MaterializeTheme>();
  return {
    running: { label: "running", color: colors.accent.green, opacity: 0.45 },
    hydrating: {
      label: "hydrating",
      color: colors.accent.darkYellow,
      opacity: 0.7,
    },
    offline: {
      label: "offline",
      color: colors.foreground.tertiary,
      opacity: 0.45,
    },
  } satisfies Record<
    ReplicaState,
    { label: string; color: string; opacity: number }
  >;
};
