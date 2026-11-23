// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Card, Text, useTheme, VStack } from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";

export interface MetricCardProps {
  label: string;
  children: React.ReactNode;
  /** Defaults to the card's theme surface. */
  background?: string;
}

/** A labelled tile: the label above whatever value it holds. */
export const MetricCard = ({
  label,
  children,
  background,
}: MetricCardProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Card
      px={3}
      py={2}
      borderWidth="1px"
      borderColor={colors.border.primary}
      borderRadius="md"
      background={background}
    >
      <VStack align="stretch" spacing={2}>
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          {label}
        </Text>
        {children}
      </VStack>
    </Card>
  );
};
