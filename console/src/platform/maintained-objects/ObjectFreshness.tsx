// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Center, Text, useTheme } from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";

import { MaintainedObjectListItem } from "./queries";

export interface ObjectFreshnessProps {
  item: MaintainedObjectListItem;
}

// TODO: Add freshness graphs
export const ObjectFreshness = (_props: ObjectFreshnessProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Center py={10}>
      <Text textStyle="text-base" color={colors.foreground.secondary}>
        Freshness graphs placeholder
      </Text>
    </Center>
  );
};
