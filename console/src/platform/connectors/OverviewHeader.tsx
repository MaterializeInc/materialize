// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Flex, HStack, Text, useTheme } from "@chakra-ui/react";
import React, { forwardRef } from "react";

import { createNamespace } from "~/api/materialize";
import { Sink } from "~/api/materialize/sink/sinkList";
import { Source } from "~/api/materialize/source/sourceList";
import { ConnectorStatusPill } from "~/components/StatusPill";
import TimePeriodSelect from "~/components/TimePeriodSelect";
import { MaterializeTheme } from "~/theme";

// TO-DO: @leedqin will make clusterName and replicaName as unoptional when sink view is updated
interface OverviewHeaderProps {
  connector: Source | Sink;
  timePeriodMinutes: number;
  setTimePeriodMinutes: React.Dispatch<React.SetStateAction<number>>;
  clusterName?: string;
  replicaName?: string;
}

const OverviewHeader = forwardRef<HTMLDivElement, OverviewHeaderProps>(
  (
    {
      connector,
      timePeriodMinutes,
      setTimePeriodMinutes,
      clusterName,
      replicaName,
    },
    ref,
  ) => {
    const { colors } = useTheme<MaterializeTheme>();
    return (
      <Flex alignItems="flex-start" width="100%">
        <HStack
          justifyContent="space-between"
          width="100%"
          ref={ref}
          flexWrap="wrap"
        >
          <Box>
            <Text
              textStyle="text-ui-med"
              fontWeight="500"
              noOfLines={1}
              color={colors.foreground.secondary}
            >
              {createNamespace(connector.databaseName, connector.schemaName)}
            </Text>
            <HStack spacing="5">
              <Text textStyle="heading-lg">{connector.name}</Text>
              <ConnectorStatusPill connector={connector} />
            </HStack>
            {clusterName && (
              <Text wordBreak="break-all" textStyle="text-medium" noOfLines={1}>
                <Text as="span" color={colors.foreground.secondary}>
                  Usage metrics from
                </Text>{" "}
                <Text as="span" fontWeight={500}>
                  {clusterName}
                  {replicaName ? `.${replicaName}` : ""}
                </Text>
              </Text>
            )}
          </Box>
          <Box marginLeft="auto">
            <TimePeriodSelect
              timePeriodMinutes={timePeriodMinutes}
              setTimePeriodMinutes={setTimePeriodMinutes}
            />
          </Box>
        </HStack>
      </Flex>
    );
  },
);

export default OverviewHeader;
