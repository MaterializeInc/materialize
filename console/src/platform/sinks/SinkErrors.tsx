// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Flex, HStack, Text, useTheme, VStack } from "@chakra-ui/react";
import React from "react";

import { Sink } from "~/api/materialize/sink/sinkList";
import Alert from "~/components/Alert";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { HEIGHT_PX } from "~/components/ConnectorErrorsGraph";
import ConnectorErrorsTable from "~/components/ConnectorErrorsTable";
import TimePeriodSelect from "~/components/TimePeriodSelect";
import { useTimePeriodMinutes } from "~/hooks/useTimePeriodSelect";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { MaterializeTheme } from "~/theme";

import { SINKS_FETCH_ERROR_MESSAGE } from "./constants";
import { useSinkErrors } from "./queries";
import SinkErrorsGraph from "./SinkErrorsGraph";

export interface SinkDetailProps {
  sink: Sink;
}

const SinkErrors = ({ sink }: SinkDetailProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [timePeriodMinutes, setTimePeriodMinutes] = useTimePeriodMinutes({
    localStorageKey: "mz-sink-errors-time-period",
  });
  const [_, startTransition] = React.useTransition();

  const updateTimePeriod = React.useCallback(
    (value: number) => {
      startTransition(() => setTimePeriodMinutes(value));
    },
    [setTimePeriodMinutes],
  );

  return (
    <MainContentContainer>
      <HStack spacing={6} alignItems="flex-start">
        <VStack width="100%" alignItems="flex-start" spacing={6}>
          <VStack width="100%" alignItems="flex-start" spacing={6}>
            {sink?.error && (
              <Alert variant="error" message={sink?.error} width="100%" />
            )}
            <Box
              border={`solid 1px ${colors.border.primary}`}
              borderRadius="8px"
              py={4}
              px={6}
              width="100%"
            >
              <Flex
                justifyContent="space-between"
                alignItems="center"
                width="100%"
                mb={4}
              >
                <Text fontSize="16px" fontWeight="500">
                  Sink Errors
                </Text>
                <TimePeriodSelect
                  timePeriodMinutes={timePeriodMinutes}
                  setTimePeriodMinutes={updateTimePeriod}
                />
              </Flex>
              <AppErrorBoundary
                message={SINKS_FETCH_ERROR_MESSAGE}
                containerProps={{ height: HEIGHT_PX }}
              >
                <SinkErrorsGraph
                  sinkId={sink?.id}
                  timePeriodMinutes={timePeriodMinutes}
                />
              </AppErrorBoundary>
            </Box>
            <AppErrorBoundary message={SINKS_FETCH_ERROR_MESSAGE}>
              <SinkErrorsTable
                sinkId={sink.id}
                timePeriodMinutes={timePeriodMinutes}
              />
            </AppErrorBoundary>
          </VStack>
        </VStack>
      </HStack>
    </MainContentContainer>
  );
};

const SinkErrorsTable = ({
  sinkId,
  timePeriodMinutes,
}: {
  sinkId: string;
  timePeriodMinutes: number;
}) => {
  const { data } = useSinkErrors({
    sinkId,
    timePeriodMinutes,
  });

  return (
    <ConnectorErrorsTable
      errors={data.rows}
      timePeriodMinutes={timePeriodMinutes}
    />
  );
};

export default SinkErrors;
