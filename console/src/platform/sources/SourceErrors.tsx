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

import { Source } from "~/api/materialize/source/sourceList";
import Alert from "~/components/Alert";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { HEIGHT_PX } from "~/components/ConnectorErrorsGraph";
import ConnectorErrorsTable from "~/components/ConnectorErrorsTable";
import TimePeriodSelect from "~/components/TimePeriodSelect";
import { useTimePeriodMinutes } from "~/hooks/useTimePeriodSelect";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { MaterializeTheme } from "~/theme";

import { SOURCES_FETCH_ERROR_MESSAGE } from "./constants";
import { useSourceErrors } from "./queries";
import SourceErrorsGraph from "./SourceErrorsGraph";

export interface SourceErrorsProps {
  source: Source;
}

const SourceErrors = ({ source }: SourceErrorsProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [timePeriodMinutes, setTimePeriodMinutes] = useTimePeriodMinutes({
    localStorageKey: "mz-source-errors-time-period",
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
      <HStack spacing={6} alignItems="flex-start" flex="1" height="100%">
        <VStack width="100%" alignItems="flex-start" spacing={6} flex="1">
          <VStack width="100%" alignItems="flex-start" spacing={6} flex="1">
            {source?.error && (
              <Alert variant="error" width="100%" message={source?.error} />
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
                  Source Errors
                </Text>
                <TimePeriodSelect
                  timePeriodMinutes={timePeriodMinutes}
                  setTimePeriodMinutes={updateTimePeriod}
                />
              </Flex>
              <AppErrorBoundary
                message={SOURCES_FETCH_ERROR_MESSAGE}
                containerProps={{ height: HEIGHT_PX }}
              >
                <SourceErrorsGraph
                  sourceId={source.id}
                  timePeriodMinutes={timePeriodMinutes}
                />
              </AppErrorBoundary>
            </Box>
            <AppErrorBoundary
              message={SOURCES_FETCH_ERROR_MESSAGE}
              containerProps={{ height: HEIGHT_PX }}
            >
              <SourceErrorsTable
                timePeriodMinutes={timePeriodMinutes}
                sourceId={source.id}
              />
            </AppErrorBoundary>
          </VStack>
        </VStack>
      </HStack>
    </MainContentContainer>
  );
};

const SourceErrorsTable = ({
  sourceId,
  timePeriodMinutes,
}: {
  sourceId: string;
  timePeriodMinutes: number;
}) => {
  const { data } = useSourceErrors({
    sourceId,
    timePeriodMinutes,
  });

  return (
    <ConnectorErrorsTable
      errors={data.rows}
      timePeriodMinutes={timePeriodMinutes}
    />
  );
};

export default SourceErrors;
