// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Flex,
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import { GroupedError } from "~/api/materialize/source/sourceErrors";
import { DEFAULT_OPTIONS } from "~/hooks/useTimePeriodSelect";
import { MaterializeTheme } from "~/theme";
import { DATE_FORMAT_SHORT, formatDate } from "~/utils/dateFormat";

interface ConnectorErrorsTableProps {
  errors: GroupedError[] | null;
  timePeriodMinutes: number;
}

const titleForTimePeriod = (timePeriodMinutes: number) => {
  const period =
    DEFAULT_OPTIONS[
      timePeriodMinutes.toString() as keyof typeof DEFAULT_OPTIONS
    ];
  return `Errors over the ${period.toLowerCase()}`;
};

const ConnectorErrorsTable = ({
  errors,
  timePeriodMinutes,
}: ConnectorErrorsTableProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  const isEmpty = errors && errors.length === 0;

  return (
    <VStack spacing={6} width="100%" alignItems="flex-start">
      <Text fontSize="16px" fontWeight={500}>
        {titleForTimePeriod(timePeriodMinutes)}
      </Text>
      {isEmpty ? (
        <Flex width="100%" justifyContent="center">
          No errors during this time period.
        </Flex>
      ) : (
        <Table
          variant="standalone"
          data-testid="connnector-errors-table"
          borderRadius="xl"
        >
          <Thead>
            <Tr>
              <Th>Error</Th>
              <Th>Count</Th>
              <Th>Last encountered</Th>
            </Tr>
          </Thead>
          <Tbody>
            {errors?.map((error) => {
              const lastOccurredText = formatDate(
                error.lastOccurred,
                "HH:mm:ss z",
              );

              return (
                <Tr key={error.lastOccurred.getMilliseconds()}>
                  <Td>{error.error}</Td>
                  <Td>{error.count.toString()}</Td>
                  <Td>
                    <Text color={colors.foreground.secondary} display="inline">
                      {formatDate(error.lastOccurred, DATE_FORMAT_SHORT)}
                    </Text>
                    <Text color={colors.foreground.secondary} display="inline">
                      {" · "}
                    </Text>
                    <Text
                      color={colors.foreground.primary}
                      display="inline"
                      title={lastOccurredText}
                    >
                      {lastOccurredText}
                    </Text>
                  </Td>
                </Tr>
              );
            })}
          </Tbody>
        </Table>
      )}
    </VStack>
  );
};

export default ConnectorErrorsTable;
