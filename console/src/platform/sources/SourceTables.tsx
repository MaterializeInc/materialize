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
  HStack,
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

import { createNamespace } from "~/api/materialize";
import { ConnectorStatusPill } from "~/components/StatusPill";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { MaterializeTheme } from "~/theme";
import { truncateMaxWidth } from "~/theme/components/Table";

import { useSourceTables } from "./queries";

export interface SourceTableProps {
  sourceId: string;
}

export const SourceTables = ({ sourceId }: SourceTableProps) => {
  const {
    data: { rows: sources },
  } = useSourceTables({ sourceId });
  const { colors } = useTheme<MaterializeTheme>();

  const isEmpty = sources && sources.length === 0;

  return (
    <MainContentContainer>
      <HStack spacing={6} height="100%">
        <VStack width="100%" spacing={6} height="100%">
          <VStack
            spacing={6}
            width="100%"
            height="100%"
            alignItems="flex-start"
          >
            <Text textStyle="heading-sm">Subsources</Text>
            {isEmpty ? (
              <Flex width="100%" justifyContent="center">
                No subsources
              </Flex>
            ) : (
              <Table
                variant="standalone"
                data-testid="source-table"
                borderRadius="xl"
              >
                <Thead>
                  <Tr>
                    <Th>Name</Th>
                    <Th>Status</Th>
                  </Tr>
                </Thead>
                <Tbody>
                  {sources?.map((s) => (
                    <Tr key={s.id}>
                      <Td py="3" {...truncateMaxWidth}>
                        <Text
                          textStyle="text-small"
                          fontWeight="500"
                          mb="2px"
                          noOfLines={1}
                          color={colors.foreground.secondary}
                        >
                          {createNamespace(s.databaseName, s.schemaName)}
                        </Text>
                        <Text textStyle="text-ui-med" noOfLines={1}>
                          {s.name}
                        </Text>
                      </Td>
                      <Td>
                        {s.status ? <ConnectorStatusPill connector={s} /> : "-"}
                      </Td>
                    </Tr>
                  ))}
                </Tbody>
              </Table>
            )}
          </VStack>
          );
        </VStack>
      </HStack>
    </MainContentContainer>
  );
};

export default SourceTables;
