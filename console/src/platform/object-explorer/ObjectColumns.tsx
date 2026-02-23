// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { LoadingContainer } from "~/components/LoadingContainer";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { truncateMaxWidth } from "~/theme/components/Table";
import { pluralize } from "~/util";

import { useObjectColumns } from "./queries";
import { useSchemaObjectParams } from "./useSchemaObjectParams";

export const ObjectColumns = () => {
  return (
    <AppErrorBoundary message="An error occurred loading object columns.">
      <React.Suspense fallback={<LoadingContainer />}>
        <ObjectColumnsContent />
      </React.Suspense>
    </AppErrorBoundary>
  );
};

export const ObjectColumnsContent = () => {
  const { databaseName, schemaName, objectName } = useSchemaObjectParams();
  const {
    data: { rows: columns },
  } = useObjectColumns({
    databaseName,
    schemaName,
    name: objectName,
  });
  const relationComment = columns[0]?.relationComment;

  return (
    <MainContentContainer>
      <VStack alignItems="flex-start" spacing="6">
        <Text textStyle="heading-sm">
          {columns.length} {pluralize(columns.length, "Column", "Columns")}
        </Text>
        {relationComment && (
          <Text textStyle="text-small">{columns[0]?.relationComment}</Text>
        )}
        <Table variant="linkable" borderRadius="xl">
          <Thead>
            <Tr>
              <Th>Name</Th>
              <Th>Nullable</Th>
              <Th>Type</Th>
              <Th>Comment</Th>
            </Tr>
          </Thead>
          <Tbody>
            {columns.map((column) => (
              <Tr key={column.name}>
                <Td {...truncateMaxWidth} py="2">
                  <Text noOfLines={1}>{column.name}</Text>
                </Td>
                <Td {...truncateMaxWidth} py="2">
                  <Text noOfLines={1}>{column.nullable.toString()}</Text>
                </Td>
                <Td {...truncateMaxWidth} py="2">
                  <Text noOfLines={1}>{column.type}</Text>
                </Td>
                <Td {...truncateMaxWidth} py="2">
                  <Text noOfLines={1}>{column.columnComment}</Text>
                </Td>
              </Tr>
            ))}
          </Tbody>
        </Table>
      </VStack>
    </MainContentContainer>
  );
};
