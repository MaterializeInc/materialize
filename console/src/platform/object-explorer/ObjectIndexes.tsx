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
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";
import { Link } from "react-router-dom";

import { createNamespace } from "~/api/materialize";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import IndexListEmptyState from "~/components/IndexListEmptyState";
import { LoadingContainer } from "~/components/LoadingContainer";
import TextLink from "~/components/TextLink";
import { MainContentContainer } from "~/layouts/BaseLayout";
import docUrls from "~/mz-doc-urls.json";
import { MaterializeTheme } from "~/theme";
import { truncateMaxWidth } from "~/theme/components/Table";
import { formatDate, FRIENDLY_DATETIME_FORMAT } from "~/utils/dateFormat";

import { useObjectIndexes } from "./queries";
import { relativeObjectPath } from "./routerHelpers";
import { useSchemaObjectParams } from "./useSchemaObjectParams";

export const ObjectIndexes = () => {
  return (
    <AppErrorBoundary message="An error occurred loading indexes.">
      <React.Suspense fallback={<LoadingContainer />}>
        <ObjectIndexesContent />
      </React.Suspense>
    </AppErrorBoundary>
  );
};

export const ObjectIndexesContent = () => {
  const { databaseName, schemaName, objectName } = useSchemaObjectParams();
  const { colors } = useTheme<MaterializeTheme>();
  const {
    data: { rows: objectIndexes },
  } = useObjectIndexes({
    databaseName,
    schemaName,
    name: objectName,
  });

  const isEmpty = objectIndexes.length === 0;

  if (isEmpty) {
    return <IndexListEmptyState title="This object has no indexes" />;
  }

  return (
    <MainContentContainer>
      <VStack alignItems="flex-start" spacing="6">
        <VStack maxWidth="650px" spacing="4" alignItems="flex-start">
          <Text textStyle="heading-lg">Indexes</Text>
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            Indexes maintain and incrementally update results in memory within a
            cluster. The in-memory results are accessible to queries within the
            cluster, even for queries that do not use the index key(s).{" "}
            <TextLink href={docUrls["/docs/concepts/indexes/"]} isExternal>
              Learn more.
            </TextLink>
          </Text>
        </VStack>

        <Table variant="standalone">
          <Thead>
            <Tr>
              <Th>Name</Th>
              <Th>Owner</Th>
              <Th>Created at</Th>
            </Tr>
          </Thead>
          <Tbody>
            {objectIndexes.map((index) => (
              <Tr key={index.id}>
                <Td {...truncateMaxWidth} py="2">
                  <Text
                    textStyle="text-small"
                    fontWeight="500"
                    noOfLines={1}
                    mb="2px"
                    color={colors.foreground.secondary}
                  >
                    {createNamespace(index.databaseName, index.schemaName)}
                  </Text>
                  <TextLink
                    as={Link}
                    textStyle="text-ui-reg"
                    noOfLines={1}
                    to={`../../../../${relativeObjectPath({
                      id: index.id,
                      schemaName: index.schemaName,
                      objectName: index.name,
                      databaseName: index.databaseName,
                      objectType: "index",
                    })}`}
                  >
                    {index.name} ({index.indexedColumns.join(", ")})
                  </TextLink>
                </Td>
                <Td {...truncateMaxWidth} py="2">
                  <Text noOfLines={1}>{index.owner}</Text>
                </Td>
                <Td {...truncateMaxWidth} py="2">
                  {index.createdAt && (
                    <Text noOfLines={1}>
                      {formatDate(index.createdAt, FRIENDLY_DATETIME_FORMAT)}
                    </Text>
                  )}
                </Td>
              </Tr>
            ))}
          </Tbody>
        </Table>
      </VStack>
    </MainContentContainer>
  );
};
