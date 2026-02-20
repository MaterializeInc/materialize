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
} from "@chakra-ui/react";
import React from "react";
import { Link } from "react-router-dom";

import { createNamespace } from "~/api/materialize";
import { ConnectionDependency } from "~/api/materialize/object-explorer/connectionDependencies";
import { ShowCreateObjectType } from "~/api/materialize/showCreate";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { LoadingContainer } from "~/components/LoadingContainer";
import { ShowCreateBlock } from "~/components/ShowCreateBlock";
import TextLink from "~/components/TextLink";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { MaterializeTheme } from "~/theme";
import { truncateMaxWidth } from "~/theme/components/Table";
import { capitalizeSentence } from "~/util";

import { ObjectDetailsContainer, ObjectDetailsStrip } from "./detailComponents";
import { useConnectionDependencies, useObjectDetails } from "./queries";
import { relativeObjectPath } from "./routerHelpers";
import { ObjectDetailsParams } from "./useSchemaObjectParams";

const ConnectionDependencyTable = ({
  connectionDependencies,
}: {
  connectionDependencies: ConnectionDependency[];
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Table variant="standalone">
      <Thead>
        <Tr>
          <Th>Name</Th>
          <Th>Type</Th>
        </Tr>
      </Thead>
      <Tbody>
        {connectionDependencies.map((connectionDependency) => {
          return (
            <Tr
              key={connectionDependency.id}
              aria-label={connectionDependency.name}
            >
              <Td {...truncateMaxWidth} py="2">
                <Text
                  textStyle="text-small"
                  fontWeight="500"
                  noOfLines={1}
                  color={colors.foreground.secondary}
                >
                  {createNamespace(
                    connectionDependency.databaseName,
                    connectionDependency.schemaName,
                  )}
                </Text>
                <TextLink
                  as={Link}
                  textStyle="text-ui-med"
                  noOfLines={1}
                  to={`../../../${relativeObjectPath({
                    id: connectionDependency.id,
                    schemaName: connectionDependency.schemaName,
                    objectName: connectionDependency.name,
                    databaseName: connectionDependency.databaseName,
                    objectType: connectionDependency.type,
                  })}`}
                >
                  {connectionDependency.name}
                </TextLink>
              </Td>
              <Td {...truncateMaxWidth} py="2">
                <Text
                  textStyle="text-small"
                  fontWeight="500"
                  noOfLines={1}
                  color={colors.foreground.secondary}
                >
                  {capitalizeSentence(connectionDependency.subType, false)}
                </Text>
                <Text textStyle="text-ui-med" noOfLines={1}>
                  {capitalizeSentence(connectionDependency.type, false)}
                </Text>
              </Td>
            </Tr>
          );
        })}
      </Tbody>
    </Table>
  );
};

export const ConnectionDetailsContent = ({
  databaseName,
  schemaName,
  objectName: name,
  id,
}: ObjectDetailsParams) => {
  const { data: object } = useObjectDetails({
    databaseName,
    schemaName,
    name,
  });

  const { data: connectionDependencies } = useConnectionDependencies({
    connectionId: id,
  });

  return (
    <MainContentContainer>
      <ObjectDetailsContainer>
        <ObjectDetailsStrip {...object} />
        <ShowCreateBlock
          {...object}
          objectType={object.type as ShowCreateObjectType}
        />
        {connectionDependencies.length > 0 && (
          <ConnectionDependencyTable
            connectionDependencies={connectionDependencies}
          />
        )}
      </ObjectDetailsContainer>
    </MainContentContainer>
  );
};

export const ConnectionDetails = (props: ObjectDetailsParams) => (
  <AppErrorBoundary message="An error occurred loading object details.">
    <React.Suspense fallback={<LoadingContainer />}>
      <ConnectionDetailsContent {...props} />
    </React.Suspense>
  </AppErrorBoundary>
);
