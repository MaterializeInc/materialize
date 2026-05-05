// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Text, VStack } from "@chakra-ui/react";
import React from "react";

import { ShowCreateObjectType } from "~/api/materialize/showCreate";
import { LoadingContainer } from "~/components/LoadingContainer";
import { ShowCreateBlock } from "~/components/ShowCreateBlock";
import { ObjectColumnsList } from "~/platform/object-explorer/ObjectColumns";

import { MaintainedObjectListItem } from "./queries";
import { SourceDiagnostics } from "./SourceDiagnostics";

export interface ObjectMetadataProps {
  item: MaintainedObjectListItem;
}

export const ObjectMetadata = ({ item }: ObjectMetadataProps) => {
  const isSource = item.objectType === "source";

  return (
    <VStack align="start" spacing={6} width="100%">
      {isSource && <SourceDiagnostics sourceId={item.id} />}
      <ObjectColumnsList
        databaseName={item.databaseName}
        schemaName={item.schemaName}
        name={item.name}
      />
      <Box width="100%">
        <Text textStyle="heading-sm" mb={3}>
          SQL definition
        </Text>
        {/* TODO(@leedqin): drop this Suspense wrapper once ShowCreateBlock
            no longer relies on a suspense query internally. */}
        <React.Suspense fallback={<LoadingContainer />}>
          <ShowCreateBlock
            databaseName={item.databaseName}
            schemaName={item.schemaName}
            name={item.name}
            objectType={item.objectType as ShowCreateObjectType}
          />
        </React.Suspense>
      </Box>
    </VStack>
  );
};
