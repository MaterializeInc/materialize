// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Text, Tooltip, VStack } from "@chakra-ui/react";
import { createColumnHelper } from "@tanstack/react-table";
import React from "react";

import { Column } from "~/api/materialize/object-explorer/objectColumns";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { LoadingContainer } from "~/components/LoadingContainer";
import { UniversalTable } from "~/components/Table/UniversalTable";
import { useUniversalTable } from "~/components/Table/useUniversalTable";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { truncateMaxWidth } from "~/theme/components/Table";
import { pluralize } from "~/util";

import { useObjectColumns } from "./queries";
import { useSchemaObjectParams } from "./useSchemaObjectParams";

const columnHelper = createColumnHelper<Column>();

const tableColumns = [
  columnHelper.accessor("name", {
    header: "Name",
    cell: (info) => <Text noOfLines={1}>{info.getValue()}</Text>,
    meta: { cellProps: { ...truncateMaxWidth, py: "2" } },
  }),
  columnHelper.accessor("nullable", {
    header: "Nullable",
    cell: (info) => <Text noOfLines={1}>{info.getValue().toString()}</Text>,
    meta: { cellProps: { ...truncateMaxWidth, py: "2" } },
  }),
  columnHelper.accessor("type", {
    header: "Type",
    cell: (info) => <Text noOfLines={1}>{info.getValue()}</Text>,
    meta: { cellProps: { ...truncateMaxWidth, py: "2" } },
  }),
  columnHelper.accessor("columnComment", {
    header: "Comment",
    cell: (info) => {
      const comment = info.getValue() ?? "";
      if (!comment) return null;
      return (
        <Tooltip label={comment} lineHeight={1.2} openDelay={200}>
          <Box>
            <Text whiteSpace="normal" noOfLines={3}>
              {comment}
            </Text>
          </Box>
        </Tooltip>
      );
    },
    meta: {
      cellProps: { ...truncateMaxWidth, py: "2", verticalAlign: "top" },
    },
  }),
];

export interface ObjectColumnsListProps {
  databaseName: string;
  schemaName: string;
  name: string;
}

const ObjectColumnsBoundary = ({ children }: { children: React.ReactNode }) => (
  <AppErrorBoundary message="An error occurred loading object columns.">
    <React.Suspense fallback={<LoadingContainer />}>{children}</React.Suspense>
  </AppErrorBoundary>
);

const ObjectColumnsContent = ({
  databaseName,
  schemaName,
  name,
  hideIfEmpty,
}: ObjectColumnsListProps & { hideIfEmpty?: boolean }) => {
  const {
    data: { rows },
  } = useObjectColumns({ databaseName, schemaName, name });
  const table = useUniversalTable({
    data: rows,
    columns: tableColumns,
    pageSize: 1000,
  });
  const relationComment = rows[0]?.relationComment;

  if (hideIfEmpty && rows.length === 0) return null;

  return (
    <VStack alignItems="flex-start" spacing="6" width="100%">
      <Text textStyle="heading-sm">
        {rows.length} {pluralize(rows.length, "Column", "Columns")}
      </Text>
      {relationComment && <Text textStyle="text-small">{relationComment}</Text>}
      <UniversalTable table={table} variant="linkable" />
    </VStack>
  );
};

export const ObjectColumns = () => {
  const { databaseName, schemaName, objectName } = useSchemaObjectParams();
  return (
    <ObjectColumnsBoundary>
      <MainContentContainer>
        <ObjectColumnsContent
          databaseName={databaseName}
          schemaName={schemaName}
          name={objectName}
        />
      </MainContentContainer>
    </ObjectColumnsBoundary>
  );
};

export const ObjectColumnsList = (props: ObjectColumnsListProps) => (
  <ObjectColumnsBoundary>
    <ObjectColumnsContent {...props} hideIfEmpty />
  </ObjectColumnsBoundary>
);
