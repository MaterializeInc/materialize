// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Box,
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
import { useSuspenseQuery } from "@tanstack/react-query";
import React from "react";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import { mcpQuerySystemCatalog } from "~/api/materialize/mcpClient";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { LoadingContainer } from "~/components/LoadingContainer";
import { MaterializeTheme } from "~/theme";
import { truncateMaxWidth } from "~/theme/components/Table";

interface DataProduct {
  objectName: string;
  cluster: string;
  description: string;
}

function parseDataProducts(raw: string): DataProduct[] {
  try {
    const rows: unknown[][] = JSON.parse(raw);
    return rows.map((row) => ({
      objectName: String(row[0] ?? ""),
      cluster: String(row[1] ?? ""),
      description: String(row[2] ?? ""),
    }));
  } catch {
    return [];
  }
}

const dataProductsQueryKeys = {
  all: () =>
    [
      ...buildRegionQueryKey("dataProducts"),
      buildQueryKeyPart("dataProducts"),
    ] as const,
};

function useDataProducts() {
  return useSuspenseQuery({
    queryKey: dataProductsQueryKeys.all(),
    queryFn: async () => {
      const raw = await mcpQuerySystemCatalog(
        `SELECT object_name, cluster, description FROM mz_internal.mz_mcp_data_products ORDER BY object_name`,
      );
      return parseDataProducts(raw);
    },
    refetchInterval: 60_000,
  });
}

const DataProductsContent = () => {
  const { data: products } = useDataProducts();
  const { colors } = useTheme<MaterializeTheme>();

  if (products.length === 0) {
    return (
      <VStack padding="4" spacing="1" alignItems="flex-start">
        <Text textStyle="text-ui-med" color={colors.foreground.secondary}>
          No data products registered.
        </Text>
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          Add a COMMENT to an index to register it as an MCP data product.
        </Text>
      </VStack>
    );
  }

  return (
    <Box width="100%" overflowX="auto">
      <Table variant="linkable" borderRadius="xl">
        <Thead>
          <Tr>
            <Th>Data Product</Th>
            <Th>Cluster</Th>
            <Th>Description</Th>
          </Tr>
        </Thead>
        <Tbody>
          {products.map((product, idx) => (
            <Tr key={idx}>
              <Td {...truncateMaxWidth} py="2">
                <Text textStyle="text-ui-med" noOfLines={1}>
                  {product.objectName}
                </Text>
              </Td>
              <Td py="2">
                <Text
                  textStyle="text-small"
                  color={colors.foreground.secondary}
                >
                  {product.cluster}
                </Text>
              </Td>
              <Td {...truncateMaxWidth} py="2">
                <Text
                  textStyle="text-small"
                  color={colors.foreground.secondary}
                  noOfLines={2}
                >
                  {product.description}
                </Text>
              </Td>
            </Tr>
          ))}
        </Tbody>
      </Table>
    </Box>
  );
};

const DataProducts = () => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <VStack
      alignItems="flex-start"
      width="100%"
      gap="0"
      borderRadius="lg"
      borderWidth="1px"
      spacing="0"
    >
      <VStack
        alignItems="flex-start"
        gap="1"
        width="100%"
        borderBottomWidth="1px"
        borderColor={colors.border.secondary}
        padding="4"
      >
        <Text textStyle="heading-md" color={colors.foreground.primary}>
          MCP Data Products
        </Text>
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          Materialized views registered as data products, accessible to AI
          agents via the MCP server.
        </Text>
      </VStack>
      <AppErrorBoundary
        message="Unable to fetch data products from MCP agents endpoint. The endpoint may be disabled."
        containerProps={{ padding: "4" }}
      >
        <React.Suspense
          fallback={
            <Box height="80px" width="100%">
              <LoadingContainer />
            </Box>
          }
        >
          <DataProductsContent />
        </React.Suspense>
      </AppErrorBoundary>
    </VStack>
  );
};

export default DataProducts;
