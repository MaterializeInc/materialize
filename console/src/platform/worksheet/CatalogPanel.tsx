// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ArrowBackIcon, SearchIcon } from "@chakra-ui/icons";
import {
  Box,
  HStack,
  IconButton,
  Input,
  InputGroup,
  InputLeftElement,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useAtom } from "jotai";
import React, { useCallback, useState } from "react";

import { NULL_DATABASE_NAME } from "~/platform/object-explorer/constants";
import type { SupportedObjectType } from "~/platform/object-explorer/ObjectExplorerNode";
import { catalogDetailAtom } from "~/store/catalog";
import type { MaterializeTheme } from "~/theme";

import { CatalogDetailView } from "./CatalogObjectDetail";
import { CatalogTree } from "./CatalogTree";

/**
 * Identifies a catalog object selected for detail viewing.
 * Passed between CatalogTree, CatalogPanel, and CatalogDetailView.
 */
export interface CatalogSelection {
  /** The mz_objects.id of the selected catalog object (e.g. "u123"). */
  id: string;
  /** Database the object belongs to (e.g. "materialize"). */
  databaseName: string;
  /** Schema the object belongs to (e.g. "public"). */
  schemaName: string;
  /** Unqualified name of the object. */
  objectName: string;
  /** The kind of catalog object (table, view, source, etc.). */
  objectType: SupportedObjectType;
  /** Cluster ID, present only for objects bound to a cluster. */
  clusterId?: string;
  /** Cluster name, present only for objects bound to a cluster. */
  clusterName?: string;
}

/**
 * Two-mode catalog panel: browse tree or drill-in detail.
 * Rendered in BaseLayout alongside the nav bar when the catalog toggle is active.
 * Search bar at top, content below. Clicking "View full detail" in
 * the tree switches to detail mode; back button returns to browse.
 */
const CatalogPanel = () => {
  const { colors } = useTheme<MaterializeTheme>();
  const [search, setSearch] = useState("");
  const [detailObject, setDetailObject] = useAtom(catalogDetailAtom);

  const handleViewDetail = useCallback(
    (obj: CatalogSelection) => {
      setDetailObject(obj);
    },
    [setDetailObject],
  );

  const handleBack = useCallback(() => {
    setDetailObject(undefined);
  }, [setDetailObject]);

  return (
    <VStack
      height="100%"
      spacing={0}
      borderRightWidth="1px"
      borderColor={colors.border.secondary}
      bg={colors.background.secondary}
      overflow="hidden"
    >
      {detailObject && (
        <>
          <HStack px="3" py="2" width="100%" flexShrink={0}>
            <IconButton
              icon={<ArrowBackIcon />}
              aria-label="Back to catalog"
              onClick={handleBack}
              variant="ghost"
              size="sm"
            />
            <Text
              fontSize="sm"
              color={colors.foreground.secondary}
              noOfLines={1}
            >
              {detailObject.databaseName &&
              detailObject.databaseName !== NULL_DATABASE_NAME
                ? `${detailObject.databaseName}.${detailObject.schemaName}`
                : detailObject.schemaName}
            </Text>
          </HStack>
          <Box flex="1" overflow="auto" width="100%">
            <CatalogDetailView
              id={detailObject.id}
              databaseName={detailObject.databaseName}
              schemaName={detailObject.schemaName}
              objectName={detailObject.objectName}
              objectType={detailObject.objectType}
              clusterId={detailObject.clusterId}
              clusterName={detailObject.clusterName}
              onNavigate={handleViewDetail}
            />
          </Box>
        </>
      )}
      {/* Keep tree mounted but hidden so expansion state is preserved */}
      <Box
        display={detailObject ? "none" : "flex"}
        flexDirection="column"
        width="100%"
        overflow="hidden"
      >
        <Box px="3" py="3" width="100%" flexShrink={0}>
          <InputGroup size="sm">
            <InputLeftElement pointerEvents="none" height="100%">
              <SearchIcon color={colors.foreground.secondary} boxSize="3" />
            </InputLeftElement>
            <Input
              pl="8"
              placeholder="Search objects..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
            />
          </InputGroup>
        </Box>
        <Box flex="1" overflow="auto" width="100%">
          <CatalogTree search={search} onViewDetail={handleViewDetail} />
        </Box>
      </Box>
    </VStack>
  );
};

export default CatalogPanel;
