// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ChevronDownIcon, ChevronRightIcon } from "@chakra-ui/icons";
import {
  Box,
  HStack,
  Link,
  Spinner,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React, { useMemo, useState } from "react";

import type { DatabaseObject } from "~/api/materialize/objects";
import type { SupportedObjectType } from "~/api/materialize/types";
import { objectIcon } from "~/components/objectIcons";
import { NULL_DATABASE_NAME } from "~/platform/constants";
import { useAllObjects } from "~/store/allObjects";
import { useAllSchemas } from "~/store/allSchemas";
import { useObjectColumns } from "~/store/catalogColumns";
import type { MaterializeTheme } from "~/theme";

import type { CatalogSelection } from "./CatalogPanel";

/** Object types hidden from the tree (shown only in detail view). */
const HIDDEN_TYPES = new Set(["index"]);

/** The default database auto-expanded when the catalog tree first renders. */
const DEFAULT_EXPANDED_DATABASE = "materialize";

/** Returns a new Set with the given key toggled (added if absent, removed if present). */
const toggleSetEntry = (set: Set<string>, key: string): Set<string> => {
  const next = new Set(set);
  if (next.has(key)) next.delete(key);
  else next.add(key);
  return next;
};

/** Object types that have columns to preview. */
const PREVIEWABLE_TYPES = new Set([
  "table",
  "view",
  "materialized-view",
  "source",
]);

/** A schema and its visible objects within the catalog tree. */
interface SchemaGroup {
  schemaName: string;
  objects: DatabaseObject[];
}

/** A database and its schemas within the catalog tree. */
interface DatabaseGroup {
  databaseName: string;
  schemas: SchemaGroup[];
}

/** Props for the {@link CatalogTree} component. */
export interface CatalogTreeProps {
  /** Text filter applied to object names. When non-empty, all databases and schemas auto-expand. */
  search: string;
  /** Called when the user requests the full detail view for a catalog object. */
  onViewDetail: (obj: CatalogSelection) => void;
}

/**
 * Three-level browse tree: Database → Schema → Object.
 * Expanding an object shows a lightweight column preview (name + type).
 * "View full detail →" drills into the detail view.
 */
const CatalogTree = ({ search, onViewDetail }: CatalogTreeProps) => {
  const { data, snapshotComplete, isError } = useAllObjects();
  const { data: schemas, snapshotComplete: schemasReady } = useAllSchemas({
    includeSystemSchemas: false,
  });
  const { colors } = useTheme<MaterializeTheme>();

  const [expandedDatabases, setExpandedDatabases] = useState<Set<string>>(
    new Set([DEFAULT_EXPANDED_DATABASE]),
  );
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(
    new Set(),
  );
  const [expandedObject, setExpandedObject] = useState<string | null>(null);

  const tree = useMemo(() => {
    const lowerSearch = search.toLowerCase();

    const filtered = data.filter((obj) => {
      if (!obj.schemaId || !obj.schemaName) return false;
      if (HIDDEN_TYPES.has(obj.objectType)) return false;
      if (lowerSearch && !obj.name.toLowerCase().includes(lowerSearch))
        return false;
      return true;
    });

    const dbMap = new Map<string, Map<string, DatabaseObject[]>>();

    // Seed with all user schemas so empty databases/schemas appear.
    if (!lowerSearch) {
      for (const schema of schemas) {
        if (!schema.databaseName) continue;
        let schemaMap = dbMap.get(schema.databaseName);
        if (!schemaMap) {
          schemaMap = new Map();
          dbMap.set(schema.databaseName, schemaMap);
        }
        if (!schemaMap.has(schema.name)) {
          schemaMap.set(schema.name, []);
        }
      }
    }

    for (const obj of filtered) {
      const dbName = obj.databaseName ?? "System catalog";
      const schName = obj.schemaName ?? "";
      let schemaMap = dbMap.get(dbName);
      if (!schemaMap) {
        schemaMap = new Map();
        dbMap.set(dbName, schemaMap);
      }
      let objs = schemaMap.get(schName);
      if (!objs) {
        objs = [];
        schemaMap.set(schName, objs);
      }
      objs.push(obj);
    }

    const result: DatabaseGroup[] = [];
    for (const [databaseName, schemaMap] of dbMap) {
      const schemaGroups: SchemaGroup[] = [];
      for (const [schemaName, objects] of schemaMap) {
        objects.sort((a, b) => a.name.localeCompare(b.name));
        schemaGroups.push({ schemaName, objects });
      }
      schemaGroups.sort((a, b) => a.schemaName.localeCompare(b.schemaName));
      result.push({ databaseName, schemas: schemaGroups });
    }
    result.sort((a, b) => a.databaseName.localeCompare(b.databaseName));
    return result;
  }, [data, schemas, search]);

  if (!snapshotComplete || !schemasReady) {
    return (
      <Box display="flex" justifyContent="center" py="4">
        <Spinner />
      </Box>
    );
  }

  if (isError) {
    return (
      <Box px="3" py="2">
        <Text color={colors.foreground.secondary}>
          An error occurred loading objects.
        </Text>
      </Box>
    );
  }

  return (
    <VStack align="stretch" spacing="0">
      {tree.map((db) => {
        const dbExpanded = search
          ? true
          : expandedDatabases.has(db.databaseName);
        return (
          <React.Fragment key={db.databaseName}>
            {/* Database row */}
            <HStack
              px="3"
              py="2"
              cursor="pointer"
              _hover={{ bg: colors.background.secondary }}
              onClick={() =>
                setExpandedDatabases((s) => toggleSetEntry(s, db.databaseName))
              }
              spacing="1"
            >
              {dbExpanded ? (
                <ChevronDownIcon boxSize="4" />
              ) : (
                <ChevronRightIcon boxSize="4" />
              )}
              {objectIcon("database")}
              <Text fontWeight="bold" fontSize="xs">
                {db.databaseName}
              </Text>
            </HStack>

            {dbExpanded &&
              db.schemas.map((schema) => {
                const schemaKey = `${db.databaseName}.${schema.schemaName}`;
                const schemaExpanded = search
                  ? true
                  : expandedSchemas.has(schemaKey);
                return (
                  <React.Fragment key={schemaKey}>
                    {/* Schema row */}
                    <HStack
                      pl="5"
                      pr="3"
                      py="2"
                      cursor="pointer"
                      _hover={{ bg: colors.background.secondary }}
                      onClick={() =>
                        setExpandedSchemas((s) => toggleSetEntry(s, schemaKey))
                      }
                      spacing="1"
                    >
                      {schemaExpanded ? (
                        <ChevronDownIcon boxSize="3" />
                      ) : (
                        <ChevronRightIcon boxSize="3" />
                      )}
                      {objectIcon("schema")}
                      <Text
                        fontSize="xs"
                        fontWeight="bold"
                        textTransform="uppercase"
                        letterSpacing="wide"
                        color={colors.foreground.secondary}
                      >
                        {schema.schemaName}
                      </Text>
                    </HStack>

                    {schemaExpanded &&
                      schema.objects.map((obj) => {
                        const isExpanded = expandedObject === obj.id;
                        const canPreview = PREVIEWABLE_TYPES.has(
                          obj.objectType,
                        );
                        const selection: CatalogSelection = {
                          id: obj.id,
                          databaseName: obj.databaseName ?? NULL_DATABASE_NAME,
                          schemaName: schema.schemaName,
                          objectName: obj.name,
                          objectType: obj.objectType as SupportedObjectType,
                          clusterId: obj.clusterId ?? undefined,
                          clusterName: obj.clusterName ?? undefined,
                        };
                        return (
                          <React.Fragment key={obj.id}>
                            {/* Object row */}
                            <HStack
                              pl="8"
                              pr="3"
                              py="2"
                              cursor="pointer"
                              _hover={{
                                bg: colors.background.accent,
                              }}
                              bg={
                                isExpanded
                                  ? colors.background.secondary
                                  : undefined
                              }
                              onClick={() => {
                                if (canPreview) {
                                  setExpandedObject((prev) =>
                                    prev === obj.id ? null : obj.id,
                                  );
                                } else {
                                  onViewDetail(selection);
                                }
                              }}
                              spacing="1"
                            >
                              {canPreview ? (
                                isExpanded ? (
                                  <ChevronDownIcon boxSize="3" />
                                ) : (
                                  <ChevronRightIcon boxSize="3" />
                                )
                              ) : (
                                <Box boxSize="3" />
                              )}
                              {objectIcon(selection.objectType)}
                              <Text fontSize="xs">{obj.name}</Text>
                            </HStack>

                            {/* Inline column preview */}
                            {isExpanded && canPreview && (
                              <InlineColumnPreview
                                objectId={obj.id}
                                onViewDetail={() => onViewDetail(selection)}
                              />
                            )}
                          </React.Fragment>
                        );
                      })}
                  </React.Fragment>
                );
              })}
          </React.Fragment>
        );
      })}
    </VStack>
  );
};

/** Lightweight column preview shown when an object is expanded in the tree. */
const InlineColumnPreview = ({
  objectId,
  onViewDetail,
}: {
  objectId: string;
  onViewDetail: () => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const columns = useObjectColumns(objectId);

  return (
    <>
      <VStack align="stretch" spacing="0" pl="12" pr="3">
        {columns.map((col) => (
          <HStack key={col.name} justifyContent="space-between" py="0.5">
            <Text fontSize="xs" color={colors.foreground.secondary}>
              {col.name}
            </Text>
            <Text fontSize="xs" color={colors.foreground.secondary}>
              {col.type}
            </Text>
          </HStack>
        ))}
      </VStack>
      <Box pl="12" pb="2">
        <Link
          fontSize="xs"
          color={colors.accent.purple}
          onClick={(e) => {
            e.stopPropagation();
            onViewDetail();
          }}
        >
          View full detail &rarr;
        </Link>
      </Box>
    </>
  );
};

export { CatalogTree };
