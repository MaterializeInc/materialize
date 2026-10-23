// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Divider,
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
import { Link as RouterLink, useOutletContext } from "react-router-dom";

import TextLink from "~/components/TextLink";
import { MaterializeTheme } from "~/theme";
import { truncateMaxWidth } from "~/theme/components/Table";

import { FreshnessCell, StatusCell } from "./MaintainedObjects";
import { MaintainedObjectsOutletContext } from "./MaintainedObjectsLayout";
import { MaintainedObjectListItem } from "./queries";
import { useJourneyLinkState } from "./useJourneyLinkState";
import { useSourceExports } from "./useSourceExports";

const sectionTitle = (tables: MaintainedObjectListItem[]) => {
  const hasSubsources = tables.some((t) => t.objectType === "subsource");
  const hasTables = tables.some((t) => t.objectType === "table");
  if (hasSubsources && hasTables) return "Subsources and tables";
  return hasSubsources ? "Subsources" : "Tables";
};

/** Worst freshness first; objects without lag history sort last. */
const byLagDesc = (a: MaintainedObjectListItem, b: MaintainedObjectListItem) =>
  (b.lag?.ms ?? -1) - (a.lag?.ms ?? -1) || a.name.localeCompare(b.name);

export interface SourceTablesFreshnessProps {
  sourceId: string;
}

/**
 * Freshness of each subsource and table a source ingests into, over the same
 * lookback window as the list, as a section of the freshness card. Renders
 * nothing, divider included, for a source without any.
 */
export const SourceTablesFreshness = ({
  sourceId,
}: SourceTablesFreshnessProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { lagReady, hydrationReady } =
    useOutletContext<MaintainedObjectsOutletContext>();
  const journeyLinkState = useJourneyLinkState(sourceId);
  const exports = useSourceExports(sourceId);
  const tables = React.useMemo(() => [...exports].sort(byLagDesc), [exports]);
  if (tables.length === 0) return null;

  return (
    <>
      <Divider />
      <VStack align="start" spacing={3} width="100%">
        <VStack align="start" spacing={1}>
          <Text textStyle="heading-sm">{sectionTitle(tables)}</Text>
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            The source&apos;s own freshness tracks how recently it read the
            upstream&apos;s position. Each subsource or table is only as fresh
            as the data written to it, so it can lag behind the source.
          </Text>
        </VStack>
        <Table variant="standalone" borderRadius="xl">
          <Thead>
            <Tr>
              <Th>Name</Th>
              <Th>Freshness (pMAX)</Th>
              <Th>Status</Th>
            </Tr>
          </Thead>
          <Tbody>
            {tables.map((t) => (
              <Tr key={t.id}>
                <Td {...truncateMaxWidth}>
                  <TextLink
                    as={RouterLink}
                    to={`../${t.id}`}
                    relative="path"
                    state={journeyLinkState}
                    textStyle="text-ui-med"
                    noOfLines={1}
                  >
                    {t.name}
                  </TextLink>
                </Td>
                <Td>
                  <FreshnessCell row={t} ready={lagReady} />
                </Td>
                <Td>
                  <StatusCell row={t} hydrationReady={hydrationReady} />
                </Td>
              </Tr>
            ))}
          </Tbody>
        </Table>
      </VStack>
    </>
  );
};

export default SourceTablesFreshness;
