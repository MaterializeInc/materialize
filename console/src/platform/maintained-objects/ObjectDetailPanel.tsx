// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Text, useTheme, VStack } from "@chakra-ui/react";
import React from "react";
import { useNavigate } from "react-router-dom";

import Alert from "~/components/Alert";
import { MaterializeTheme } from "~/theme";

import { DependenciesSection } from "./DependenciesSection";
import { ObjectDetailsCard } from "./ObjectDetailsCard";
import { ObjectFreshnessChart } from "./ObjectFreshnessChart";
import { ObjectMemoryCard } from "./ObjectMemoryCard";
import { useObjectColumns, useObjectDetail, useObjectLag, useObjectMemory } from "./queries";
import { MaintainedObjectListRow } from "./types";

export interface ObjectDetailPanelProps {
  object: MaintainedObjectListRow;
}

export const ObjectDetailPanel = ({ object }: ObjectDetailPanelProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const navigate = useNavigate();

  const {
    data: detail,
    isError: detailError,
  } = useObjectDetail(object.id);

  const { data: liveLag } = useObjectLag(object.id);

  const { data: memory } = useObjectMemory({
    objectId: object.id,
    clusterName: detail?.clusterName ?? undefined,
    replicaName: detail?.replicaName ?? undefined,
    objectType: detail?.objectType,
  });

  const { data: columns } = useObjectColumns(object.id);

  return (
    <Box p={4}>
      <VStack align="start" spacing={6} width="100%">
        {detailError && (
          <Alert
            variant="error"
            message="Failed to load object details. Please try refreshing the page."
            width="100%"
          />
        )}

        <ObjectDetailsCard
          object={{
            ...(detail ? { ...object, ...detail } : object),
            lag: liveLag?.lag ?? object.lag,
            lagMs: liveLag?.lagMs ?? object.lagMs,
          }}
          replicaName={detail?.replicaName ?? null}
          replicaSize={detail?.replicaSize ?? null}
          clusterManaged={detail?.clusterManaged ?? null}
          memoryBytes={memory?.memoryBytes ?? null}
          replicaTotalMemoryBytes={detail?.replicaTotalMemoryBytes ?? null}
        />

        <ObjectMemoryCard
          memoryBytes={memory?.memoryBytes ?? null}
          replicaTotalMemoryBytes={detail?.replicaTotalMemoryBytes ?? null}
          replicaName={detail?.replicaName ?? null}
          replicaSize={detail?.replicaSize ?? null}
        />

        <ObjectFreshnessChart objectId={object.id} />

        <DependenciesSection
          objectId={object.id}
          objectType={object.objectType}
          lagMs={liveLag?.lagMs ?? object.lagMs}
          onObjectClick={(id) => navigate(`../${id}`, { relative: "path" })}
        />

        {columns && columns.length > 0 && (
          <Box width="100%">
            <Text textStyle="heading-sm" mb={3}>
              Columns
            </Text>
            <Box
              borderRadius="md"
              border="1px"
              borderColor={colors.border.primary}
              overflow="hidden"
            >
              <Box as="table" width="100%">
                <Box as="thead" bg={colors.background.secondary}>
                  <Box as="tr">
                    <Box as="th" px={4} py={2} textAlign="left">
                      <Text textStyle="text-small-heavy">Column name</Text>
                    </Box>
                    <Box as="th" px={4} py={2} textAlign="left">
                      <Text textStyle="text-small-heavy">Type</Text>
                    </Box>
                    <Box as="th" px={4} py={2} textAlign="left">
                      <Text textStyle="text-small-heavy">Nullable</Text>
                    </Box>
                  </Box>
                </Box>
                <Box as="tbody">
                  {columns.map((col) => (
                    <Box
                      as="tr"
                      key={col.name}
                      borderTop="1px"
                      borderColor={colors.border.primary}
                    >
                      <Box as="td" px={4} py={2}>
                        <Text textStyle="text-ui-med">{col.name}</Text>
                      </Box>
                      <Box as="td" px={4} py={2}>
                        <Text textStyle="text-ui-reg">{col.type}</Text>
                      </Box>
                      <Box as="td" px={4} py={2}>
                        <Text textStyle="text-ui-reg">
                          {col.nullable ? "true" : "false"}
                        </Text>
                      </Box>
                    </Box>
                  ))}
                </Box>
              </Box>
            </Box>
          </Box>
        )}
      </VStack>
    </Box>
  );
};

export default ObjectDetailPanel;
