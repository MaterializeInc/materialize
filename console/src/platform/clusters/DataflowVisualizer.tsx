// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Alert,
  AlertIcon,
  Box,
  Button,
  HStack,
  Spinner,
  Text,
  VStack,
} from "@chakra-ui/react";
import * as d3 from "d3";
import { graphviz } from "d3-graphviz";
import React from "react";
import { useParams } from "react-router-dom";

import { Replica } from "~/api/materialize/cluster/clusterList";
import { ErrorCode } from "~/api/materialize/types";
import { useDataflowStructure } from "~/api/materialize/useDataflowStructure";
import ErrorBox from "~/components/ErrorBox";
import LabeledSelect from "~/components/LabeledSelect";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { useAllClusters } from "~/store/allClusters";
import { useAllObjects } from "~/store/allObjects";
import { assert } from "~/util";

import { collateOperators, scopeToGv } from "./dataflowGraph";

interface DotVizProps {
  dot?: string;
  onClickedNode: (id: string) => void;
}

const DotViz = ({ dot, onClickedNode }: DotVizProps) => {
  const d3Container = React.useRef(null);
  React.useEffect(() => {
    if (d3Container.current && dot) {
      const gv = graphviz(d3Container.current)
        .scale(0.5)
        .attributer(function (d) {
          if (d.tag === "svg") {
            d.attributes.width = "100%";
            d.attributes.height = "100%";
          }
        });
      gv.on("initEnd", () => {
        gv.renderDot(dot, function () {
          gv.resetZoom();

          const regions = d3.selectAll(".region");
          regions.on("dblclick", function (event) {
            const clickedId = event.currentTarget.getAttribute("id")!;
            if (clickedId) {
              event.stopPropagation();
              onClickedNode(clickedId);
            }
          });
        });
      });
    }
  }, [dot, onClickedNode]);
  return <Box width="100%" flex="1" ref={d3Container} />;
};

const defaultReplicas: Replica[] = [];

const DataflowVisualizer = () => {
  const { getClusterById } = useAllClusters();
  const params = useParams();
  const { data: allObjects } = useAllObjects();
  const object = allObjects.find((o) => o.id === params.id);
  assert(object && object.clusterId);
  const cluster = getClusterById(object.clusterId);
  const replicas = cluster?.replicas ?? defaultReplicas;
  const [scopeBreadcrumb, setScopeBreadcrumb] = React.useState<string[]>([]);
  const [replicaName, setReplicaName] = React.useState<string | null>(null);

  React.useEffect(() => {
    if (replicas.length > 0 && replicaName === null) {
      setReplicaName(replicas[0].name);
    }
  }, [replicaName, replicas]);

  // Reset scope if props changed.
  // TODO - If we track scopes by
  // address, rather than by operator ID, we can avoid resetting it
  // when replica name changes (addresses other than the initial component
  // are the same across replicas, whereas operator IDs aren't
  React.useEffect(() => {
    setScopeBreadcrumb([]);
  }, [params.id, replicaName]);
  const dfStructureParams = React.useMemo(
    () =>
      cluster && replicaName
        ? { clusterName: cluster.name, replicaName, objectId: object.id }
        : undefined,
    [cluster, object.id, replicaName],
  );
  const {
    results: structure,
    failedToLoad,
    databaseError,
    loading,
  } = useDataflowStructure(dfStructureParams);
  const [allEnriched, root, lirOperators] = React.useMemo(() => {
    return structure && structure.operators.length > 0
      ? collateOperators(
          structure.operators,
          structure.channels,
          structure.lir_operators,
        )
      : [null, null, []];
  }, [structure]);
  const dot = React.useMemo(() => {
    const scopeOperator =
      scopeBreadcrumb.length > 0 && allEnriched
        ? allEnriched.get(scopeBreadcrumb[scopeBreadcrumb.length - 1])!
        : root;
    return scopeOperator ? scopeToGv(scopeOperator, lirOperators) : undefined;
  }, [allEnriched, root, lirOperators, scopeBreadcrumb]);

  const pushScope = React.useCallback(
    (s: string) => {
      const newBreadcrumb = [...scopeBreadcrumb, s];
      setScopeBreadcrumb(newBreadcrumb);
    },
    [scopeBreadcrumb, setScopeBreadcrumb],
  );

  if (!cluster) return null;

  const permissionError =
    databaseError &&
    "code" in databaseError &&
    databaseError.code === ErrorCode.INSUFFICIENT_PRIVILEGE;
  return (
    <MainContentContainer alignItems="center" width="100%">
      {permissionError ? (
        <Alert status="info" rounded="md" p={4} marginTop={2} width="auto">
          <AlertIcon />
          <Text>
            You&apos;ll need{" "}
            <Text as="span" textStyle="monospace">
              USAGE
            </Text>{" "}
            privilege on this cluster to visualize this dataflow.
          </Text>
        </Alert>
      ) : failedToLoad ? (
        <ErrorBox message="There was an error visualizing your dataflow" />
      ) : loading ? (
        <Spinner />
      ) : (
        <VStack width="100%" height="100%">
          {replicaName && (
            <LabeledSelect
              label="Replicas"
              value={replicaName}
              onChange={(e) => setReplicaName(e.target.value)}
              flexShrink={0}
            >
              {cluster.replicas.map((r) => (
                <option key={r.name} value={r.name}>
                  {r.name}
                </option>
              ))}
            </LabeledSelect>
          )}
          <HStack>
            <Button
              size="xs"
              onClick={() => setScopeBreadcrumb([])}
              isDisabled={scopeBreadcrumb.length === 0}
            >
              {"<<"}
            </Button>{" "}
            <Button
              size="xs"
              onClick={() => setScopeBreadcrumb(scopeBreadcrumb.slice(0, -1))}
              isDisabled={scopeBreadcrumb.length === 0}
            >
              {"<"}
            </Button>
          </HStack>
          {dot === undefined ? (
            <Text>This dataflow contains no operators.</Text>
          ) : (
            <DotViz dot={dot} onClickedNode={pushScope} />
          )}
        </VStack>
      )}
    </MainContentContainer>
  );
};

export default DataflowVisualizer;
