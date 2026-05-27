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
import {
  Channel,
  LirOperator,
  Operator,
  useDataflowStructure,
} from "~/api/materialize/useDataflowStructure";
import ErrorBox from "~/components/ErrorBox";
import LabeledSelect from "~/components/LabeledSelect";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { useAllClusters } from "~/store/allClusters";
import { useAllObjects } from "~/store/allObjects";
import { assert } from "~/util";
import { formatBytesShort } from "~/utils/format";

interface EnrichedOperator extends Operator {
  // First-level.
  channelsInScope: Channel[];
  children: EnrichedOperator[];
  transitiveArrangementRecords: bigint | null;
  transitiveArrangementSizes: bigint | null;
}

type EnrichedLirOperator = {
  lir_id: string;
  operator: string;
  addresses: string[];
};

function groupBy<T, K>(values: T[], group: (item: T) => K): Map<K, T[]> {
  const output = new Map();
  for (const v of values) {
    const k = group(v);
    if (!output.has(k)) {
      output.set(k, []);
    }
    output.get(k)!.push(v);
  }
  return output;
}

// Returns a map of (stringified) operator address to corresponding operators,
// as well as a designated root.
function collateOperators(
  operators: Operator[],
  channels: Channel[],
  lirOperators: LirOperator[],
): [Map<string, EnrichedOperator>, EnrichedOperator, EnrichedLirOperator[]] {
  const scopes = groupBy(operators, (o) => o.parentId);
  const channelsByParentScope = groupBy(channels, (ch) =>
    stringifyAddress(ch.fromOperatorAddress.slice(0, -1)),
  );

  const roots = scopes.get(null) || [];

  function walk(
    op: Operator,
    m: Map<string, EnrichedOperator>,
  ): EnrichedOperator {
    assert(!m.has(stringifyAddress(op.address)));
    const children = (scopes.get(op.id) || []).map((ch) => walk(ch, m));
    const channelsInScope =
      channelsByParentScope.get(stringifyAddress(op.address)) || [];
    const ret = {
      ...op,
      children,
      channelsInScope,
      transitiveArrangementRecords:
        children
          .map((child) => child.transitiveArrangementRecords || 0n)
          .reduce((a, b) => a + b, 0n) + (op.arrangementRecords || 0n),
      transitiveArrangementSizes:
        children
          .map((child) => child.transitiveArrangementSizes || 0n)
          .reduce((a, b) => a + b, 0n) + (op.arrangementSizes || 0n),
    };
    m.set(stringifyAddress(ret.address), ret);
    return ret;
  }

  const enrichedLirOperators: EnrichedLirOperator[] = lirOperators.map(
    (lirOp) => ({
      lir_id: lirOp.lir_id,
      operator: lirOp.operator,
      addresses: lirOp.addresses.map((addr) => stringifyAddress(addr)),
    }),
  );

  const m = new Map();

  const enrichedRoots = roots.map((r) => walk(r, m));
  assert(enrichedRoots.length == 1);
  return [m, enrichedRoots[0], enrichedLirOperators];
}

const noArrangementRegionColor = "#12b886";
const noArrangementOperatorColor = "#ffffff";
const arrangementRegionColor = "#7950f2";
const arrangementOperatorColor = "#fab005";

function stringifyAddress(address: string[]) {
  return JSON.stringify(address.map((val) => parseInt(val)));
}

function scopeToGv(
  scope: EnrichedOperator,
  lir_operators: EnrichedLirOperator[],
): string {
  const chunks = ["digraph {", 'node [style="filled",shape=box];'];
  const addresses = new Set<string>();
  for (const op of scope.children) {
    const isRegion = op.children.length !== 0;
    const hasArrangedData = (op.transitiveArrangementRecords || 0n) > 0n;
    let fillColor;
    if (isRegion) {
      if (hasArrangedData) {
        fillColor = arrangementRegionColor;
      } else {
        fillColor = noArrangementRegionColor;
      }
    } else {
      if (hasArrangedData) {
        fillColor = arrangementOperatorColor;
      } else {
        fillColor = noArrangementOperatorColor;
      }
    }
    const nodeLabelFields = [op.name];
    if (hasArrangedData) {
      nodeLabelFields.push(
        `${op.transitiveArrangementRecords} arranged records`,
      );
      nodeLabelFields.push(
        formatBytesShort(BigInt(op.transitiveArrangementSizes || 0)),
      );
    }
    if (op.elapsedNs > 0) {
      nodeLabelFields.push(
        `scheduled ${Math.round(op.elapsedNs / 1_000_000_000)}s`,
      );
    }

    const tooltip = `Lir ID ${op.lirId}: ${op.lirOperator}`.replace(
      /"/g,
      '\\"',
    );

    const opAddressString = stringifyAddress(op.address);
    addresses.add(opAddressString);

    const nodeGv = `"${opAddressString}" [fillcolor="${fillColor}",tooltip="${tooltip}",id="${opAddressString}",label="${nodeLabelFields.join("\n").replace(/"/g, '\\"')}",class="${isRegion ? "region" : ""}"];`;
    chunks.push(nodeGv);
  }
  const pseudoOperators = new Map();
  for (const ch of scope.channelsInScope) {
    let fromAddressKey = stringifyAddress(ch.fromOperatorAddress);
    let toAddressKey = stringifyAddress(ch.toOperatorAddress);

    if (ch.fromOperatorAddress[ch.fromOperatorAddress.length - 1] === "0") {
      fromAddressKey = `${fromAddressKey}:${ch.fromPort}:FROM`;
      pseudoOperators.set(fromAddressKey, `input ${ch.fromPort}`);
    }
    if (ch.toOperatorAddress[ch.toOperatorAddress.length - 1] === "0") {
      toAddressKey = `${toAddressKey}:${ch.toPort}:TO`;
      pseudoOperators.set(toAddressKey, `output ${ch.toPort}`);
    }
    const chanAttr = ch.messagesSent === 0 ? `,style="dashed"` : "";
    const chanLabel = `${ch.messagesSent > 0 ? `${ch.messagesSent} records` : ""}${ch.batchesSent > 0 ? `\n${ch.batchesSent} batches` : ""}`;
    const tooltip = ch.channelType || "unknown channel type";
    const chanGv = `"${fromAddressKey}" -> "${toAddressKey}" [label="${chanLabel}",tooltip="${tooltip.replace(/"/g, '\\"')}"${chanAttr}];`;
    chunks.push(chanGv);
  }

  for (const [k, v] of pseudoOperators) {
    chunks.push(
      `"${k}" [fillcolor="lightgrey",id="${k}",label="${v.replace(/"/g, '\\"')}"]`,
    );
  }

  for (const lir_operator of lir_operators) {
    if (!lir_operator.addresses.some((addr) => addresses.has(addr))) continue;
    chunks.push("subgraph cluster_" + lir_operator.lir_id + " {");
    chunks.push(
      `label="LIR ID ${lir_operator.lir_id}: ${lir_operator.operator.replace(/"/g, '\\"')}";`,
    );
    for (const addr of lir_operator.addresses) {
      chunks.push(`"${addr}";`);
    }
    chunks.push("}");
  }

  chunks.push("}");
  const ret = chunks.join("\n");
  return ret;
}

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
