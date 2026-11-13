// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useTheme } from "@chakra-ui/react";
import React from "react";

import { Canvas } from "~/components/Graph";
import { MaterializeTheme, ThemeColors } from "~/theme";

import { Freshness, freshnessColor, freshnessText } from "./freshness";
import { GraphLayout, labelWidth, NODE_HEIGHT, NODE_WIDTH } from "./layout";
import { edgeLabel, isErrorEdge, OntologyEdge, OntologyNode } from "./model";
import { edgeKindStyle, groupColor, nodeKindLabel } from "./style";

export type Selection =
  { type: "node"; id: string } | { type: "edge"; id: string } | null;

export interface OntologyCanvasProps {
  nodes: OntologyNode[];
  edges: OntologyEdge[];
  layout: GraphLayout;
  allGroups: string[];
  selection: Selection;
  onSelect: (selection: Selection) => void;
  /** Nodes to emphasize, from a search or a semantic type. Null for none. */
  highlighted: ReadonlySet<string> | null;
  /** Nodes and edges that appeared after the first snapshot. */
  recentlyAdded: ReadonlySet<string>;
  showEdgeLabels: boolean;
  getFreshness: ((node: OntologyNode) => Freshness) | undefined;
  rightOffset: number;
  svgRef: React.RefObject<SVGSVGElement>;
}

const TITLE_CHARS = 24;
const SUBTITLE_CHARS = 18;

const truncate = (text: string, max: number) =>
  text.length > max ? `${text.slice(0, max - 1)}…` : text;

/** Arrowhead marker ids by stroke color. */
function markerId(color: string) {
  return `ontology-arrow-${color.replace(/[^a-zA-Z0-9]/g, "")}`;
}

export const OntologyCanvas = ({
  nodes,
  edges,
  layout,
  allGroups,
  selection,
  onSelect,
  highlighted,
  recentlyAdded,
  showEdgeLabels,
  getFreshness,
  rightOffset,
  svgRef,
}: OntologyCanvasProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [hoveredEdge, setHoveredEdge] = React.useState<string | null>(null);

  const edgesById = React.useMemo(
    () => new Map(edges.map((edge) => [edge.id, edge])),
    [edges],
  );
  // What the selection touches stays bright and everything else recedes.
  const focus = React.useMemo(() => {
    if (!selection) return null;
    const nodeIds = new Set<string>();
    const edgeIds = new Set<string>();
    if (selection.type === "node") {
      nodeIds.add(selection.id);
      for (const edge of edges) {
        if (edge.source === selection.id || edge.target === selection.id) {
          edgeIds.add(edge.id);
          nodeIds.add(edge.source);
          nodeIds.add(edge.target);
        }
      }
    } else {
      const edge = edgesById.get(selection.id);
      if (edge) {
        edgeIds.add(edge.id);
        nodeIds.add(edge.source);
        nodeIds.add(edge.target);
      }
    }
    return { nodeIds, edgeIds };
  }, [selection, edges, edgesById]);

  const edgeColors = React.useMemo(() => {
    const set = new Set<string>([colors.accent.red]);
    for (const edge of edges) set.add(edgeKindStyle(edge.kind, colors).color);
    return [...set];
  }, [edges, colors]);

  const selectedPosition =
    selection?.type === "node" ? layout.nodes.get(selection.id) : undefined;
  const selectedNode = selectedPosition
    ? { ...selectedPosition, width: NODE_WIDTH, height: NODE_HEIGHT }
    : null;

  return (
    <Canvas
      width={layout.width}
      height={layout.height}
      selectedNode={selectedNode}
      rightOffset={rightOffset}
      fitToContent
    >
      <svg
        ref={svgRef}
        xmlns="http://www.w3.org/2000/svg"
        width={layout.width}
        height={layout.height}
        viewBox={`0 0 ${layout.width} ${layout.height}`}
        fontFamily="Inter, sans-serif"
        onClick={(e) => {
          if (e.target === e.currentTarget) onSelect(null);
        }}
      >
        <defs>
          {edgeColors.map((color) => (
            <marker
              key={color}
              id={markerId(color)}
              viewBox="0 0 10 10"
              refX="9"
              refY="5"
              markerWidth="7"
              markerHeight="7"
              orient="auto-start-reverse"
            >
              <path d="M0,1 L9,5 L0,9 z" fill={color} />
            </marker>
          ))}
          <style>
            {`
              @keyframes ontology-appear {
                0% { stroke-width: 8; stroke-opacity: 0.9; }
                100% { stroke-width: 2; stroke-opacity: 1; }
              }
              @keyframes ontology-pulse {
                0%, 100% { opacity: 1; }
                50% { opacity: 0.35; }
              }
              .ontology-new { animation: ontology-appear 1.2s ease-out 3; }
              .ontology-hydrating { animation: ontology-pulse 1.4s ease-in-out infinite; }
              .ontology-edge-hit { cursor: pointer; }
              .ontology-node { cursor: pointer; }
            `}
          </style>
        </defs>

        {layout.groups.map((group) => {
          const color = groupColor(group.name, allGroups, colors);
          return (
            <g key={group.name} pointerEvents="none">
              <rect
                x={group.x}
                y={group.y}
                width={group.width}
                height={group.height}
                rx={12}
                fill={color}
                fillOpacity={0.05}
                stroke={color}
                strokeOpacity={0.35}
                strokeDasharray="4 4"
              />
              <text
                x={group.x + 14}
                y={group.y + 19}
                fontSize={12}
                fontWeight={600}
                fill={color}
              >
                {group.name}
              </text>
            </g>
          );
        })}

        {edges.map((edge) => {
          const position = layout.edges.get(edge.id);
          if (!position) return null;
          const style = edgeKindStyle(edge.kind, colors);
          const hasError = isErrorEdge(edge);
          const color = hasError ? colors.accent.red : style.color;
          const isFocused =
            focus?.edgeIds.has(edge.id) || hoveredEdge === edge.id;
          const isDimmed = focus !== null && !focus.edgeIds.has(edge.id);
          return (
            <g key={edge.id} opacity={isDimmed ? 0.15 : 1}>
              <path
                d={position.path}
                fill="none"
                stroke={color}
                strokeWidth={isFocused ? 3 : 1.75}
                strokeDasharray={hasError ? "5 4" : style.dash}
                markerEnd={`url(#${markerId(color)})`}
                className={
                  recentlyAdded.has(edge.id) ? "ontology-new" : undefined
                }
              />
              <path
                d={position.path}
                fill="none"
                stroke="transparent"
                strokeWidth={14}
                className="ontology-edge-hit"
                data-testid={`ontology-edge-${edge.id}`}
                onMouseEnter={() => setHoveredEdge(edge.id)}
                onMouseLeave={() => setHoveredEdge(null)}
                onClick={(e) => {
                  e.stopPropagation();
                  onSelect({ type: "edge", id: edge.id });
                }}
              >
                <title>{edgeLabel(edge) || "derives from"}</title>
              </path>
            </g>
          );
        })}

        {showEdgeLabels &&
          edges.map((edge) => {
            const position = layout.edges.get(edge.id);
            if (!position) return null;
            const label = edgeLabel(edge);
            if (!label) return null;
            const text = truncate(label, 40);
            const width = labelWidth(text);
            const hasError = isErrorEdge(edge);
            const color = hasError
              ? colors.accent.red
              : edgeKindStyle(edge.kind, colors).color;
            const isDimmed = focus !== null && !focus.edgeIds.has(edge.id);
            return (
              <g
                key={edge.id}
                opacity={isDimmed ? 0.15 : 1}
                className="ontology-edge-hit"
                onClick={(e) => {
                  e.stopPropagation();
                  onSelect({ type: "edge", id: edge.id });
                }}
              >
                <rect
                  x={position.labelX - width / 2}
                  y={position.labelY - 9}
                  width={width}
                  height={18}
                  rx={9}
                  fill={colors.background.primary}
                  stroke={color}
                  strokeOpacity={0.5}
                />
                <text
                  x={position.labelX}
                  y={position.labelY + 4}
                  textAnchor="middle"
                  fontSize={10.5}
                  fill={
                    hasError ? colors.accent.red : colors.foreground.secondary
                  }
                  fontFamily="'Roboto Mono', monospace"
                >
                  {text}
                </text>
              </g>
            );
          })}

        {nodes.map((node) => {
          const position = layout.nodes.get(node.id);
          if (!position) return null;
          return (
            <OntologyNodeCard
              key={node.id}
              node={node}
              x={position.x - NODE_WIDTH / 2}
              y={position.y - NODE_HEIGHT / 2}
              color={groupColor(node.group, allGroups, colors)}
              colors={colors}
              isSelected={
                selection?.type === "node" && selection.id === node.id
              }
              isDimmed={focus !== null && !focus.nodeIds.has(node.id)}
              isHighlighted={highlighted?.has(node.id) ?? false}
              isNew={recentlyAdded.has(node.id)}
              freshness={getFreshness?.(node)}
              onClick={() => onSelect({ type: "node", id: node.id })}
            />
          );
        })}
      </svg>
    </Canvas>
  );
};

interface OntologyNodeCardProps {
  node: OntologyNode;
  x: number;
  y: number;
  color: string;
  colors: ThemeColors;
  isSelected: boolean;
  isDimmed: boolean;
  isHighlighted: boolean;
  isNew: boolean;
  freshness: Freshness | undefined;
  onClick: () => void;
}

const OntologyNodeCard = React.memo(
  ({
    node,
    x,
    y,
    color,
    colors,
    isSelected,
    isDimmed,
    isHighlighted,
    isNew,
    freshness,
    onClick,
  }: OntologyNodeCardProps) => {
    const status = freshness ? freshnessText(freshness) : null;
    const border = node.isUnresolved
      ? colors.accent.red
      : isSelected
        ? colors.accent.brightPurple
        : isHighlighted
          ? colors.accent.orange
          : colors.border.secondary;
    return (
      <g
        transform={`translate(${x}, ${y})`}
        opacity={isDimmed ? 0.3 : 1}
        className="ontology-node"
        data-testid={`ontology-node-${node.label}`}
        onClick={(e) => {
          e.stopPropagation();
          onClick();
        }}
      >
        <title>
          {[node.relation ?? node.label, node.description]
            .filter(Boolean)
            .join("\n")}
        </title>
        <rect
          width={NODE_WIDTH}
          height={NODE_HEIGHT}
          rx={8}
          fill={
            node.isUnresolved
              ? colors.background.error
              : colors.background.primary
          }
          stroke={border}
          strokeWidth={isSelected || isHighlighted ? 2.5 : 1}
          strokeDasharray={node.isUnresolved ? "5 4" : undefined}
          className={isNew ? "ontology-new" : undefined}
        />
        {!node.isUnresolved && (
          <rect
            x={0}
            y={10}
            width={4}
            height={NODE_HEIGHT - 20}
            fill={color}
            rx={2}
          />
        )}
        <text
          x={14}
          y={24}
          fontSize={13}
          fontWeight={600}
          fill={
            node.isUnresolved ? colors.accent.red : colors.foreground.primary
          }
        >
          {truncate(
            node.label,
            node.isDataProduct ? TITLE_CHARS - 5 : TITLE_CHARS,
          )}
        </text>
        <text x={14} y={44} fontSize={11} fill={colors.foreground.secondary}>
          {truncate(
            // Every catalog entity is a relation, so its name says more.
            node.kind === "entity" && node.relation
              ? node.relation
              : nodeKindLabel(node.kind),
            node.kind === "entity" ? TITLE_CHARS + 4 : SUBTITLE_CHARS,
          )}
        </text>
        {node.isDataProduct && (
          <g transform={`translate(${NODE_WIDTH - 46}, 11)`}>
            <title>Listed as a data product by the agent MCP server</title>
            <rect
              width={36}
              height={16}
              rx={8}
              fill={colors.background.accent}
              stroke={colors.accent.purple}
              strokeOpacity={0.4}
            />
            <text
              x={18}
              y={11.5}
              textAnchor="middle"
              fontSize={9.5}
              fontWeight={600}
              fill={colors.accent.purple}
            >
              MCP
            </text>
          </g>
        )}
        {freshness && status && (
          <g transform={`translate(${NODE_WIDTH - 12}, 40)`}>
            <circle
              cx={0}
              cy={0}
              r={4}
              fill={freshnessColor(freshness, colors)}
              className={
                freshness.state === "hydrating"
                  ? "ontology-hydrating"
                  : undefined
              }
            />
            <text
              x={-9}
              y={4}
              textAnchor="end"
              fontSize={10.5}
              fill={colors.foreground.secondary}
              style={{ fontVariantNumeric: "tabular-nums" }}
            >
              {status}
            </text>
          </g>
        )}
      </g>
    );
  },
);
