// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { sql } from "kysely";
import React from "react";

import { escapedLiteral as lit, useSqlManyTyped } from "~/api/materialize";

import { queryBuilder } from "./db";

export interface DataflowStructureParams {
  clusterName: string;
  replicaName: string;
  objectId: string;
}

export type DataflowStructureResult = ReturnType<
  typeof useDataflowStructure
>["results"];

export type Operator = {
  id: bigint;
  address: string[];
  name: string;
  parentId: bigint | null;
  arrangementRecords: bigint | null;
  arrangementSizes: bigint | null;
  elapsedNs: number;
  lirId: string | null;
  lirOperator: string | null;
};

export type Channel = {
  id: number;
  fromOperatorId: number;
  fromOperatorAddress: string[];
  fromPort: number;
  toOperatorId: number;
  toOperatorAddress: string[];
  toPort: number;
  messagesSent: number;
  batchesSent: number;
  channelType: string;
};

export type LirOperator = {
  lir_id: string;
  operator: string;
  addresses: string[][];
};

export function useDataflowStructure(params?: DataflowStructureParams) {
  const queries = React.useMemo(() => {
    if (!params) {
      return null;
    }
    const { objectId } = params;

    return {
      channels: sql`
        SELECT
            mdco.id,
            from_operator_id AS "fromOperatorId",
            from_operator_address AS "fromOperatorAddress",
            from_port AS "fromPort",
            to_operator_id AS "toOperatorId",
            to_operator_address AS "toOperatorAddress",
            to_port AS "toPort",
            COALESCE(sum(sent), 0) AS "messagesSent",
            COALESCE(sum(batch_sent), 0) AS "batchesSent",
            mdc.type as "channelType"
        FROM
            mz_dataflow_channel_operators AS mdco
            JOIN mz_dataflow_channels AS mdc ON
                            mdc.id = mdco.id
            LEFT JOIN mz_message_counts AS mmc ON
                            mdco.id = mmc.channel_id
            JOIN mz_compute_exports mce ON mce.dataflow_id = from_operator_address[1]
        WHERE mce.export_id = ${lit(objectId)}
        GROUP BY
          mdco.id,
          "fromOperatorId",
          "fromOperatorAddress",
          "toOperatorId",
          "toOperatorAddress",
          "fromPort",
          "toPort",
          mdc."type"`
        .$castTo<Channel>()
        .compile(queryBuilder),

      operators: sql`
        WITH
          export_to_dataflow AS (
            SELECT export_id, id FROM mz_compute_exports AS mce JOIN mz_dataflows AS md ON mce.dataflow_id = md.id
          ),
          all_ops AS (
            SELECT
              e2d.export_id, mdod.id, mda.address, mdod.name, mdop.parent_id AS "parentId",
              coalesce(mas.records, 0) AS "arrangementRecords",
              coalesce(mas.size, 0) AS "arrangementSizes",
              coalesce(mse.elapsed_ns, 0) AS "elapsedNs",
              mlm.lir_id::text AS "lirId",
              mlm.operator AS "lirOperator"
            FROM
              export_to_dataflow AS e2d
              JOIN mz_dataflow_operator_dataflows
                              AS mdod ON e2d.id = mdod.dataflow_id
              LEFT JOIN mz_scheduling_elapsed
                              AS mse ON mdod.id = mse.id
              LEFT JOIN mz_arrangement_sizes
                              AS mas ON mdod.id = mas.operator_id
              LEFT JOIN mz_dataflow_operator_parents
                              AS mdop ON mdod.id = mdop.id
              LEFT JOIN mz_dataflow_addresses
                              AS mda ON mdod.id = mda.id
              LEFT JOIN mz_lir_mapping
                              AS mlm ON mlm.operator_id_start <= mdod.id AND mdod.id < mlm.operator_id_end AND mlm.global_id = export_id
          )
        SELECT
          id,
          address,
          name,
          "parentId",
          "arrangementRecords",
          "arrangementSizes",
          "elapsedNs",
          "lirId",
          "lirOperator"
        FROM all_ops WHERE export_id = ${lit(objectId)}`
        .$castTo<Operator>()
        .compile(queryBuilder),

      lir_operators: sql`
        SELECT lir_id, operator, list_agg(mda.address) AS addresses
        FROM
                    mz_lir_mapping AS mlm
          LEFT JOIN mz_dataflow_operators AS mdo
            ON (mlm.operator_id_start <= mdo.id
                           AND mdo.id < mlm.operator_id_end)
          JOIN      mz_dataflow_addresses AS mda
            ON mdo.id = mda.id
        WHERE mlm.global_id = ${lit(objectId)}
        GROUP BY lir_id, operator`
        .$castTo<LirOperator>()
        .compile(queryBuilder),
    };
  }, [params]);
  const { clusterName, replicaName } = params ?? {};
  return useSqlManyTyped(queries, {
    cluster: clusterName,
    replica: replicaName,
    // This query can be slow for large dataflows
    timeout: 30_000,
  });
}
