// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { PostgresError as ErrorCode } from "pg-error-enum";

import { SchemaObject } from "~/api/materialize";

export { ErrorCode };

export type SessionVariables = {
  application_name?: string | undefined;
  cluster?: string | undefined;
  cluster_replica?: string | undefined;
  database?: string | undefined;
  search_path?: string | undefined;
  transaction_isolation?: string | undefined;
};

/** Types copied from https://materialize.com/docs/integrations/http-api/#output-format */
export interface SimpleRequest {
  query: string;
}

export interface ExtendedRequestItem {
  query: string;
  params?: (string | null)[];
}

export interface ExtendedRequest {
  queries: ExtendedRequestItem[];
}

export type SqlRequest = SimpleRequest | ExtendedRequest;

// Based on https://github.com/MaterializeInc/materialize/blob/67ceb5670b515887357624709acb904e7f39f42b/src/pgwire/src/message.rs#L446-L456
export type NoticeSeverity =
  | "Panic"
  | "Fatal"
  | "Error"
  | "Warning"
  | "Notice"
  | "Debug"
  | "Info"
  | "Log";

export interface Notice {
  // TODO: Make code required when v0.96 is released
  code?: string;
  message: string;
  severity: NoticeSeverity;
  detail?: string;
  hint?: string;
}

export interface Error {
  message: string;
  /* Postgres error code from https://www.postgresql.org/docs/current/errcodes-appendix.html */
  code: ErrorCode;
  detail?: string;
  hint?: string;
  position?: number;
}

export interface Column {
  name: string;
  type_oid: MzDataType;
  type_len: number; // i16
  type_mod: number; // i32
}

export type ColumnMetadata = {
  name: string;
  typeLength: number;
  typeOid: MzDataType;
};

interface Description {
  columns: Column[];
}

// We receive these results on read queries such as SELECT
export type TabularSqlResult = {
  tag: string;
  rows: any[][];
  desc: Description;
  notices: Notice[];
};

// We receive these results on write queries such as CREATE
export type OkSqlResult = {
  ok: string;
  notices: Notice[];
};

export type ErrorSqlResult = {
  error: Error;
  notices: Notice[];
};

export type SqlResult = TabularSqlResult | OkSqlResult | ErrorSqlResult;

export interface ClusterReplica {
  id: string;
  name: string;
  clusterName: string;
}

export interface Cluster {
  id: string;
  name: string;
}

export interface ExplainTimestampResult {
  determination: {
    timestamp_context: { TimelineTimestamp: Array<number | string> };
    since: { elements: number[] };
    upper: { elements: number[] };
    largest_not_in_advance_of_upper: number;
    oracle_read_ts: number;
  };
  sources: {
    name: string;
    read_frontier: number[];
    write_frontier: number[];
  }[];
}

export interface Schema {
  id: string;
  name: string;
  databaseName: string | null;
}

export type DatabaseObject = Schema | SchemaObject | Cluster | ClusterReplica;

export type ConnectorStatus =
  | "created"
  | "dropped"
  | "failed"
  | "paused"
  | "running"
  | "stalled"
  | "starting";

/**
 * Materialize object types collected from mz_objects table.
 * Our HTTP API and WebSocket API retrieves these values to determine the types of each column returned.
 * Although these object IDs are subject to change, they rarely do since they're derived from Postgres' object IDs for equivalent types.
 * Types prefixed with underscores represent an array of that type.
 * A leading underscore indicates an array of that type, e.g. _bool is a bool array.
 *
 * Source: https://materialize.com/docs/sql/system-catalog/mz_catalog/#mz_objects
 */
export enum MzDataType {
  any = 2276,
  map = 16385,
  oid = 26,
  _oid = 1028,
  bool = 16,
  char = 18,
  date = 1082,
  int2 = 21,
  int4 = 23,
  int8 = 20,
  list = 16384,
  text = 25,
  time = 1083,
  uuid = 2950,
  _bool = 1000,
  _char = 1002,
  _date = 1182,
  _int2 = 1005,
  _int4 = 1007,
  _int8 = 1016,
  _text = 1009,
  _time = 1183,
  _uuid = 2951,
  bytea = 17,
  jsonb = 3802,
  uint2 = 16460,
  uint4 = 16462,
  uint8 = 16464,
  _bytea = 1001,
  _jsonb = 3807,
  _uint2 = 16461,
  _uint4 = 16463,
  _uint8 = 16465,
  bpchar = 1042,
  float4 = 700,
  float8 = 701,
  record = 2249,
  _bpchar = 1014,
  _float4 = 1021,
  _float8 = 1022,
  _record = 2287,
  numeric = 1700,
  regproc = 24,
  regtype = 2206,
  tsrange = 3908,
  varchar = 1043,
  _numeric = 1231,
  _regproc = 1008,
  _regtype = 2211,
  _tsrange = 3909,
  _varchar = 1015,
  anyarray = 2277,
  anyrange = 3831,
  interval = 1186,
  numrange = 3906,
  regclass = 2205,
  _interval = 1187,
  _numrange = 3907,
  _regclass = 2210,
  daterange = 3912,
  int4range = 3904,
  int8range = 3926,
  timestamp = 1114,
  tstzrange = 3910,
  _daterange = 3913,
  _int4range = 3905,
  _int8range = 3927,
  _timestamp = 1115,
  _tstzrange = 3911,
  anyelement = 2283,
  int2vector = 22,
  mz_aclitem = 16566,
  _int2vector = 1006,
  _mz_aclitem = 16567,
  anynonarray = 2776,
  timestamptz = 1184,
  _timestamptz = 1185,
  mz_timestamp = 16552,
  _mz_timestamp = 16553,
  anycompatible = 5077,
  anycompatiblemap = 16455,
  anycompatiblelist = 16454,
  anycompatiblearray = 5078,
  anycompatiblerange = 5080,
  anycompatiblenonarray = 5079,
}
export function getMzDataTypeOid(
  type: keyof typeof MzDataType,
  isArray = false,
) {
  if (isArray) {
    return MzDataType[`_${type}` as keyof typeof MzDataType];
  } else {
    return MzDataType[type as keyof typeof MzDataType];
  }
}
