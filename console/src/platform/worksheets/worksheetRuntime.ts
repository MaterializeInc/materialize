// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Module, Observer, Runtime, Variable } from "@observablehq/runtime";

import {
  convertResultValue,
  mapColumnToColumnMetadata,
} from "~/api/materialize";
import { Column } from "~/api/materialize/types";

export type WorksheetRow = Record<string, unknown>;

/**
 * Converts raw websocket rows into objects keyed by column name, with values
 * typed the way the rest of the Console types them.
 */
export function toWorksheetRows(
  columns: Column[],
  rows: unknown[][],
): WorksheetRow[] {
  const metadata = columns.map(mapColumnToColumnMetadata);
  return rows.map((row) => {
    const result: WorksheetRow = {};
    metadata.forEach((column, i) => {
      const value = convertResultValue(row[i], column);
      // int8 arrives as a BigInt, which Plot's scales reject. Charts tolerate
      // the precision loss above 2^53.
      result[column.name] = typeof value === "bigint" ? Number(value) : value;
    });
    return result;
  });
}

/** What a SQL cell publishes: its rows, capped for display, and the true count. */
export interface PublishedRows {
  rows: WorksheetRow[];
  totalRows: number;
}

/**
 * The Observable runtime the worksheet's cells share. SQL cells publish their
 * current rows under the cell's name, and consumers such as chart cells
 * observe a name and re-run whenever it is republished.
 *
 * A consumer may observe a name before anything publishes it. It is rejected
 * until a publish defines the name, then recomputes.
 */
export class WorksheetRuntime {
  // The default global falls back to `globalThis`, so a cell named `history`
  // that has not run would resolve to the browser's history object.
  private runtime = new Runtime(undefined, (name: string) => {
    throw new Error(`${name} has not run yet`);
  });
  private module: Module = this.runtime.module();
  /** Keyed by cell id, so two cells briefly sharing a name never collide. */
  private published = new Map<string, Variable>();

  /**
   * Publishes a cell's rows. `name` is undefined while the cell's name is
   * invalid or taken, which leaves its value unreachable.
   */
  publish(cellId: string, name: string | undefined, value: PublishedRows) {
    let variable = this.published.get(cellId);
    if (!variable) {
      variable = this.module.variable();
      this.published.set(cellId, variable);
    }
    variable.define(name ?? null, [], () => value);
  }

  unpublish(cellId: string) {
    this.published.get(cellId)?.delete();
    this.published.delete(cellId);
  }

  /** Returns a function that stops observing. */
  observe(name: string, observer: Observer<PublishedRows>) {
    let observing = true;
    // NOTE: deleting a variable redefines it as a no-op first, which reports
    // `undefined` to its observer. Callbacks after stopping are dropped.
    const variable = this.module
      .variable({
        pending: () => observing && observer.pending?.(),
        fulfilled: (value) =>
          observing && observer.fulfilled?.(value as PublishedRows),
        rejected: (error) => observing && observer.rejected?.(error),
      })
      .define([name], (value: PublishedRows) => value);
    return () => {
      observing = false;
      variable.delete();
    };
  }

  dispose() {
    this.runtime.dispose();
  }
}
