// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Column } from "~/api/materialize/types";

// `numeric` orders digit runs by value, so `[9]` sorts before `[10]`.
const ROW_KEY_COLLATOR = new Intl.Collator(undefined, { numeric: true });

const METADATA_COLUMNS = new Set(["mz_timestamp", "mz_progressed", "mz_diff"]);

interface PendingUpdate {
  key: string;
  values: unknown[];
  diff: number;
}

/**
 * Reduces the output of `SUBSCRIBE ... WITH (PROGRESS)` into the multiset of
 * rows as of the latest closed timestamp.
 *
 * Updates are buffered until a progress message closes their timestamp, so a
 * reader never observes a partially applied timestamp.
 */
export class SubscribeDiffs {
  /** The latest closed timestamp. Every update before it has been applied. */
  frontier: string | undefined;
  private timestampIndex = -1;
  private progressedIndex = -1;
  private diffIndex = -1;
  private dataIndexes: number[] = [];
  private dataColumns: Column[] = [];
  private pending: PendingUpdate[] = [];
  private rowsByKey = new Map<string, { values: unknown[]; count: number }>();

  setColumns(columns: Column[]) {
    this.timestampIndex = columns.findIndex((c) => c.name === "mz_timestamp");
    this.progressedIndex = columns.findIndex((c) => c.name === "mz_progressed");
    this.diffIndex = columns.findIndex((c) => c.name === "mz_diff");
    this.dataIndexes = [];
    this.dataColumns = [];
    columns.forEach((c, i) => {
      if (!METADATA_COLUMNS.has(c.name)) {
        this.dataIndexes.push(i);
        this.dataColumns.push(c);
      }
    });
  }

  get columns() {
    return this.dataColumns;
  }

  /** Applies one `Row` message. Returns true when it closed a timestamp. */
  push(row: unknown[]): boolean {
    if (row[this.progressedIndex] === true) {
      this.applyPending();
      this.frontier = String(row[this.timestampIndex]);
      return true;
    }
    const values = this.dataIndexes.map((i) => row[i]);
    this.pending.push({
      key: JSON.stringify(values),
      values,
      diff: Number(row[this.diffIndex]),
    });
    return false;
  }

  /**
   * Applies updates still waiting for a progress message. Only correct once
   * the SUBSCRIBE has completed: a SUBSCRIBE over constant data can finish
   * without reporting progress past its last updates.
   */
  complete() {
    this.applyPending();
  }

  private applyPending() {
    for (const { key, values, diff } of this.pending) {
      const existing = this.rowsByKey.get(key);
      const count = (existing?.count ?? 0) + diff;
      if (count === 0) {
        this.rowsByKey.delete(key);
      } else {
        this.rowsByKey.set(key, { values, count });
      }
    }
    this.pending = [];
  }

  /**
   * The current rows, expanding multiplicities, capped at `limit`.
   *
   * Rows are ordered by value. An update arrives as a retraction plus an
   * insertion, so insertion order would move every changed row to the end.
   */
  rows(limit: number): { rows: unknown[][]; total: number } {
    const keys = Array.from(this.rowsByKey.keys()).sort(
      ROW_KEY_COLLATOR.compare,
    );
    const rows: unknown[][] = [];
    let total = 0;
    for (const key of keys) {
      const { values, count } = this.rowsByKey.get(key) as {
        values: unknown[];
        count: number;
      };
      total += count;
      for (let i = 0; i < count && rows.length < limit; i++) {
        rows.push(values);
      }
    }
    return { rows, total };
  }
}
