// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";
import { useSearchParams } from "react-router-dom";

import { TIME_PERIOD_SEARCH_PARAM_KEY } from "~/hooks/useTimePeriodSelect";

import {
  CLUSTER_SEARCH_PARAM,
  DEFAULT_THRESHOLD_MS,
  DEFAULT_TIME_PERIOD_MINUTES,
  FreshnessScope,
  OBJECT_TYPE_FILTERS,
  OBJECT_TYPE_SEARCH_PARAM,
  PREDICATE_SEARCH_PARAM,
  SCHEMA_SEARCH_PARAM,
  SCOPE_LABELS,
  SCOPE_SEARCH_PARAM,
  THRESHOLD_SEARCH_PARAM,
  TIME_PERIOD_OPTIONS,
} from "./constants";
import { Predicate, PREDICATE_LABELS } from "./freshnessRows";

/**
 * Reads a positive number from a search param, or null if it isn't one.
 *
 * Search params are user-editable, so every read here is a parse of untrusted
 * text. Anything that isn't a finite non-negative number falls back to the
 * default rather than reaching the graph, where a NaN threshold would silently
 * stop matching every line.
 */
export function parsePositiveNumber(raw: string | null): number | null {
  if (raw === null || raw.trim() === "") return null;
  const value = Number(raw);
  return Number.isFinite(value) && value >= 0 ? value : null;
}

/**
 * A time period the selector actually offers, or null.
 *
 * NOTE: `Object.hasOwn` rather than `in`, which also answers true for
 * inherited keys. `?timePeriod=constructor` passed an `in` check, and
 * `Number` of it is NaN, which reached the query as
 * `INTERVAL 'NaN MILLISECONDS'`.
 */
export function parseTimePeriod(raw: string | null): number | null {
  if (raw === null || !Object.hasOwn(TIME_PERIOD_OPTIONS, raw)) return null;
  return Number(raw);
}

/**
 * A predicate the menu offers, or null.
 *
 * NOTE: `Object.hasOwn` for the same reason as `parseTimePeriod`, and the
 * failure here is quieter. `?exceeded=toString` passed an `in` check and then
 * fell through `statFor` to p90, so the page judged by one statistic while the
 * menu named another.
 */
export function parsePredicate(raw: string | null): Predicate | null {
  return raw !== null && Object.hasOwn(PREDICATE_LABELS, raw)
    ? (raw as Predicate)
    : null;
}

/** A scope the menu offers, or null. `Object.hasOwn` as in `parsePredicate`. */
export function parseScope(raw: string | null): FreshnessScope | null {
  return raw !== null && Object.hasOwn(SCOPE_LABELS, raw)
    ? (raw as FreshnessScope)
    : null;
}

/**
 * The object types named in the param, dropping any the scope's menu does not
 * offer.
 *
 * An empty result means no filter rather than "nothing matches", so a stale or
 * hand-edited param degrades to showing everything instead of an empty page.
 */
export function parseObjectTypes(
  raw: string | null,
  scope: FreshnessScope,
): string[] {
  if (raw === null) return [];
  const offered = OBJECT_TYPE_FILTERS[scope].map((filter) => filter.value);
  return raw
    .split(",")
    .map((value) => value.trim())
    .filter((value) => offered.includes(value));
}

export interface FreshnessParams {
  scope: FreshnessScope;
  clusterId: string | null;
  schemaId: string | null;
  threshold: number;
  timePeriodMinutes: number;
  predicate: Predicate;
  objectTypes: string[];
  setScope: (scope: FreshnessScope) => void;
  setClusterId: (id: string) => void;
  setSchemaId: (id: string) => void;
  setThreshold: (ms: number) => void;
  setTimePeriodMinutes: (minutes: number) => void;
  setPredicate: (predicate: Predicate) => void;
  setObjectTypes: (types: string[]) => void;
}

/**
 * The page's controls, held in the URL.
 *
 * All of them go in the URL rather than in component state or local storage so
 * that a view is a link: the cluster or schema, what counts as stale, and over what
 * window are exactly the things someone wants to hand to a colleague along with
 * "look at this". Replacing rather than pushing keeps dragging the threshold
 * from filling the back button with history.
 */
export function useFreshnessParams(): FreshnessParams {
  const [searchParams, setSearchParams] = useSearchParams();

  const clear = (key: string) =>
    setSearchParams(
      (prev) => {
        prev.delete(key);
        return prev;
      },
      { replace: true },
    );

  const set = (key: string, value: string) =>
    setSearchParams(
      (prev) => {
        prev.set(key, value);
        return prev;
      },
      { replace: true },
    );

  // Drops both selections so the page falls back to the new scope's first
  // option, and the types the new scope's menu does not offer, so the param
  // never carries a filter the menu cannot show.
  const setScope = (scope: FreshnessScope) =>
    setSearchParams(
      (prev) => {
        prev.set(SCOPE_SEARCH_PARAM, scope);
        prev.delete(CLUSTER_SEARCH_PARAM);
        prev.delete(SCHEMA_SEARCH_PARAM);
        const types = parseObjectTypes(
          prev.get(OBJECT_TYPE_SEARCH_PARAM),
          scope,
        );
        if (types.length === 0) {
          prev.delete(OBJECT_TYPE_SEARCH_PARAM);
        } else {
          prev.set(OBJECT_TYPE_SEARCH_PARAM, types.join(","));
        }
        return prev;
      },
      { replace: true },
    );
  const setClusterId = (id: string) => set(CLUSTER_SEARCH_PARAM, id);
  const setSchemaId = (id: string) => set(SCHEMA_SEARCH_PARAM, id);
  const setThreshold = (ms: number) => set(THRESHOLD_SEARCH_PARAM, String(ms));
  const setTimePeriodMinutes = (minutes: number) =>
    set(TIME_PERIOD_SEARCH_PARAM_KEY, String(minutes));
  const setPredicate = (predicate: Predicate) =>
    set(PREDICATE_SEARCH_PARAM, predicate);
  // An empty selection is the absence of a filter, so it clears the param
  // rather than writing a sentinel nobody else would recognise.
  const setObjectTypes = (types: string[]) =>
    types.length === 0
      ? clear(OBJECT_TYPE_SEARCH_PARAM)
      : set(OBJECT_TYPE_SEARCH_PARAM, types.join(","));

  // Memoized on the raw param, which is a string and so compares by value.
  // Rebuilding it per render would hand every consuming `useMemo` a new array
  // identity, which silently disables their caches: the whole stats and rows
  // chain would then recompute on each pointer move of a threshold drag.
  const scope = parseScope(searchParams.get(SCOPE_SEARCH_PARAM)) ?? "cluster";
  const rawObjectTypes = searchParams.get(OBJECT_TYPE_SEARCH_PARAM);
  const objectTypes = React.useMemo(
    () => parseObjectTypes(rawObjectTypes, scope),
    [rawObjectTypes, scope],
  );

  return {
    scope,
    clusterId: searchParams.get(CLUSTER_SEARCH_PARAM),
    schemaId: searchParams.get(SCHEMA_SEARCH_PARAM),
    threshold:
      parsePositiveNumber(searchParams.get(THRESHOLD_SEARCH_PARAM)) ??
      DEFAULT_THRESHOLD_MS,
    timePeriodMinutes:
      parseTimePeriod(searchParams.get(TIME_PERIOD_SEARCH_PARAM_KEY)) ??
      DEFAULT_TIME_PERIOD_MINUTES,
    predicate:
      parsePredicate(searchParams.get(PREDICATE_SEARCH_PARAM)) ?? "peak",
    objectTypes,
    setScope,
    setClusterId,
    setSchemaId,
    setThreshold,
    setTimePeriodMinutes,
    setPredicate,
    setObjectTypes,
  };
}
