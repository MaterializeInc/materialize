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
  OBJECT_TYPE_SEARCH_PARAM,
  OBJECT_TYPE_VALUES,
  PREDICATE_SEARCH_PARAM,
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

/**
 * The object types named in the param, dropping any the menu does not offer.
 *
 * An empty result means no filter rather than "nothing matches", so a stale or
 * hand-edited param degrades to showing everything instead of an empty page.
 */
export function parseObjectTypes(raw: string | null): string[] {
  if (raw === null) return [];
  return raw
    .split(",")
    .map((value) => value.trim())
    .filter((value) => OBJECT_TYPE_VALUES.includes(value));
}

export interface FreshnessParams {
  clusterId: string | null;
  threshold: number;
  timePeriodMinutes: number;
  predicate: Predicate;
  objectTypes: string[];
  setClusterId: (id: string) => void;
  setThreshold: (ms: number) => void;
  setTimePeriodMinutes: (minutes: number) => void;
  setPredicate: (predicate: Predicate) => void;
  setObjectTypes: (types: string[]) => void;
}

/**
 * The page's three controls, held in the URL.
 *
 * All of them go in the URL rather than in component state or local storage so
 * that a view is a link: the cluster, what counts as stale, and over what
 * window are exactly the things someone wants to hand to a colleague along with
 * "look at this". Replacing rather than pushing keeps dragging the threshold
 * from filling the back button with history.
 */
export function useFreshnessParams(): FreshnessParams {
  const [searchParams, setSearchParams] = useSearchParams();

  const clear = React.useCallback(
    (key: string) => {
      setSearchParams(
        (prev) => {
          prev.delete(key);
          return prev;
        },
        { replace: true },
      );
    },
    [setSearchParams],
  );

  const set = React.useCallback(
    (key: string, value: string) => {
      setSearchParams(
        (prev) => {
          prev.set(key, value);
          return prev;
        },
        { replace: true },
      );
    },
    [setSearchParams],
  );

  // Stable identities. A caller debouncing one of these memoizes on it, and a
  // setter rebuilt every render would rebuild the debounce with it, leaving
  // every call to fire on its own timer.
  const setClusterId = React.useCallback(
    (id: string) => set(CLUSTER_SEARCH_PARAM, id),
    [set],
  );
  const setThreshold = React.useCallback(
    (ms: number) => set(THRESHOLD_SEARCH_PARAM, String(ms)),
    [set],
  );
  const setTimePeriodMinutes = React.useCallback(
    (minutes: number) => set(TIME_PERIOD_SEARCH_PARAM_KEY, String(minutes)),
    [set],
  );
  const setPredicate = React.useCallback(
    (predicate: Predicate) => set(PREDICATE_SEARCH_PARAM, predicate),
    [set],
  );
  // An empty selection is the absence of a filter, so it clears the param
  // rather than writing a sentinel nobody else would recognise.
  const setObjectTypes = React.useCallback(
    (types: string[]) =>
      types.length === 0
        ? clear(OBJECT_TYPE_SEARCH_PARAM)
        : set(OBJECT_TYPE_SEARCH_PARAM, types.join(",")),
    [set, clear],
  );

  // Memoized on the raw param, which is a string and so compares by value.
  // Rebuilding it per render would hand every consuming `useMemo` a new array
  // identity, which silently disables their caches: the whole stats and rows
  // chain would then recompute on each pointer move of a threshold drag.
  const rawObjectTypes = searchParams.get(OBJECT_TYPE_SEARCH_PARAM);
  const objectTypes = React.useMemo(
    () => parseObjectTypes(rawObjectTypes),
    [rawObjectTypes],
  );

  return {
    clusterId: searchParams.get(CLUSTER_SEARCH_PARAM),
    threshold:
      parsePositiveNumber(searchParams.get(THRESHOLD_SEARCH_PARAM)) ??
      DEFAULT_THRESHOLD_MS,
    timePeriodMinutes:
      parseTimePeriod(searchParams.get(TIME_PERIOD_SEARCH_PARAM_KEY)) ??
      DEFAULT_TIME_PERIOD_MINUTES,
    predicate:
      parsePredicate(searchParams.get(PREDICATE_SEARCH_PARAM)) ?? "peak",
    objectTypes,
    setClusterId,
    setThreshold,
    setTimePeriodMinutes,
    setPredicate,
    setObjectTypes,
  };
}
