// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { AutoScalingPolicyParams } from "~/api/materialize/cluster/alterCluster";
import { clusterSizeFamily } from "~/api/materialize/cluster/availableClusterSizes";

import { AutoScaling } from "./ClusterPage/Configuration/clusterConfigurationModel";

export interface AutoScalingFormValues {
  autoScalingEnabled: boolean;
  hydrationSize: string;
  /** A duration like `15s`, or empty for the default. */
  linger: string;
}

const LINGER_PATTERN = /^(\s*\d+(\.\d+)?\s*(ms|s|m|h))+\s*$/i;
const LINGER_TERM = /(\d+(?:\.\d+)?)\s*(ms|s|m|h)/gi;
const UNIT_MS: Record<string, number> = {
  ms: 1,
  s: 1000,
  m: 60_000,
  h: 3_600_000,
};

/** Reads a linger like `15s`, `2m`, `1m 30s` or `1500ms`. */
export function parseLinger(
  input: string,
): { ok: true; lingerMs: number | undefined } | { ok: false; message: string } {
  if (input.trim() === "") return { ok: true, lingerMs: undefined };
  if (!LINGER_PATTERN.test(input)) {
    return { ok: false, message: "Enter a duration like 15s, 2m or 1500ms." };
  }
  let lingerMs = 0;
  for (const [, amount, unit] of input.matchAll(LINGER_TERM)) {
    lingerMs += parseFloat(amount) * UNIT_MS[unit.toLowerCase()];
  }
  // Past this, microseconds lose integer precision and the literal would
  // stop naming the duration entered.
  if (!Number.isSafeInteger(Math.round(lingerMs * 1000))) {
    return { ok: false, message: "Linger duration is too long." };
  }
  return { ok: true, lingerMs };
}

/** A linger as the input shows it, in the largest unit that keeps it exact. */
export function lingerInputValue(lingerMs: number | undefined) {
  if (lingerMs === undefined) return "";
  const micros = Math.round(lingerMs * 1000);
  if (micros === 0) return "0s";
  if (micros % 3_600_000_000 === 0) return `${micros / 3_600_000_000}h`;
  if (micros % 60_000_000 === 0) return `${micros / 60_000_000}m`;
  if (micros % 1_000_000 === 0) return `${micros / 1_000_000}s`;
  return `${micros / 1000}ms`;
}

/**
 * The size after the cluster's in its family, or empty. Sizes arrive grouped
 * by family, then by cost (`buildAvailableClusterReplicaSizesQuery`), so the
 * next one is larger unless it starts another family.
 */
export function nextLargerSize(sizes: string[], size: string | null) {
  const index = size === null ? -1 : sizes.indexOf(size);
  const next = index === -1 ? undefined : sizes[index + 1];
  return next !== undefined &&
    size !== null &&
    clusterSizeFamily(next) === clusterSizeFamily(size)
    ? next
    : "";
}

/**
 * The burst size the form starts with: the next larger size than the one the
 * cluster is heading to, its in-progress resize's target if it has one. The
 * server rejects a burst equal to either the target or the current size, and
 * a resize down leaves the current size next.
 */
export function defaultBurstSize(
  sizes: string[],
  size: string | null,
  resizeTargetSize: string | undefined,
) {
  const next = nextLargerSize(sizes, resizeTargetSize ?? size);
  return next === size ? nextLargerSize(sizes, next) : next;
}

const sameLinger = (a: number | undefined, b: number | undefined) =>
  a === undefined || b === undefined
    ? a === b
    : Math.round(a * 1000) === Math.round(b * 1000);

/**
 * The policy a submit sends: one to set, null to remove the current one, or
 * undefined when the form leaves the policy as it is or can't build one yet.
 */
export function autoScalingChange(
  current: AutoScaling["policy"],
  { autoScalingEnabled, hydrationSize, linger }: AutoScalingFormValues,
): AutoScalingPolicyParams | null | undefined {
  if (!autoScalingEnabled) return current ? null : undefined;
  const parsed = parseLinger(linger);
  if (hydrationSize === "" || !parsed.ok) return undefined;
  if (
    current &&
    current.hydrationSize === hydrationSize &&
    sameLinger(current.lingerMs, parsed.lingerMs)
  ) {
    return undefined;
  }
  return { hydrationSize, lingerMs: parsed.lingerMs };
}
