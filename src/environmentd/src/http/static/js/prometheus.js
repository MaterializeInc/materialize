// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

'use strict';

/**
 * @typedef {Object<string, string>} Labels
 *
 * @typedef {{
 *   metricName: string,
 *   labels: Labels,
 *   value: number,
 *   raw: string
 * }} Sample
 *
 * @typedef {{ le: number, value: number }} CumulativeBucket
 * @typedef {{ le: number, count: number }} DeCumulatedBucket
 *
 * @typedef {{
 *   labels: Labels,
 *   buckets?: CumulativeBucket[],
 *   deCumulatedBuckets: DeCumulatedBucket[],
 *   sum: number|null,
 *   count: number|null,
 *   average: number|null
 * }} HistogramSeries
 *
 * @typedef {{ labels: Labels, value: number }} ScalarSeries
 *
 * @typedef {{
 *   name: string,
 *   help: string,
 *   type: string,
 *   samples: Sample[],
 *   labelNames: string[],
 *   series?: ScalarSeries[],
 *   histogramSeries?: HistogramSeries[]
 * }} MetricFamily
 */

/**
 * Parse Prometheus text exposition format into structured metric families and groups.
 *
 * @param {string} text - Raw Prometheus metrics text
 * @returns {{ families: Map<string, MetricFamily>, groups: Map<string, string[]> }}
 */
function parsePrometheusText(text) {
  const families = new Map();
  const lines = text.split('\n');

  // First pass: collect HELP and TYPE metadata
  for (const line of lines) {
    if (line.startsWith('# HELP ')) {
      const rest = line.slice(7);
      const spaceIdx = rest.indexOf(' ');
      if (spaceIdx === -1) continue;
      const name = rest.slice(0, spaceIdx);
      const help = rest.slice(spaceIdx + 1);
      updateFamily(families, name, { help });
    } else if (line.startsWith('# TYPE ')) {
      const rest = line.slice(7);
      const spaceIdx = rest.indexOf(' ');
      if (spaceIdx === -1) continue;
      const name = rest.slice(0, spaceIdx);
      const type = rest.slice(spaceIdx + 1);
      updateFamily(families, name, { type });
    }
  }

  // Second pass: parse sample lines and attach to families
  for (const line of lines) {
    if (line === '' || line.startsWith('#')) continue;

    const parsed = parseSampleLine(line);
    if (!parsed) continue;

    const { metricName, labels, value } = parsed;

    // Match to a known family (including histogram/summary suffixes)
    let familyName = null;
    if (families.has(metricName)) {
      familyName = metricName;
    } else {
      for (const suffix of ['_bucket', '_sum', '_count', '_total']) {
        const base = metricName.endsWith(suffix) ? metricName.slice(0, -suffix.length) : null;
        if (base && families.has(base)) {
          familyName = base;
          break;
        }
      }
    }

    if (!familyName) {
      familyName = metricName;
      updateFamily(families, familyName);
    }

    families.get(familyName).samples.push({ metricName, labels, value, raw: line });
  }

  // Third pass: build structured series per family
  for (const [, family] of families) {
    buildSeries(family);
  }

  // Group by prefix
  const groups = new Map();
  for (const name of families.keys()) {
    const prefix = getMetricPrefix(name);
    if (!groups.has(prefix)) {
      groups.set(prefix, []);
    }
    groups.get(prefix).push(name);
  }
  for (const [, names] of groups) {
    names.sort();
  }

  return { families, groups };
}

/**
 * Get or create a metric family in the families map, optionally updating help/type.
 *
 * @param {Map<string, MetricFamily>} families
 * @param {string} name
 * @param {{ help?: string, type?: string }} [opts]
 * @returns {MetricFamily}
 */
function updateFamily(families, name, { help, type } = {}) {
  if (!families.has(name)) {
    families.set(name, { name, help: '', type: 'untyped', samples: [], labelNames: [] });
  }
  const family = families.get(name);
  if (help !== undefined) family.help = help;
  if (type !== undefined) family.type = type;
  return family;
}

/**
 * Build structured series data for a family (mutates family in place).
 * - For histograms: groups buckets/sum/count by label combination (excluding `le`)
 * - For counters/gauges: produces a flat series array of { labels, value }
 *
 * @param {MetricFamily} family
 */
function buildSeries(family) {
  const isHistogram = family.type === 'histogram';
  const excludeLabels = isHistogram ? new Set(['le']) : new Set();

  // Collect label names (excluding le for histograms)
  const labelNameSet = new Set();
  for (const sample of family.samples) {
    for (const key of Object.keys(sample.labels)) {
      if (!excludeLabels.has(key)) {
        labelNameSet.add(key);
      }
    }
  }
  family.labelNames = [...labelNameSet].sort();

  if (isHistogram) {
    family.histogramSeries = buildHistogramSeries(family.samples, family.labelNames);
  } else {
    family.series = buildScalarSeries(family.samples, family.name);
  }
}

/**
 * Group histogram samples by label combination and build de-cumulated bucket series.
 *
 * @param {Sample[]} samples
 * @param {string[]} labelNames - Label names excluding 'le'
 * @returns {HistogramSeries[]}
 */
function buildHistogramSeries(samples, labelNames) {
  const seriesMap = new Map();

  for (const sample of samples) {
    const groupLabels = {};
    for (const key of labelNames) {
      if (sample.labels[key] !== undefined) {
        groupLabels[key] = sample.labels[key];
      }
    }
    const key = JSON.stringify(groupLabels);

    if (!seriesMap.has(key)) {
      seriesMap.set(key, { labels: groupLabels, buckets: [], sum: null, count: null, average: null });
    }
    const series = seriesMap.get(key);

    if (sample.metricName.endsWith('_bucket')) {
      const le = sample.labels.le;
      if (le !== undefined) {
        series.buckets.push({ le: le === '+Inf' ? Infinity : parseFloat(le), value: sample.value });
      }
    } else if (sample.metricName.endsWith('_sum')) {
      series.sum = sample.value;
    } else if (sample.metricName.endsWith('_count')) {
      series.count = sample.value;
    }
  }

  const result = [];
  for (const [, series] of seriesMap) {
    series.buckets.sort((a, b) => a.le - b.le);
    // De-cumulate
    series.deCumulatedBuckets = [];
    let prev = 0;
    for (const bucket of series.buckets) {
      const count = bucket.value - prev;
      series.deCumulatedBuckets.push({ le: bucket.le, count: Math.max(0, count) });
      prev = bucket.value;
    }
    if (series.sum !== null && series.count !== null && series.count > 0) {
      series.average = series.sum / series.count;
    }
    result.push(series);
  }
  return result;
}

/**
 * Build scalar (counter/gauge) series from samples matching the family name.
 *
 * @param {Sample[]} samples
 * @param {string} familyName
 * @returns {ScalarSeries[]}
 */
function buildScalarSeries(samples, familyName) {
  return samples
    .filter(s => s.metricName === familyName || s.metricName === familyName + '_total')
    .map(s => ({ labels: s.labels, value: s.value }));
}

// --- Line parsing ---

/**
 * Parse a single Prometheus sample line into metric name, labels, and value.
 *
 * @param {string} line - e.g. 'http_requests_total{method="GET"} 42'
 * @returns {{ metricName: string, labels: Labels, value: number } | null}
 */
function parseSampleLine(line) {
  const braceIdx = line.indexOf('{');
  const spaceIdx = line.indexOf(' ');

  if (braceIdx !== -1 && (spaceIdx === -1 || braceIdx < spaceIdx)) {
    const metricName = line.slice(0, braceIdx);
    const closeBrace = line.indexOf('}', braceIdx);
    if (closeBrace === -1) return null;
    const labels = parseLabels(line.slice(braceIdx + 1, closeBrace));
    const value = parseFloat(line.slice(closeBrace + 1).trim().split(/\s+/)[0]);
    if (isNaN(value)) return null;
    return { metricName, labels, value };
  } else if (spaceIdx !== -1) {
    const metricName = line.slice(0, spaceIdx);
    const value = parseFloat(line.slice(spaceIdx + 1).trim().split(/\s+/)[0]);
    if (isNaN(value)) return null;
    return { metricName, labels: {}, value };
  }
  return null;
}

/**
 * Parse a Prometheus label string into key-value pairs.
 *
 * @param {string} str - e.g. 'method="GET",status="200"'
 * @returns {Labels}
 */
function parseLabels(str) {
  /** @type {Labels} */
  const labels = {};
  if (!str) return labels;
  const re = /(\w+)="([^"]*)"/g;
  for (const m of str.matchAll(re)) {
    labels[m[1]] = m[2];
  }
  return labels;
}

/**
 * Extract the grouping prefix from a metric name.
 * For mz_* metrics, uses the second segment (e.g. 'mz_compute').
 * For others, uses the first segment (e.g. 'http').
 *
 * @param {string} name
 * @returns {string}
 */
function getMetricPrefix(name) {
  if (name.startsWith('mz_')) {
    const rest = name.slice(3);
    const idx = rest.indexOf('_');
    if (idx !== -1) {
      return 'mz_' + rest.slice(0, idx);
    }
    return name;
  }
  const idx = name.indexOf('_');
  if (idx !== -1) {
    return name.slice(0, idx);
  }
  return name;
}
