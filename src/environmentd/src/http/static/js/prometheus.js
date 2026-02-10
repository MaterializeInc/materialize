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
      ensureFamily(families, name).help = help;
    } else if (line.startsWith('# TYPE ')) {
      const rest = line.slice(7);
      const spaceIdx = rest.indexOf(' ');
      if (spaceIdx === -1) continue;
      const name = rest.slice(0, spaceIdx);
      const type = rest.slice(spaceIdx + 1);
      ensureFamily(families, name).type = type;
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
      ensureFamily(families, familyName);
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

function ensureFamily(families, name) {
  if (!families.has(name)) {
    families.set(name, { name, help: '', type: 'untyped', samples: [] });
  }
  return families.get(name);
}

/**
 * Build structured series data for a family.
 * - For histograms: groups buckets/sum/count by label combination (excluding `le`)
 * - For counters/gauges: produces a flat series array of { labels, value }
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
    buildHistogramSeries(family);
  } else {
    buildScalarSeries(family);
  }
}

function buildHistogramSeries(family) {
  const seriesMap = new Map();

  for (const sample of family.samples) {
    // Build grouping key from non-le labels
    const groupLabels = {};
    for (const key of family.labelNames) {
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

  family.histogramSeries = [];
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
    family.histogramSeries.push(series);
  }
}

function buildScalarSeries(family) {
  // "Main" samples: those matching family name or family_name_total
  // (exclude _sum, _count, _bucket which belong to sub-metrics)
  const mainSamples = family.samples.filter(s =>
    s.metricName === family.name || s.metricName === family.name + '_total'
  );
  family.series = mainSamples.map(s => ({ labels: s.labels, value: s.value }));
}

// --- Line parsing ---

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

function parseLabels(str) {
  const labels = {};
  if (!str) return labels;
  let i = 0;
  while (i < str.length) {
    while (i < str.length && (str[i] === ',' || str[i] === ' ')) i++;
    if (i >= str.length) break;
    const eqIdx = str.indexOf('=', i);
    if (eqIdx === -1) break;
    const key = str.slice(i, eqIdx);
    if (str[eqIdx + 1] !== '"') break;
    let j = eqIdx + 2;
    let value = '';
    while (j < str.length) {
      if (str[j] === '\\' && j + 1 < str.length) {
        value += str[j + 1];
        j += 2;
      } else if (str[j] === '"') {
        break;
      } else {
        value += str[j];
        j++;
      }
    }
    labels[key] = value;
    i = j + 1;
  }
  return labels;
}

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
