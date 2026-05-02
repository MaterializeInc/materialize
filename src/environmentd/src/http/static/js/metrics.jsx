// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

'use strict';

const { useState, useEffect, useRef, useCallback, useMemo } = React;

/**
 * Types referenced from prometheus.js: Labels, ScalarSeries, HistogramSeries, MetricFamily, DeCumulatedBucket
 *
 * @typedef {{ labels: Labels, value: number, delta?: number, rate?: number }} ScalarSeriesWithDelta
 *
 * @typedef {{
 *   labels: Labels,
 *   deCumulatedBuckets: DeCumulatedBucket[],
 *   sum: number,
 *   count: number,
 *   average: number|null,
 *   deltaCount?: number,
 *   obsRate?: number,
 *   rateBuckets?: DeCumulatedBucket[]
 * }} HistogramSeriesWithDelta
 */

// --- Collection utilities ---

/**
 * Group items by a key function, returning a Map from key to array of items.
 *
 * @template T
 * @param {T[]} items
 * @param {(item: T) => string} keyFn
 * @returns {Map<string, T[]>}
 */
function groupBy(items, keyFn) {
  const groups = new Map();
  for (const item of items) {
    const key = keyFn(item);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(item);
  }
  return groups;
}

/**
 * Index items by a key function, returning a Map from key to item (last wins on collision).
 *
 * @template T
 * @param {T[]} items
 * @param {(item: T) => string} keyFn
 * @returns {Map<string, T>}
 */
function keyBy(items, keyFn) {
  const map = new Map();
  for (const item of items) {
    map.set(keyFn(item), item);
  }
  return map;
}

/**
 * Pick a subset of label keys from a labels object.
 *
 * @param {Labels} labels
 * @param {Iterable<string>} keys
 * @returns {Labels}
 */
function pickLabels(labels, keys) {
  /** @type {Labels} */
  const result = {};
  for (const k of keys) {
    result[k] = labels[k] || '';
  }
  return result;
}

/** @param {Labels} labels */
const labelsKey = (labels) => JSON.stringify(labels);

// Max samples before requiring expand to see values
const AUTO_SHOW_THRESHOLD = 5;

/**
 * Fetch raw Prometheus metrics text from a URL.
 *
 * @param {string} url
 * @returns {Promise<string>}
 */
async function fetchMetrics(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw `Failed to fetch metrics: ${response.status} ${response.statusText}`;
  }
  return response.text();
}

/**
 * Discover available metrics endpoints (environmentd + cluster replicas).
 *
 * @returns {Promise<Array<{ label: string, url: string }>>}
 */
async function discoverEndpoints() {
  const endpoints = [{ label: 'environmentd', url: '/metrics' }];
  try {
    const data = await query(`
      SELECT
        c.id AS cluster_id,
        c.name AS cluster_name,
        r.id AS replica_id,
        r.name AS replica_name,
        sz.processes
      FROM
        mz_catalog.mz_cluster_replicas r
        JOIN mz_catalog.mz_clusters c ON c.id = r.cluster_id
        LEFT JOIN mz_catalog.mz_cluster_replica_sizes sz ON sz.size = r.size
      ORDER BY c.name, r.name
    `);
    const rows = data.results[0]?.rows ?? [];
    for (const [clusterId, clusterName, replicaId, replicaName, processes] of rows) {
      const numProcesses = parseInt(processes, 10) || 1;
      for (let p = 0; p < numProcesses; p++) {
        const label = numProcesses > 1
          ? `${clusterName}.${replicaName}/p${p}`
          : `${clusterName}.${replicaName}`;
        endpoints.push({
          label,
          url: `/api/cluster/${clusterId}/replica/${replicaId}/process/${p}/metrics`,
        });
      }
    }
  } catch (e) {
    console.warn('Could not discover cluster replicas:', e);
  }
  return endpoints;
}

/**
 * Format a numeric metric value for display.
 *
 * @param {number} v
 * @returns {string}
 */
function formatValue(v) {
  if (Number.isInteger(v)) return String(v);
  if (Math.abs(v) >= 0.01 && Math.abs(v) < 1e6) return v.toFixed(4).replace(/0+$/, '').replace(/\.$/, '');
  return v.toPrecision(6);
}

// --- Styles ---
/** @type {Record<string, React.CSSProperties>} */
const styles = {
  container: { fontFamily: 'system-ui, -apple-system, sans-serif', padding: '16px', maxWidth: '1200px', margin: '0 auto' },
  toolbar: { display: 'flex', gap: '12px', alignItems: 'center', marginBottom: '16px', flexWrap: 'wrap' },
  select: { padding: '6px 10px', fontSize: '14px', borderRadius: '4px', border: '1px solid #ccc' },
  input: { padding: '6px 10px', fontSize: '14px', borderRadius: '4px', border: '1px solid #ccc', minWidth: '250px' },
  button: { padding: '6px 14px', fontSize: '14px', borderRadius: '4px', border: '1px solid #ccc', background: '#f5f5f5', cursor: 'pointer' },
  groupHeader: { cursor: 'pointer', userSelect: 'none', padding: '8px 12px', background: '#f0f0f0', borderRadius: '4px', marginBottom: '4px', display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontWeight: 600 },
  groupContent: { paddingLeft: '12px', marginBottom: '12px' },
  familyCard: { border: '1px solid #e0e0e0', borderRadius: '6px', padding: '12px', marginBottom: '8px' },
  familyHeader: { display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '4px' },
  familyName: { fontWeight: 600, fontSize: '14px', fontFamily: 'monospace' },
  helpText: { color: '#666', fontStyle: 'italic', fontSize: '13px', marginBottom: '8px' },
  badge: { padding: '2px 8px', borderRadius: '10px', fontSize: '11px', fontWeight: 600, color: '#fff', textTransform: 'uppercase' },
  rawToggle: { fontSize: '12px', color: '#0066cc', cursor: 'pointer', border: 'none', background: 'none', textDecoration: 'underline', padding: 0 },
  rawPre: { background: '#f8f8f8', padding: '8px', borderRadius: '4px', fontSize: '12px', overflow: 'auto', maxHeight: '300px', fontFamily: 'monospace' },
  table: { borderCollapse: 'collapse', fontSize: '13px', fontFamily: 'monospace', width: '100%', marginTop: '4px' },
  th: { textAlign: 'left', padding: '4px 10px', borderBottom: '2px solid #ddd', fontWeight: 600, whiteSpace: 'nowrap', fontSize: '12px', color: '#555' },
  td: { padding: '3px 10px', borderBottom: '1px solid #eee', whiteSpace: 'nowrap' },
  tdValue: { padding: '3px 10px', borderBottom: '1px solid #eee', whiteSpace: 'nowrap', textAlign: 'right', fontWeight: 500 },
  seriesLabel: { display: 'inline-block', background: '#e8e8e8', borderRadius: '3px', padding: '1px 6px', fontSize: '12px', fontFamily: 'monospace', marginRight: '4px', marginBottom: '2px' },
  histSection: { marginTop: '6px', marginBottom: '6px', paddingLeft: '8px', borderLeft: '3px solid #ddd' },
  avgDisplay: { fontSize: '13px', color: '#333', margin: '2px 0' },
  copyBtn: { fontSize: '11px', color: '#0066cc', cursor: 'pointer', border: '1px solid #ccc', background: '#fafafa', borderRadius: '3px', padding: '1px 6px', marginLeft: '6px', verticalAlign: 'middle' },
};

// --- Copy to clipboard ---

function CopyButton({ getText }) {
  const [copied, setCopied] = useState(false);
  const onClick = () => {
    navigator.clipboard.writeText(getText()).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  };
  return (
    <button
      onClick={onClick}
      title="Copy to clipboard"
      style={styles.copyBtn}
    >
      {copied ? 'copied' : 'copy'}
    </button>
  );
}

/**
 * Format a histogram series as tab-separated text for clipboard export.
 *
 * @param {HistogramSeries} series
 * @returns {string}
 */
function histogramSeriesToTsv(series) {
  const labelParts = Object.entries(series.labels).map(([k, v]) => `${k}="${v}"`).join(', ');
  const header = 'le\tcount';
  const rows = series.deCumulatedBuckets.map(b =>
    `${b.le === Infinity ? '+Inf' : b.le}\t${b.count}`
  );
  const lines = [header, ...rows];
  if (series.average !== null) {
    lines.push(`# avg=${formatValue(series.average)} sum=${formatValue(series.sum)} count=${series.count}`);
  }
  if (labelParts) {
    lines.unshift(`# ${labelParts}`);
  }
  return lines.join('\n');
}

// --- Label dimension toggles & aggregation ---

function LabelDimensionToggles({ labelNames, selectedLabels, onToggle }) {
  if (!labelNames || labelNames.length === 0) return null;
  return (
    <div style={{ display: 'flex', gap: '4px', alignItems: 'center', flexWrap: 'wrap', margin: '4px 0 8px 0' }}>
      <span style={{ fontSize: '12px', color: '#888', marginRight: '2px' }}>split by:</span>
      {labelNames.map(name => {
        const active = selectedLabels.has(name);
        return (
          <button
            key={name}
            onClick={() => onToggle(name)}
            style={{
              fontSize: '12px',
              fontFamily: 'monospace',
              padding: '2px 8px',
              borderRadius: '12px',
              border: active ? '1px solid #2980b9' : '1px solid #bbb',
              background: active ? '#2980b9' : '#fff',
              color: active ? '#fff' : '#888',
              cursor: 'pointer',
            }}
          >
            {name}
          </button>
        );
      })}
    </div>
  );
}

/**
 * Aggregate histogram series by a subset of label dimensions, summing buckets.
 *
 * @param {HistogramSeries[]} histogramSeries
 * @param {string[]} labelNames - All label names on this family
 * @param {Set<string>} selectedLabels - Label dimensions to keep (others are collapsed)
 * @returns {HistogramSeries[]}
 */
function aggregateHistogramSeries(histogramSeries, labelNames, selectedLabels) {
  if (!histogramSeries || selectedLabels.size === labelNames.length) return histogramSeries;

  const grouped = groupBy(histogramSeries, s => labelsKey(pickLabels(s.labels, selectedLabels)));

  return [...grouped.values()].map(seriesList => {
    const labels = pickLabels(seriesList[0].labels, selectedLabels);
    const sum = seriesList.reduce((acc, s) => acc + (s.sum || 0), 0);
    const count = seriesList.reduce((acc, s) => acc + (s.count || 0), 0);

    // Merge de-cumulated buckets across all series in the group
    const bucketSums = {};
    for (const series of seriesList) {
      for (const b of series.deCumulatedBuckets) {
        const leKey = String(b.le);
        bucketSums[leKey] = (bucketSums[leKey] || 0) + b.count;
      }
    }
    const deCumulatedBuckets = Object.entries(bucketSums)
      .map(([le, count]) => ({ le: parseFloat(le), count }))
      .sort((a, b) => a.le - b.le);

    return { labels, deCumulatedBuckets, sum, count, average: count > 0 ? sum / count : null };
  });
}

/**
 * Aggregate scalar series by a subset of label dimensions, summing values.
 *
 * @param {ScalarSeries[]} series
 * @param {string[]} labelNames - All label names on this family
 * @param {Set<string>} selectedLabels - Label dimensions to keep
 * @returns {{ series: ScalarSeries[], labelNames: string[] }}
 */
function aggregateScalarSeries(series, labelNames, selectedLabels) {
  if (!series || selectedLabels.size === labelNames.length) {
    return { series, labelNames: [...labelNames] };
  }

  const activeLabelNames = labelNames.filter(n => selectedLabels.has(n));
  const grouped = groupBy(series, s => labelsKey(pickLabels(s.labels, activeLabelNames)));

  const aggregated = [...grouped.values()].map(seriesList => ({
    labels: pickLabels(seriesList[0].labels, activeLabelNames),
    value: seriesList.reduce((acc, s) => acc + s.value, 0),
  }));

  return { series: aggregated, labelNames: activeLabelNames };
}

// --- Delta / rate computation ---

/**
 * Enrich scalar series with delta and rate by comparing against a previous snapshot.
 *
 * @param {ScalarSeries[]} currentSeries
 * @param {ScalarSeries[]} prevSeries
 * @param {number} dtSeconds - Elapsed time between snapshots
 * @returns {ScalarSeriesWithDelta[]}
 */
function computeScalarDeltas(currentSeries, prevSeries, dtSeconds) {
  if (!prevSeries || !dtSeconds) return currentSeries;
  const prevMap = keyBy(prevSeries, s => labelsKey(s.labels));
  return currentSeries.map(s => {
    const prev = prevMap.get(labelsKey(s.labels));
    if (!prev) return s;
    const delta = s.value - prev.value;
    return { ...s, delta, rate: delta / dtSeconds };
  });
}

/**
 * Enrich histogram series with delta counts, observation rates, and rate buckets.
 *
 * @param {HistogramSeries[]} currentList
 * @param {HistogramSeries[]} prevList
 * @param {number} dtSeconds - Elapsed time between snapshots
 * @returns {HistogramSeriesWithDelta[]}
 */
function computeHistogramDeltas(currentList, prevList, dtSeconds) {
  if (!prevList || !dtSeconds) return currentList;
  const prevMap = keyBy(prevList, s => labelsKey(s.labels));
  return currentList.map(s => {
    const prev = prevMap.get(labelsKey(s.labels));
    if (!prev) return s;
    const deltaCount = (s.count || 0) - (prev.count || 0);
    const obsRate = deltaCount / dtSeconds;
    const rateBuckets = s.deCumulatedBuckets.map((b, i) => {
      const prevCount = prev.deCumulatedBuckets?.[i]?.count ?? 0;
      return { le: b.le, count: Math.max(0, (b.count - prevCount) / dtSeconds) };
    });
    return { ...s, deltaCount, obsRate, rateBuckets };
  });
}

const badgeColors = {
  histogram: '#8e44ad',
  counter: '#2980b9',
  gauge: '#27ae60',
  summary: '#d35400',
  untyped: '#7f8c8d',
};

function TypeBadge({ type }) {
  const color = badgeColors[type] || badgeColors.untyped;
  return <span style={{ ...styles.badge, background: color }}>{type}</span>;
}

function SeriesLabels({ labels }) {
  const keys = Object.keys(labels);
  if (keys.length === 0) return null;
  return (
    <span>
      {keys.map(k => (
        <span key={k} style={styles.seriesLabel}>{k}="{labels[k]}"</span>
      ))}
    </span>
  );
}

// --- Counter/Gauge: Table display ---

function DeltaCell({ value, isRate }) {
  if (value === undefined || value === null) return <td style={styles.tdValue}>-</td>;
  const color = value > 0 ? '#27ae60' : value < 0 ? '#c0392b' : '#888';
  const prefix = value > 0 ? '+' : '';
  const display = isRate ? `${prefix}${formatValue(value)}/s` : `${prefix}${formatValue(value)}`;
  return <td style={{ ...styles.tdValue, color, fontSize: '12px' }}>{display}</td>;
}

function ScalarTable({ series, labelNames, metricType }) {
  if (!series || series.length === 0) return null;

  const hasRates = series.some(s => s.rate !== undefined);
  const isCounter = metricType === 'counter';

  // Single value with no labels: show inline
  if (series.length === 1 && labelNames.length === 0) {
    const s = series[0];
    const val = formatValue(s.value);
    return (
      <div style={{ fontSize: '14px', fontFamily: 'monospace', fontWeight: 500, margin: '4px 0' }}>
        {val}
        {hasRates && s.rate !== undefined && (
          <span style={{ fontSize: '12px', color: s.rate > 0 ? '#27ae60' : '#888', marginLeft: '8px' }}>
            {isCounter ? `${formatValue(s.rate)}/s` : `(${s.delta > 0 ? '+' : ''}${formatValue(s.delta)})`}
          </span>
        )}
        <CopyButton getText={() => val} />
      </div>
    );
  }

  const tsvText = () => {
    const cols = [...labelNames, 'value'];
    if (hasRates) cols.push(isCounter ? 'rate/s' : 'delta');
    const header = cols.join('\t');
    const rows = series.map(s => {
      const vals = [...labelNames.map(n => s.labels[n] || ''), formatValue(s.value)];
      if (hasRates) vals.push(s.rate !== undefined ? formatValue(isCounter ? s.rate : s.delta) : '');
      return vals.join('\t');
    });
    return header + '\n' + rows.join('\n');
  };

  return (
    <div style={{ overflowX: 'auto' }}>
      <div style={{ textAlign: 'right', marginBottom: '2px' }}>
        <CopyButton getText={tsvText} />
      </div>
      <table style={styles.table}>
        <thead>
          <tr>
            {labelNames.map(name => <th key={name} style={styles.th}>{name}</th>)}
            <th style={{ ...styles.th, textAlign: 'right' }}>value</th>
            {hasRates && <th style={{ ...styles.th, textAlign: 'right' }}>{isCounter ? 'rate/s' : 'delta'}</th>}
          </tr>
        </thead>
        <tbody>
          {series.map((s, i) => (
            <tr key={i}>
              {labelNames.map(name => <td key={name} style={styles.td}>{s.labels[name] || ''}</td>)}
              <td style={styles.tdValue}>{formatValue(s.value)}</td>
              {hasRates && <DeltaCell value={isCounter ? s.rate : s.delta} isRate={isCounter} />}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// --- Histogram: one chart per label combination ---

function HistogramChart({ buckets, width = 500 }) {
  const ref = useRef(null);
  const chartWidth = width || 500;

  useEffect(() => {
    if (!buckets || buckets.length === 0 || !ref.current) return;

    // Filter out +Inf bucket (skews the chart)
    const data = buckets.filter(b => isFinite(b.le));
    if (data.length === 0) return;

    const height = 180;
    const margin = { top: 8, right: 10, bottom: 36, left: 50 };
    const innerW = chartWidth - margin.left - margin.right;
    const innerH = height - margin.top - margin.bottom;

    d3.select(ref.current).selectAll('*').remove();

    const svg = d3.select(ref.current)
      .append('svg')
      .attr('width', chartWidth)
      .attr('height', height);

    const g = svg.append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`);

    const x = d3.scaleBand()
      .domain(data.map(b => String(b.le)))
      .range([0, innerW])
      .padding(0.1);

    const y = d3.scaleLinear()
      .domain([0, d3.max(data, /** @param {DeCumulatedBucket} b */ (b) => b.count) || 1])
      .range([innerH, 0]);

    g.selectAll('rect')
      .data(data)
      .enter()
      .append('rect')
      .attr('x', d => x(String(d.le)))
      .attr('y', d => y(d.count))
      .attr('width', x.bandwidth())
      .attr('height', d => innerH - y(d.count))
      .attr('fill', '#5b8def');

    g.append('g')
      .attr('transform', `translate(0,${innerH})`)
      .call(d3.axisBottom(x).tickSizeOuter(0))
      .selectAll('text')
      .attr('transform', 'rotate(-45)')
      .style('text-anchor', 'end')
      .style('font-size', '10px');

    g.append('g')
      .call(d3.axisLeft(y).ticks(5))
      .selectAll('text')
      .style('font-size', '10px');
  }, [buckets, chartWidth]);

  return <div ref={ref} aria-label="histogram-chart" />;
}

function HistogramSeriesDisplay({ series }) {
  const [showRate, setShowRate] = useState(false);
  const hasRate = series.rateBuckets && series.rateBuckets.length > 0;

  return (
    <div style={styles.histSection}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '6px', marginBottom: '4px' }}>
        {Object.keys(series.labels).length > 0 && <SeriesLabels labels={series.labels} />}
        <CopyButton getText={() => histogramSeriesToTsv(series)} />
      </div>
      <div style={styles.avgDisplay}>
        {series.average !== null && (
          <span>avg={formatValue(series.average)} (sum={formatValue(series.sum)}, count={series.count})</span>
        )}
        {series.obsRate !== undefined && (
          <span style={{ marginLeft: '8px', color: '#2980b9' }}>
            {formatValue(series.obsRate)} obs/s
          </span>
        )}
      </div>
      {hasRate && (
        <div style={{ margin: '2px 0' }}>
          <button style={styles.rawToggle} onClick={() => setShowRate(!showRate)}>
            {showRate ? 'show totals' : 'show rate/s'}
          </button>
        </div>
      )}
      {series.deCumulatedBuckets && series.deCumulatedBuckets.length > 0 && (
        <HistogramChart buckets={showRate && hasRate ? series.rateBuckets : series.deCumulatedBuckets} />
      )}
    </div>
  );
}

function HistogramDisplay({ seriesList }) {
  if (!seriesList || seriesList.length === 0) return null;

  return (
    <div>
      {seriesList.map((series, i) => (
        <HistogramSeriesDisplay key={i} series={series} />
      ))}
    </div>
  );
}

// --- Expandable (collapsible) ---

function Expandable({ children, type, numValues }) {
  const [expanded, setExpanded] = useState(false);
  return (
    <div style={{ marginTop: '4px' }}>
      <button style={styles.rawToggle} onClick={() => setExpanded(!expanded)}>
        {expanded ? `hide ${type}` : `show ${type}`} ({numValues} values)
      </button>
      {expanded && children}
    </div>
  );
}

// --- MetricFamily: type-aware rendering ---

function MetricFamily({ family, prevFamilies, dtSeconds }) {
  const isFewValues = family.samples.length <= AUTO_SHOW_THRESHOLD;
  const isHistogram = family.type === 'histogram';
  const isScalar = !isHistogram;
  const labelNames = family.labelNames || [];
  const hasLabels = labelNames.length > 0;
  const prevFamily = prevFamilies ? prevFamilies.get(family.name) : null;

  const [selectedLabels, setSelectedLabels] = useState(() => new Set(labelNames));

  const toggleLabel = useCallback((name) => {
    setSelectedLabels(prev => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  }, []);

  // Compute aggregated views (current)
  const aggHistogram = useMemo(() => {
    if (!isHistogram) return null;
    return aggregateHistogramSeries(family.histogramSeries, labelNames, selectedLabels);
  }, [isHistogram, family.histogramSeries, labelNames, selectedLabels]);

  const aggScalar = useMemo(() => {
    if (!isScalar || !family.series) return null;
    return aggregateScalarSeries(family.series, labelNames, selectedLabels);
  }, [isScalar, family.series, labelNames, selectedLabels]);

  // Compute aggregated views (previous) and deltas
  const prevLabelNames = prevFamily ? (prevFamily.labelNames || []) : [];

  const displayHistogram = useMemo(() => {
    if (!isHistogram || !aggHistogram) return aggHistogram;
    if (!prevFamily || !prevFamily.histogramSeries || !dtSeconds) return aggHistogram;
    const prevAgg = aggregateHistogramSeries(prevFamily.histogramSeries, prevLabelNames, selectedLabels);
    return computeHistogramDeltas(aggHistogram, prevAgg, dtSeconds);
  }, [aggHistogram, prevFamily, dtSeconds, selectedLabels, prevLabelNames, isHistogram]);

  const displayScalar = useMemo(() => {
    if (!isScalar || !aggScalar) return aggScalar;
    if (!prevFamily || !prevFamily.series || !dtSeconds) return aggScalar;
    const prevAgg = aggregateScalarSeries(prevFamily.series, prevLabelNames, selectedLabels);
    const withDeltas = computeScalarDeltas(aggScalar.series, prevAgg.series, dtSeconds);
    return { ...aggScalar, series: withDeltas };
  }, [aggScalar, prevFamily, dtSeconds, selectedLabels, prevLabelNames, isScalar]);

  return (
    <div style={styles.familyCard} aria-label="metric-family">
      <div style={styles.familyHeader}>
        <TypeBadge type={family.type} />
        <span style={styles.familyName}>{family.name}</span>
      </div>
      {family.help && <div style={styles.helpText}>{family.help}</div>}

      {hasLabels && (
        <LabelDimensionToggles
          labelNames={labelNames}
          selectedLabels={selectedLabels}
          onToggle={toggleLabel}
        />
      )}

      {isHistogram && <HistogramDisplay seriesList={displayHistogram} />}

      {isScalar && displayScalar && displayScalar.series.length > 0 && (
        isFewValues || displayScalar.series.length <= AUTO_SHOW_THRESHOLD ? (
          <ScalarTable series={displayScalar.series} labelNames={displayScalar.labelNames} metricType={family.type} />
        ) : (
          <Expandable type="table" numValues={displayScalar.series.length}>
            <ScalarTable series={displayScalar.series} labelNames={displayScalar.labelNames} metricType={family.type} />
          </Expandable>
        )
      )}

      {/* For metrics with few values and no structured series, show raw inline */}
      {isScalar && (!family.series || family.series.length === 0) && isFewValues && (
        <div style={{ position: 'relative' }}>
          <div style={{ position: 'absolute', top: '4px', right: '4px' }}>
            <CopyButton getText={() => family.samples.map(s => s.raw).join('\n')} />
          </div>
          <pre style={styles.rawPre}>{family.samples.map(s => s.raw).join('\n')}</pre>
        </div>
      )}

      {/* Raw data toggle for metrics with many values or histograms */}
      {(!isFewValues || isHistogram) && <RawData family={family} />}
    </div>
  );
}

function RawData({ family }) {
  const rawText = family.samples.map(s => s.raw).join('\n');
  return (
    <Expandable type="raw" numValues={family.samples.length}>
      <div style={{ position: 'relative' }}>
        <div style={{ position: 'absolute', top: '4px', right: '4px' }}>
          <CopyButton getText={() => rawText} />
        </div>
        <pre style={styles.rawPre}>{rawText}</pre>
      </div>
    </Expandable>
  );
}

// --- Group + Content ---

function MetricGroup({ prefix, names, families, defaultExpanded, prevFamilies, dtSeconds }) {
  const [expanded, setExpanded] = useState(defaultExpanded);
  return (
    <div aria-label="metric-group" style={{ marginBottom: '8px' }}>
      <div style={styles.groupHeader} onClick={() => setExpanded(!expanded)}>
        <span>{prefix} ({names.length} metrics)</span>
        <span>{expanded ? '\u25BC' : '\u25B6'}</span>
      </div>
      {expanded && (
        <div style={styles.groupContent}>
          {names.map(name => (
            <MetricFamily key={name} family={families.get(name)} prevFamilies={prevFamilies} dtSeconds={dtSeconds} />
          ))}
        </div>
      )}
    </div>
  );
}

function MetricsContent({ data, search, prevFamilies, dtSeconds }) {
  const { families, groups } = data;
  const lowerSearch = search.toLowerCase();

  const filteredGroups = [];
  for (const [prefix, names] of groups) {
    const matchedNames = names.filter(name => {
      if (!search) return true;
      const family = families.get(name);
      return name.toLowerCase().includes(lowerSearch) ||
        (family.help && family.help.toLowerCase().includes(lowerSearch));
    });
    if (matchedNames.length > 0) {
      filteredGroups.push({ prefix, names: matchedNames });
    }
  }

  filteredGroups.sort((a, b) => a.prefix.localeCompare(b.prefix));

  if (filteredGroups.length === 0) {
    return <div>No metrics match the search.</div>;
  }

  const autoExpand = search.length > 0 && filteredGroups.length <= 5;

  return (
    <div>
      <div style={{ marginBottom: '8px', color: '#666', fontSize: '13px' }}>
        {filteredGroups.length} groups, {filteredGroups.reduce((s, g) => s + g.names.length, 0)} metrics
        {dtSeconds && <span style={{ marginLeft: '8px', color: '#2980b9' }}>(rates over {dtSeconds.toFixed(1)}s)</span>}
      </div>
      {filteredGroups.map(({ prefix, names }) => (
        <MetricGroup
          key={prefix}
          prefix={prefix}
          names={names}
          families={families}
          defaultExpanded={autoExpand}
          prevFamilies={prevFamilies}
          dtSeconds={dtSeconds}
        />
      ))}
    </div>
  );
}

const POLL_INTERVALS = [
  { label: '1s', ms: 1000 },
  { label: '5s', ms: 5000 },
  { label: '10s', ms: 10000 },
  { label: '30s', ms: 30000 },
];

function MetricsApp() {
  const params = new URLSearchParams(location.search);
  const [endpoints, setEndpoints] = useState([]);
  const [selectedUrl, setSelectedUrl] = useState(params.get('endpoint') || '/metrics');
  const [snapshots, setSnapshots] = useState([]); // [{ families, groups, timestamp }, ...] (max 2)
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [search, setSearch] = useState(params.get('search') || '');
  const [polling, setPolling] = useState(false);
  const [pollInterval, setPollInterval] = useState(5000);
  const rawTextRef = useRef('');
  const fileInputRef = useRef(null);

  // Sync state to URL
  useEffect(() => {
    const p = new URLSearchParams();
    if (selectedUrl && selectedUrl !== '/metrics') p.set('endpoint', selectedUrl);
    if (search) p.set('search', search);
    const qs = p.toString();
    window.history.replaceState({}, '', qs ? `${location.pathname}?${qs}` : location.pathname);
  }, [selectedUrl, search]);

  // Derive current data and previous snapshot for delta computation
  const data = snapshots.length > 0 ? snapshots[snapshots.length - 1] : null;
  const prevFamilies = snapshots.length >= 2 ? snapshots[0].families : null;
  const dtSeconds = snapshots.length >= 2 ? (snapshots[1].timestamp - snapshots[0].timestamp) / 1000 : null;

  // Reset snapshots on endpoint change
  useEffect(() => {
    setSnapshots([]);
    setLoading(true);
  }, [selectedUrl]);

  useEffect(() => {
    discoverEndpoints().then(eps => {
      setEndpoints(eps);
    });
  }, []);

  const loadMetrics = useCallback((url) => {
    setError(null);
    fetchMetrics(url)
      .then(text => {
        rawTextRef.current = text;
        const parsed = parsePrometheusText(text);
        const snap = { ...parsed, timestamp: Date.now() };
        setSnapshots(prev => prev.length >= 2 ? [prev[1], snap] : [...prev, snap]);
        setLoading(false);
      })
      .catch(err => {
        setError(String(err));
        setLoading(false);
      });
  }, []);

  // Initial load
  useEffect(() => {
    loadMetrics(selectedUrl);
  }, [selectedUrl, loadMetrics]);

  // Poll for changes
  useEffect(() => {
    if (!polling) return;
    const id = setInterval(() => loadMetrics(selectedUrl), pollInterval);
    return () => clearInterval(id);
  }, [polling, pollInterval, selectedUrl, loadMetrics]);

  const saveToFile = useCallback(() => {
    const text = rawTextRef.current;
    if (!text) return;
    const blob = new Blob([text], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    const ts = new Date().toISOString().replace(/[:.]/g, '-');
    const ep = (endpoints.find(e => e.url === selectedUrl) || {}).label || 'unknown';
    const epSlug = ep.replace(/[^a-zA-Z0-9]+/g, '-');
    a.download = `metrics-${epSlug}-${ts}.txt`;
    a.click();
    URL.revokeObjectURL(url);
  }, []);

  const loadFromFile = useCallback((e) => {
    const file = e.target.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      const text = /** @type {string} */ (reader.result);
      rawTextRef.current = text;
      const parsed = parsePrometheusText(text);
      setSnapshots([{ ...parsed, timestamp: Date.now() }]);
      setLoading(false);
      setError(null);
    };
    reader.readAsText(file);
    e.target.value = '';
  }, []);

  const pollBtnStyle = polling
    ? { ...styles.button, background: '#e74c3c', color: '#fff', borderColor: '#c0392b' }
    : { ...styles.button, background: '#27ae60', color: '#fff', borderColor: '#1e8449' };

  return (
    <div style={styles.container}>
      <h2>Metrics Visualization</h2>
      <div style={styles.toolbar}>
        <label htmlFor="endpoint-select">Endpoint:</label>
        <select
          id="endpoint-select"
          style={styles.select}
          value={selectedUrl}
          onChange={e => setSelectedUrl(e.target.value)}
        >
          {endpoints.map(ep => (
            <option key={ep.url} value={ep.url}>{ep.label}</option>
          ))}
        </select>
        <div style={{ position: 'relative', display: 'inline-block' }}>
          <input
            type="text"
            style={{ ...styles.input, paddingRight: '24px' }}
            placeholder="Search metrics..."
            value={search}
            onChange={e => setSearch(e.target.value)}
            aria-label="search-box"
          />
          {search && (
            <button
              onClick={() => setSearch('')}
              style={{ position: 'absolute', right: '6px', top: '50%', transform: 'translateY(-50%)', border: 'none', background: 'none', cursor: 'pointer', fontSize: '14px', color: '#999', padding: '0 2px', lineHeight: 1 }}
              title="Clear search"
            >x</button>
          )}
        </div>
        <button style={styles.button} onClick={() => loadMetrics(selectedUrl)}>
          Refresh
        </button>
        <button style={pollBtnStyle} onClick={() => setPolling(s => !s)}>
          {polling ? 'Stop' : 'Poll'}
        </button>
        {polling && (
          <select
            style={styles.select}
            value={pollInterval}
            onChange={e => setPollInterval(Number(e.target.value))}
          >
            {POLL_INTERVALS.map(p => (
              <option key={p.ms} value={p.ms}>{p.label}</option>
            ))}
          </select>
        )}
        <button style={styles.button} onClick={saveToFile} disabled={!rawTextRef.current} title="Save metrics to file">
          Save
        </button>
        <button style={styles.button} onClick={() => fileInputRef.current.click()} title="Load metrics from file">
          Load
        </button>
        <input ref={fileInputRef} type="file" accept=".txt,.prom,text/plain" style={{ display: 'none' }} onChange={loadFromFile} />
      </div>
      {loading ? (
        <div>Loading metrics...</div>
      ) : error ? (
        <div style={{ color: 'red' }}>Error: {error}</div>
      ) : data ? (
        <MetricsContent data={data} search={search} prevFamilies={prevFamilies} dtSeconds={dtSeconds} />
      ) : null}
    </div>
  );
}

const content = document.getElementById('content');
ReactDOM.render(<MetricsApp />, content);
