// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { test, expect } from '@playwright/test';

// Load prometheus.js into a page context by navigating to metrics-viz
// (which includes the script), then run unit tests via page.evaluate.

test.describe('prometheus.js parser', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/metrics-viz');
    // Wait for scripts to load
    await page.waitForFunction(() => typeof parsePrometheusText === 'function');
  });

  test('parseSampleLine parses a simple metric', async ({ page }) => {
    const result = await page.evaluate(() => parseSampleLine('http_requests_total 1234'));
    expect(result).toEqual({ metricName: 'http_requests_total', labels: {}, value: 1234 });
  });

  test('parseSampleLine parses metric with labels', async ({ page }) => {
    const result = await page.evaluate(() =>
      parseSampleLine('http_requests_total{method="GET",status="200"} 42')
    );
    expect(result).toEqual({
      metricName: 'http_requests_total',
      labels: { method: 'GET', status: '200' },
      value: 42,
    });
  });

  test('parseSampleLine parses metric with timestamp', async ({ page }) => {
    const result = await page.evaluate(() =>
      parseSampleLine('http_requests_total 100 1625000000')
    );
    expect(result).toEqual({ metricName: 'http_requests_total', labels: {}, value: 100 });
  });

  test('parseSampleLine returns null for comment lines', async ({ page }) => {
    const result = await page.evaluate(() => parseSampleLine('# HELP foo bar'));
    expect(result).toBeNull();
  });

  test('parseSampleLine returns null for empty lines', async ({ page }) => {
    const result = await page.evaluate(() => parseSampleLine(''));
    expect(result).toBeNull();
  });

  test('parseLabels parses multiple labels', async ({ page }) => {
    const result = await page.evaluate(() =>
      parseLabels('method="GET",status="200",path="/api"')
    );
    expect(result).toEqual({ method: 'GET', status: '200', path: '/api' });
  });

  test('parseLabels handles empty string', async ({ page }) => {
    const result = await page.evaluate(() => parseLabels(''));
    expect(result).toEqual({});
  });

  test('parseLabels handles empty label value', async ({ page }) => {
    const result = await page.evaluate(() => parseLabels('key=""'));
    expect(result).toEqual({ key: '' });
  });

  test('parsePrometheusText parses a counter', async ({ page }) => {
    const result = await page.evaluate(() => {
      const text = [
        '# HELP http_requests_total Total HTTP requests',
        '# TYPE http_requests_total counter',
        'http_requests_total{method="GET"} 100',
        'http_requests_total{method="POST"} 50',
      ].join('\n');
      const { families, groups } = parsePrometheusText(text);
      const family = families.get('http_requests_total');
      return {
        name: family.name,
        help: family.help,
        type: family.type,
        seriesCount: family.series.length,
        series: family.series,
        labelNames: family.labelNames,
      };
    });
    expect(result.name).toBe('http_requests_total');
    expect(result.help).toBe('Total HTTP requests');
    expect(result.type).toBe('counter');
    expect(result.seriesCount).toBe(2);
    expect(result.labelNames).toEqual(['method']);
    expect(result.series).toEqual([
      { labels: { method: 'GET' }, value: 100 },
      { labels: { method: 'POST' }, value: 50 },
    ]);
  });

  test('parsePrometheusText parses a gauge', async ({ page }) => {
    const result = await page.evaluate(() => {
      const text = [
        '# TYPE temperature gauge',
        'temperature 23.5',
      ].join('\n');
      const { families } = parsePrometheusText(text);
      const family = families.get('temperature');
      return {
        type: family.type,
        series: family.series,
        labelNames: family.labelNames,
      };
    });
    expect(result.type).toBe('gauge');
    expect(result.series).toEqual([{ labels: {}, value: 23.5 }]);
    expect(result.labelNames).toEqual([]);
  });

  test('parsePrometheusText parses a histogram', async ({ page }) => {
    const result = await page.evaluate(() => {
      const text = [
        '# TYPE request_duration histogram',
        'request_duration_bucket{le="0.01"} 10',
        'request_duration_bucket{le="0.1"} 30',
        'request_duration_bucket{le="1"} 45',
        'request_duration_bucket{le="+Inf"} 50',
        'request_duration_sum 12.5',
        'request_duration_count 50',
      ].join('\n');
      const { families } = parsePrometheusText(text);
      const family = families.get('request_duration');
      return {
        type: family.type,
        histogramSeriesCount: family.histogramSeries.length,
        firstSeries: {
          sum: family.histogramSeries[0].sum,
          count: family.histogramSeries[0].count,
          average: family.histogramSeries[0].average,
          bucketCount: family.histogramSeries[0].deCumulatedBuckets.length,
          firstBucket: family.histogramSeries[0].deCumulatedBuckets[0],
        },
      };
    });
    expect(result.type).toBe('histogram');
    expect(result.histogramSeriesCount).toBe(1);
    expect(result.firstSeries.sum).toBe(12.5);
    expect(result.firstSeries.count).toBe(50);
    expect(result.firstSeries.average).toBe(0.25);
    expect(result.firstSeries.bucketCount).toBe(4);
    expect(result.firstSeries.firstBucket).toEqual({ le: 0.01, count: 10 });
  });

  test('parsePrometheusText groups metrics by prefix', async ({ page }) => {
    const result = await page.evaluate(() => {
      const text = [
        '# TYPE mz_compute_foo counter',
        'mz_compute_foo 1',
        '# TYPE mz_compute_bar counter',
        'mz_compute_bar 2',
        '# TYPE mz_storage_baz counter',
        'mz_storage_baz 3',
      ].join('\n');
      const { groups } = parsePrometheusText(text);
      return {
        groupKeys: [...groups.keys()].sort(),
        computeMetrics: groups.get('mz_compute'),
        storageMetrics: groups.get('mz_storage'),
      };
    });
    expect(result.groupKeys).toEqual(['mz_compute', 'mz_storage']);
    expect(result.computeMetrics).toEqual(['mz_compute_bar', 'mz_compute_foo']);
    expect(result.storageMetrics).toEqual(['mz_storage_baz']);
  });

  test('parsePrometheusText matches _total suffix to counter family', async ({ page }) => {
    const result = await page.evaluate(() => {
      const text = [
        '# TYPE http_requests counter',
        'http_requests_total{method="GET"} 100',
      ].join('\n');
      const { families } = parsePrometheusText(text);
      const family = families.get('http_requests');
      return {
        sampleCount: family.samples.length,
        seriesCount: family.series.length,
        value: family.series[0].value,
      };
    });
    expect(result.sampleCount).toBe(1);
    expect(result.seriesCount).toBe(1);
    expect(result.value).toBe(100);
  });

  test('getMetricPrefix extracts prefix correctly', async ({ page }) => {
    const results = await page.evaluate(() => ({
      mz: getMetricPrefix('mz_compute_foo_bar'),
      nonMz: getMetricPrefix('http_requests_total'),
      noUnderscore: getMetricPrefix('metric'),
    }));
    expect(results.mz).toBe('mz_compute');
    expect(results.nonMz).toBe('http');
    expect(results.noUnderscore).toBe('metric');
  });
});
