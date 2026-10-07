// Copyright Materialize, Inc. and contributors. All rights reserved.
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.

// qpsbench measures closed-loop queries on persistent, independently routed
// connections. Configuration arrives on stdin so credentials never enter argv.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"math/bits"
	"os"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

type config struct {
	DSNs                []string `json:"dsns"`
	Concurrency         int      `json:"concurrency"`
	Protocol            string   `json:"protocol"`
	WarmupSeconds       float64  `json:"warmup_seconds"`
	DurationSeconds     float64  `json:"duration_seconds"`
	QueryTimeoutSeconds float64  `json:"query_timeout_seconds"`
	Query               string   `json:"query"`
	ExpectedRows        int      `json:"expected_rows"`
}

type histogram [64 * 16]uint64

func (h *histogram) add(d time.Duration) {
	n := uint64(d / time.Microsecond)
	if n == 0 {
		n = 1
	}
	e := bits.Len64(n) - 1
	base := uint64(1) << e
	h[e*16+int((n-base)*16/base)]++
}

// Percentiles are upper bounds in logarithmic buckets, within 6.25% or 1us.
func (h *histogram) percentile(p float64) float64 {
	var total, seen uint64
	for _, n := range h {
		total += n
	}
	if total == 0 {
		return 0
	}
	target := uint64(math.Ceil(float64(total) * p))
	for i, n := range h {
		seen += n
		if seen >= target {
			base := uint64(1) << uint(i/16)
			return float64(base+(base*uint64(i%16+1)+15)/16) / 1000
		}
	}
	panic("invalid histogram")
}

type clientStats struct {
	queries  uint64
	latency  time.Duration
	hist     histogram
	finished time.Time
	err      error
}

type result struct {
	StartedAt      string      `json:"started_at"`
	FinishedAt     string      `json:"finished_at"`
	Concurrency    int         `json:"concurrency"`
	Clusters       int         `json:"clusters"`
	Protocol       string      `json:"protocol"`
	Queries        uint64      `json:"queries"`
	Errors         int         `json:"errors"`
	ElapsedSeconds float64     `json:"elapsed_seconds"`
	QPS            float64     `json:"qps"`
	MeanLatencyMS  float64     `json:"mean_latency_ms"`
	P50LatencyMS   float64     `json:"p50_latency_ms"`
	P95LatencyMS   float64     `json:"p95_latency_ms"`
	P99LatencyMS   float64     `json:"p99_latency_ms"`
	Driver         driverStats `json:"driver"`
}

type queryFunc func(context.Context) (*sql.Rows, error)

func execute(ctx context.Context, query queryFunc, timeout time.Duration, expected int) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	rows, err := query(ctx)
	if err != nil {
		return err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if n != expected {
		return fmt.Errorf("expected %d rows, got %d", expected, n)
	}
	return nil
}

func validate(c config) error {
	if len(c.DSNs) == 0 || c.Concurrency < 1 || c.WarmupSeconds < 0 || c.DurationSeconds <= 0 || c.QueryTimeoutSeconds <= 0 || c.ExpectedRows < 0 || c.Query == "" {
		return fmt.Errorf("invalid QPS configuration")
	}
	if c.Protocol != "simple" && c.Protocol != "prepared" {
		return fmt.Errorf("protocol must be simple or prepared")
	}
	return nil
}

func benchmark(c config) (result, error) {
	return benchmarkWithDriver(c, "postgres")
}

func benchmarkWithDriver(c config, driverName string) (result, error) {
	if err := validate(c); err != nil {
		return result{}, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queries := make([]queryFunc, c.Concurrency)
	pools := make([]*sql.DB, c.Concurrency)
	defer func() {
		for _, db := range pools {
			if db != nil {
				_ = db.Close()
			}
		}
	}()
	// Each client owns its pool's only connection. Opening is bounded and occurs
	// before either timed phase, including prepared-statement construction.
	var open sync.WaitGroup
	var mu sync.Mutex
	var openErr error
	sem := make(chan struct{}, 32)
	for i := range queries {
		open.Add(1)
		go func(i int) {
			defer open.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			db, err := sql.Open(driverName, c.DSNs[i%len(c.DSNs)])
			pools[i] = db
			if err == nil {
				db.SetMaxOpenConns(1)
				db.SetMaxIdleConns(1)
				startup, stop := context.WithTimeout(ctx, time.Duration(c.QueryTimeoutSeconds*float64(time.Second)))
				err = db.PingContext(startup)
				if err == nil {
					if c.Protocol == "prepared" {
						var stmt *sql.Stmt
						stmt, err = db.PrepareContext(startup, c.Query)
						if err == nil {
							queries[i] = func(ctx context.Context) (*sql.Rows, error) { return stmt.QueryContext(ctx) }
						}
					} else {
						queries[i] = func(ctx context.Context) (*sql.Rows, error) { return db.QueryContext(ctx, c.Query) }
					}
				}
				stop()
			}
			mu.Lock()
			if err != nil && openErr == nil {
				openErr = err
				cancel()
			}
			mu.Unlock()
		}(i)
	}
	open.Wait()
	if openErr != nil {
		return result{}, fmt.Errorf("client startup failed: %w", openErr)
	}
	stats := make([]clientStats, c.Concurrency)
	warmupStart := time.Now()
	warmupEnd := warmupStart.Add(time.Duration(c.WarmupSeconds * float64(time.Second)))
	var warmed sync.WaitGroup
	warmed.Add(c.Concurrency)
	measure := make(chan struct{})
	var done sync.WaitGroup
	var start, end time.Time
	timeout := time.Duration(c.QueryTimeoutSeconds * float64(time.Second))
	for i := range queries {
		done.Add(1)
		go func(i int) {
			defer done.Done()
			s := &stats[i]
			for time.Now().Before(warmupEnd) && ctx.Err() == nil {
				if err := execute(ctx, queries[i], timeout, c.ExpectedRows); err != nil {
					s.err = err
					cancel()
					break
				}
			}
			warmed.Done()
			<-measure
			for time.Now().Before(end) && ctx.Err() == nil {
				qstart := time.Now()
				if err := execute(ctx, queries[i], timeout, c.ExpectedRows); err != nil {
					s.err = err
					cancel()
					break
				}
				elapsed := time.Since(qstart)
				s.queries++
				s.latency += elapsed
				s.hist.add(elapsed)
			}
			s.finished = time.Now()
		}(i)
	}
	warmed.Wait()
	driverStart := snapshot()
	lagDone := make(chan struct{})
	lagResult := make(chan histogram, 1)
	go sampleLag(lagDone, lagResult)
	start = time.Now()
	end = start.Add(time.Duration(c.DurationSeconds * float64(time.Second)))
	close(measure)
	done.Wait()
	close(lagDone)
	lag := <-lagResult
	driverEnd := snapshot()
	r := result{Concurrency: c.Concurrency, Clusters: minInt(len(c.DSNs), c.Concurrency), Protocol: c.Protocol}
	finish := end
	var h histogram
	var latency time.Duration
	for _, s := range stats {
		r.Queries += s.queries
		latency += s.latency
		for i, n := range s.hist {
			h[i] += n
		}
		if s.finished.After(finish) {
			finish = s.finished
		}
		if s.err != nil {
			r.Errors++
		}
	}
	r.ElapsedSeconds = finish.Sub(start).Seconds()
	r.StartedAt = start.UTC().Format(time.RFC3339Nano)
	r.FinishedAt = finish.UTC().Format(time.RFC3339Nano)
	r.QPS = float64(r.Queries) / r.ElapsedSeconds
	if r.Queries > 0 {
		r.MeanLatencyMS = float64(latency) / float64(time.Millisecond) / float64(r.Queries)
	}
	r.P50LatencyMS = h.percentile(0.50)
	r.P95LatencyMS = h.percentile(0.95)
	r.P99LatencyMS = h.percentile(0.99)
	r.Driver = describeDriver(driverStart, driverEnd, r.ElapsedSeconds, lag.percentile(.99))
	if r.Errors > 0 || r.Queries == 0 {
		return r, fmt.Errorf("invalid measurement: %d client errors, %d queries", r.Errors, r.Queries)
	}
	return r, nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	var c config
	if err := json.NewDecoder(os.Stdin).Decode(&c); err != nil {
		fmt.Fprintln(os.Stderr, "invalid JSON configuration")
		os.Exit(1)
	}
	r, err := benchmark(c)
	if encErr := json.NewEncoder(os.Stdout).Encode(r); encErr != nil {
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "QPS measurement failed (credentials and query errors omitted)")
		os.Exit(1)
	}
}
