// Copyright Materialize, Inc. and contributors. All rights reserved.
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.

package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

type testDriver struct {
	mu                sync.Mutex
	opens             map[string]int
	prepares, queries int
	rows              int
	fail              bool
}

type testConn struct{ d *testDriver }
type testStmt struct{ c *testConn }
type testRows struct{ remaining int }

func (d *testDriver) Open(dsn string) (driver.Conn, error) {
	d.mu.Lock()
	d.opens[dsn]++
	d.mu.Unlock()
	return &testConn{d}, nil
}
func (c *testConn) Prepare(string) (driver.Stmt, error) {
	c.d.mu.Lock()
	c.d.prepares++
	c.d.mu.Unlock()
	return &testStmt{c}, nil
}
func (c *testConn) Close() error              { return nil }
func (c *testConn) Begin() (driver.Tx, error) { return nil, fmt.Errorf("unexpected transaction") }
func (c *testConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(time.Millisecond):
	}
	c.d.mu.Lock()
	c.d.queries++
	c.d.mu.Unlock()
	if c.d.fail {
		return nil, fmt.Errorf("injected query failure")
	}
	return &testRows{c.d.rows}, nil
}
func (s *testStmt) Close() error  { return nil }
func (s *testStmt) NumInput() int { return 0 }
func (s *testStmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, fmt.Errorf("unexpected exec")
}
func (s *testStmt) Query([]driver.Value) (driver.Rows, error) {
	return s.c.QueryContext(context.Background(), "", nil)
}
func (s *testStmt) QueryContext(ctx context.Context, _ []driver.NamedValue) (driver.Rows, error) {
	return s.c.QueryContext(ctx, "", nil)
}
func (r *testRows) Columns() []string { return []string{"x"} }
func (r *testRows) Close() error      { return nil }
func (r *testRows) Next(values []driver.Value) error {
	if r.remaining == 0 {
		return io.EOF
	}
	r.remaining--
	values[0] = int64(r.remaining)
	return nil
}

func TestBenchmark(t *testing.T) {
	for _, protocol := range []string{"prepared", "simple"} {
		t.Run(protocol, func(t *testing.T) {
			d := &testDriver{opens: make(map[string]int), rows: 10}
			name := "qps-test-" + protocol
			sql.Register(name, d)
			c := config{DSNs: []string{"a", "b"}, Concurrency: 4, Protocol: protocol, WarmupSeconds: .03, DurationSeconds: .04, QueryTimeoutSeconds: 1, Query: "SELECT x", ExpectedRows: 10}
			r, err := benchmarkWithDriver(c, name)
			if err != nil {
				t.Fatal(err)
			}
			if d.opens["a"] != 2 || d.opens["b"] != 2 {
				t.Fatalf("connections not persistent or evenly routed: %v", d.opens)
			}
			wantPrepares := 0
			if protocol == "prepared" {
				wantPrepares = 4
			}
			if d.prepares != wantPrepares {
				t.Fatalf("prepares: %d, want %d", d.prepares, wantPrepares)
			}
			if r.Queries == 0 || uint64(d.queries) <= r.Queries {
				t.Fatalf("warmup not excluded: %+v, total queries %d", r, d.queries)
			}
			if r.ElapsedSeconds < c.DurationSeconds || r.QPS != float64(r.Queries)/r.ElapsedSeconds || r.MeanLatencyMS <= 0 || r.P99LatencyMS < r.P50LatencyMS {
				t.Fatalf("invalid measurement: %+v", r)
			}
		})
	}
}

func TestBenchmarkRejectsQueryFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		d := &testDriver{opens: make(map[string]int), rows: 9, fail: fail}
		name := fmt.Sprintf("qps-test-fail-%v", fail)
		sql.Register(name, d)
		c := config{DSNs: []string{"a"}, Concurrency: 1, Protocol: "prepared", DurationSeconds: .01, QueryTimeoutSeconds: 1, Query: "SELECT x", ExpectedRows: 10}
		r, err := benchmarkWithDriver(c, name)
		if err == nil || r.Errors == 0 {
			t.Fatalf("failed query accepted: %+v", r)
		}
	}
}

func TestHistogram(t *testing.T) {
	var h histogram
	for i := 1; i <= 100; i++ {
		h.add(time.Duration(i) * time.Millisecond)
	}
	for _, p := range []float64{.50, .95, .99} {
		v := h.percentile(p)
		if v < p*100 || v > p*100*1.0625+.001 {
			t.Fatalf("percentile %v: %v", p, v)
		}
	}
	if (&histogram{}).percentile(.99) != 0 {
		t.Fatal("empty histogram")
	}
}

func TestCgroupParsing(t *testing.T) {
	for s, want := range map[string]float64{"max 100000": 0, "200000 100000": 2, "-1 100000": 0, "oops": 0} {
		if got := parseQuota(s); got != want {
			t.Fatalf("quota %q: %v", s, got)
		}
	}
	a, b := parseCPUStat("usage_usec 900000\nnr_periods 100\nnr_throttled 12\nthrottled_usec 123")
	if a != 12 || b != 100 {
		t.Fatalf("stat: %d %d", a, b)
	}
}

func TestSaturationWarnings(t *testing.T) {
	a := driverSnapshot{cpu: 1, periods: 100, throttled: 1, quota: 1, available: true}
	b := driverSnapshot{cpu: 10, periods: 200, throttled: 11, quota: 1, available: true}
	d := describeDriver(a, b, 10, 12)
	if d.CPUPercent != 90 || d.ThrottledPeriodPercent != 10 || len(d.Warnings) != 3 {
		t.Fatalf("saturation: %+v", d)
	}
	b.cpu = 2
	b.throttled = 1
	d = describeDriver(a, b, 10, 1)
	if len(d.Warnings) != 0 {
		t.Fatalf("idle: %+v", d)
	}
}

func TestConfigValidation(t *testing.T) {
	c := config{DSNs: []string{"unused"}, Concurrency: 1, Protocol: "prepared", DurationSeconds: 1, QueryTimeoutSeconds: 1, Query: "SELECT 1", ExpectedRows: 1}
	if err := validate(c); err != nil {
		t.Fatal(err)
	}
	c.Protocol = "typo"
	if validate(c) == nil {
		t.Fatal("invalid protocol accepted")
	}
	c.Protocol = "simple"
	c.Concurrency = 0
	if validate(c) == nil {
		t.Fatal("zero clients accepted")
	}
}
