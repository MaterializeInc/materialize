// Copyright Materialize, Inc. and contributors. All rights reserved.
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.

package main

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type driverSnapshot struct {
	cpu                float64
	throttled, periods uint64
	quota              float64
	available          bool
}
type driverStats struct {
	CPUCores               float64  `json:"cpu_cores"`
	CapacityCores          float64  `json:"capacity_cores"`
	CPUPercent             float64  `json:"cpu_percent"`
	ThrottledPeriodPercent float64  `json:"throttled_period_percent"`
	CgroupAvailable        bool     `json:"cgroup_available"`
	SchedulingLagP99MS     float64  `json:"scheduling_lag_p99_ms"`
	Warnings               []string `json:"warnings"`
}

func parseQuota(s string) float64 {
	a := strings.Fields(s)
	if len(a) != 2 || a[0] == "max" {
		return 0
	}
	q, _ := strconv.ParseFloat(a[0], 64)
	p, _ := strconv.ParseFloat(a[1], 64)
	if q <= 0 || p <= 0 {
		return 0
	}
	return q / p
}

func parseCPUStat(s string) (uint64, uint64) {
	var throttled, periods uint64
	for _, line := range strings.Split(s, "\n") {
		a := strings.Fields(line)
		if len(a) != 2 {
			continue
		}
		n, _ := strconv.ParseUint(a[1], 10, 64)
		if a[0] == "nr_throttled" {
			throttled = n
		}
		if a[0] == "nr_periods" {
			periods = n
		}
	}
	return throttled, periods
}

func snapshot() driverSnapshot {
	var r syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &r)
	s := driverSnapshot{cpu: float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6}
	if b, err := os.ReadFile("/sys/fs/cgroup/cpu.max"); err == nil {
		s.quota = parseQuota(string(b))
	}
	for _, root := range []string{"/sys/fs/cgroup", "/sys/fs/cgroup/cpu", "/sys/fs/cgroup/cpu,cpuacct"} {
		b, err := os.ReadFile(root + "/cpu.stat")
		if err != nil {
			continue
		}
		s.throttled, s.periods = parseCPUStat(string(b))
		s.available = true
		if q, err := os.ReadFile(root + "/cpu.cfs_quota_us"); err == nil {
			p, _ := os.ReadFile(root + "/cpu.cfs_period_us")
			s.quota = parseQuota(string(q) + " " + string(p))
		}
		break
	}
	return s
}

func describeDriver(a, b driverSnapshot, seconds, lag float64) driverStats {
	capacity := float64(runtime.NumCPU())
	if b.quota > 0 && b.quota < capacity {
		capacity = b.quota
	}
	d := driverStats{CPUCores: (b.cpu - a.cpu) / seconds, CapacityCores: capacity, CgroupAvailable: a.available && b.available, SchedulingLagP99MS: lag, Warnings: []string{}}
	d.CPUPercent = 100 * d.CPUCores / capacity
	if b.periods > a.periods && b.throttled >= a.throttled {
		d.ThrottledPeriodPercent = 100 * float64(b.throttled-a.throttled) / float64(b.periods-a.periods)
	}
	if d.CPUPercent >= 80 {
		d.Warnings = append(d.Warnings, "load driver uses at least 80% of its CPU capacity")
	}
	if d.ThrottledPeriodPercent >= 5 {
		d.Warnings = append(d.Warnings, "load driver is CPU-throttled in at least 5% of quota periods")
	}
	if lag >= 10 {
		d.Warnings = append(d.Warnings, "load driver scheduling lag p99 is at least 10ms")
	}
	if !d.CgroupAvailable {
		d.Warnings = append(d.Warnings, "load driver cgroup telemetry unavailable")
	}
	return d
}

func sampleLag(done <-chan struct{}, out chan<- histogram) {
	var h histogram
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case tick := <-t.C:
			h.add(time.Since(tick))
		case <-done:
			out <- h
			return
		}
	}
}
