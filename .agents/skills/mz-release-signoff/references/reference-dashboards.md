# Reference dashboards metric reference

Two dashboards that the release bot files under `Reference`: `release-health`, UID `zKe0K0N4z`, and `networking`, UID `bHQE8bN4k`. Neither belongs to a database team, and neither is organized around comparing one release against another. They answer a different question: is the fleet available, and did the rollout itself proceed. Use them to establish that the deploy landed and nothing is on fire, then use the team dashboards to look for regressions.

## Scoping differs from every other dashboard

`release-health` keys on `$organization`, an organization id rather than a namespace, and constructs namespaces from it as `environment-$organization-0` or `environment-${organization}-.`. It also takes `$mz_cluster`, the EKS cluster, and `$version`. Many of its panels are deliberately fleet-wide with no environment filter at all, because they measure regional availability.

`networking` keys on `$namespace`, `$pod`, and `$tenant`. Most of its panels are node-level or cluster-level rather than environment-level.

Nearly every `release-health` panel excludes end-to-end test organizations with `mz_context_org_type!="e2e_test"`. Reproduce that exclusion or the availability percentages will be wrong.

## Roster: release-health

### Rollout progress

These come from the cloud control plane, not from Materialize, and carry no namespace label.

| Metric | Type | Notes |
|---|---|---|
| `running_deploys` | gauge, by `deploy_id`, `mz_cloud_stack_name`, `state` | One series per in-flight deploy. |
| `running_environment_rollouts` | gauge, by `deploy_id`, `state`, `organization_id` | Panels exclude `state="Deprovisioned"`, and the per-environment view filters `state="RollingOut"`. |
| `running_node_replacements` | gauge, by `deploy_id`, `state`, `node_id` | Panels exclude `NoChanges` and `Deleted`. |
| `environmentd_needs_update` | gauge | Count of `environmentd` instances pending a restart. The cleanest single indicator of rollout progress: it jumps to the fleet size when a deploy starts and returns to zero when it finishes. |

### Availability

| Metric | Type | Notes |
|---|---|---|
| `v2_mz_can_connect` | gauge, 0 or 1 | Drives `Minutes of SLA downtime`. |
| `v2_mz_views_query_successful` | gauge, 0 or 1 | Drives `Minutes of SHOW VIEWS downtime`. |
| `mz_external_envd_up` | gauge, 0 or 1 | Reachability from outside the VPC, averaged by `mz_aws_region`. |
| `v2_mz_envd_up` | gauge | Also on the adapter dashboard. |
| `mz_envd_up` | gauge, with `mz_version` | Used only by the CPU-throttling panel, which compares the set of organizations up an hour ago against the set up now via `unless`. |

The downtime panels compute `sum_over_time((1 - max by (org) (metric))[$__range:])`. That counts samples where the metric was zero, so the result is only "minutes" if the scrape interval is one minute. Read it as a sample count and convert deliberately.

### Restarts and crashloops

| Metric | Notes |
|---|---|
| `kube_pod_container_status_restarts_total` | Split into `container!~"clusterd"` and `container="clusterd"` panels, each divided by 100 in the display. |
| `kube_pod_container_status_waiting_reason{reason="CrashLoopBackOff"}` | The direct crashloop indicator. |
| `kube_pod_container_status_last_terminated_exitcode` | The non-clusterd panel joins against `!= 166`, so exit code 166 is treated as an expected termination here. Note the contrast with the compute dashboard, which looks for 137, an OOM kill. |
| `kube_pod_created` | Used by `New clusterd restarts` to bound pod age. |

`New clusterd restarts` is the most intricate expression on the dashboard. It selects pods younger than 86400 seconds that have restarted, strips the generation suffix into a `pod_base` label, and joins that against the same `pod_base` in an older generation which did not restart in the last 12 hours. The effect is to surface restarts that are new to the current generation, which is exactly the release question. It is also fragile, because it depends on the pod name matching `.*-(cluster-.*)-gen-([0-9]+)-[0-9]+$`.

### Rehydration

| Metric | Notes |
|---|---|
| `mz_dataflow_initial_output_duration_seconds` | Present once a dataflow has produced output. `sgn()` of it over the count of series gives the hydrated fraction. |
| `mz_compute_collection_count` | Carries a `hydrated` label. The by-generation panel extracts `gen` from `environmentd_materialize_cloud_service_id` with the pattern `.*-replica-.*-gen-(.*)`, which is how the new generation's hydration is separated from the old one's. |

This is the only place hydration progress is broken out per generation, which makes it the right dashboard for answering whether the new version hydrated as fast as the old one.

### External dependencies

`crdb_dedicated_sys_cpu_combined_percent_normalized` and `crdb_dedicated_capacity_used / crdb_dedicated_capacity` describe the regional CockroachDB cluster and cannot be attributed to one environment. `mz_parameter_frontend_last_cse_time_seconds` and `_last_sse_time_seconds` are LaunchDarkly sync freshness, compared as `timestamp(metric) - metric` against 60 or 600 seconds, so they measure staleness rather than a value. `mz_connection_status` by `status` and `source` covers upstream connection health. `cilium_bpf_map_pressure` and `container_tasks_state{state="running"}` cover the data plane.

## Roster: networking

| Metric | Type | Notes |
|---|---|---|
| `mz_balancer_connection_status` | counter, by `status` and `source` | `source` is `pgwire` or `https`; `status` is `success` or `error`. |
| `mz_balancer_connection_active` | gauge, by `source` | |
| `mz_balancer_metadata_seconds` | gauge | Counting series gives the balancer count, the same trick persist uses. |
| `mz_balancer_tenant_connection_active`, `_rx`, `_tx` | gauge, counter, by `tenant` | Per-tenant balancer traffic. |
| `mz_auth_session_request_count` | counter, by `existing_session` | Values `new`, `active`, `pending`. The panel titles explain the balancerd session cache semantics in full and are worth reading once. |
| `mz_external_calls_count` | counter, by `status`, `connection_type`, `job` | The uptime-checker panels filter `job="external-uptime-checker"`. |
| `mz_external_long_lived_connection_count` | counter, by `status` | |
| `mz_cloud_egress_check_reachability_count` | counter, by `k8s_app`, `result` | Synthetic egress reachability. |
| `cilium_drop_count_total` | counter, by `direction`, `reason` | The panel excludes system clusters with `mz_cluster!~".*-sys"`. |
| `cilium_bpf_map_pressure`, `cilium_bpf_map_ops_total` | gauge, counter, by `map_name` | |
| `cilium_endpoint_regeneration_time_stats_seconds_bucket` | histogram, by `scope` | |
| `cilium_datapath_signals_handled_total` | counter, by `signal` | Panel excludes `status="muted"`. |
| `cilium_node_connectivity_latency_seconds` | gauge, by `protocol` | |
| `container_network_receive_errors_total`, `_transmit_errors_total`, `_receive_packets_dropped_total`, `_transmit_packets_dropped_total` | counter | Pod and node network faults. |
| `node_network_receive_bytes_total`, `_transmit_bytes_total` | counter, `device="eth0"` | Egress gateway traffic. |
| `node_netstat_Tcp_InErrs`, `node_netstat_TcpExt_TCPTimeouts` | counter | Node TCP errors and timeouts. |
| `machine_memory_bytes` | gauge, `workload="materialize-egress"` | |

Three panels hardcode organization ids in `materialize_cloud_organization_id=~"3b1aeb7c-...|b65cc970-..."`, one staging environment and the production analytics canary. These will rot when the canary set changes, and they are not driven by the dashboard variables.

## Hazards

**The egress panels use `^0` as a set-membership filter.** Expressions such as `... * on(node) group_left (workload) (sum by (node, workload) (rate(...)) ^0)` raise the right side to the power zero, which yields 1 for every series that exists and drops nodes where it does not. It is an intersection filter, not arithmetic, so do not simplify it away.

**Downtime panels count samples, not minutes.** See the availability section above.

**Availability metrics are booleans averaged into percentages.** A single environment failing for one scrape moves the fleet percentage by a fraction of a percent, which is why the numbers read as 99.98 rather than 99. Convert to affected-environment-samples before deciding whether a dip matters.

**Balancer errors are not rare.** Production us-east-1 sustained 2.9 to 5.3 errors per second against 5.2 to 21.3 successes, an error fraction around a half, flat across the release boundary. Whatever `status="error"` counts on the balancer, it is a normal part of steady state here, so only a change in the ratio is informative.

**Crashloop series are absent when there are none.** Both the clusterd and non-clusterd crashloop queries returned no series at all across the whole window in production us-east-1. Absent is the healthy case and must not be reported as "checked and zero" without confirming the metric exists elsewhere.

**`environmentd_needs_update` is the rollout clock.** It sat at zero except for one bucket at 74.4 during the production rollout, and under 1.0 during canary upgrades. If it is non-zero when you start a sign-off, the rollout you are trying to evaluate has not finished.

## Measured baselines

Measured 2026-08-19 across production us-east-1, region-wide rather than restricted to the canary environments, twelve hour buckets. Boundary for the fleet was the v26.37.0 rollout on 2026-08-13; canary moved to v26.38.0-rc.1 on 2026-08-15 and rc.3 on 2026-08-18.

| Metric | Value |
|---|---|
| `v2_mz_can_connect`, mean | 100%, one bucket at 99.982% during the fleet rollout |
| `v2_mz_views_query_successful`, mean | 100%, one bucket at 99.982% |
| `mz_external_envd_up`, mean | 100% throughout |
| CrashLoopBackOff containers, clusterd and other | no series, that is none |
| Non-clusterd restarts per s | 0 to 0.0035, peak in the rollout bucket |
| `environmentd_needs_update` | 0, 74.4 during the fleet rollout, 0.44 to 0.96 at canary upgrades |
| CRDB CPU, max normalized | 0.45 to 0.59 |
| CRDB disk used, fraction | 0.032 to 0.038 |
| Upstream connection errors per s | 0.059 to 0.32 |
| Cilium BPF map pressure, max | 0.10 to 0.15 |
| Balancer errors per s | 2.9 to 5.3 |
| Balancer successes per s | 5.2 to 21.3 |
| Balancer active connections | 718 to 1333 |
| Cilium drops per s | 1.0 to 7.9 |
| Pod network errors per s | 5.8 to 29.9 |
| Pod network packets dropped per s | 0, three buckets at 0.010 to 0.027 |
| External calls failed per s | 0, one bucket at 9e-5 |
| Egress reachability checks per s | 1.0, dipping to 0.985 |

Staging was not measured for these two dashboards. The availability and control-plane metrics there describe a fleet nobody is paged for, so their baselines would carry little weight; the rollout-progress metrics are worth checking in staging only to confirm a deploy finished.
