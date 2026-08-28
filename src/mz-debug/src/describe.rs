// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A native rendering of `kubectl describe` for the kinds mz-debug dumps.
//!
//! Every kind shares the header (name, namespace, labels, annotations, owner,
//! age) and the trailing `Events` section. Events are joined in memory from a
//! namespace's Event list by `involvedObject`, so describing a namespace's
//! worth of objects costs no API calls beyond the lists already fetched for
//! the YAML dump. Kinds where the summary adds real value over the YAML get a
//! hand-written body; the rest render header and events only.

use std::collections::BTreeMap;
use std::fmt::Write;

use k8s_openapi::api::admissionregistration::v1::{
    MutatingWebhookConfiguration, ValidatingWebhookConfiguration,
};
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::core::v1::{
    ConfigMap, Container, ContainerState, ContainerStatus, Endpoints, EnvVar, Event, Node,
    PersistentVolume, PersistentVolumeClaim, Pod, PodSpec, PodTemplateSpec, Service,
    ServiceAccount, Volume,
};
use k8s_openapi::api::networking::v1::NetworkPolicy;
use k8s_openapi::api::rbac::v1::{Role, RoleBinding};
use k8s_openapi::api::storage::v1::StorageClass;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, Time};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use k8s_openapi::jiff::{SignedDuration, Timestamp};
use kube::Resource;
use mz_cloud_resources::crd::generated::cert_manager::certificates::Certificate;
use mz_cloud_resources::crd::materialize::v1alpha1::Materialize;

/// A kind that can be rendered in `kubectl describe` style.
///
/// The header and events are shared. `describe_body` writes the kind-specific
/// middle section, if any; the default writes nothing, which is the right
/// choice for kinds whose YAML is already the best summary of them.
pub trait DescribeResource: Resource<DynamicType = ()> {
    fn describe_body(&self, _out: &mut Writer) {}
}

/// Renders `object` followed by the events that refer to it.
///
/// `now` anchors the relative ages. Callers pass a single value for a whole
/// dump so every object in it is aged against the same instant.
pub fn describe<K: DescribeResource>(object: &K, events: &[Event], now: Timestamp) -> String {
    let mut out = Writer::new(now);
    let meta = object.meta();

    out.field("Name", meta.name.as_deref().unwrap_or("<unknown>"));
    if let Some(namespace) = &meta.namespace {
        out.field("Namespace", namespace);
    }
    out.map("Labels", meta.labels.as_ref());
    out.map("Annotations", meta.annotations.as_ref());
    if let Some(created) = &meta.creation_timestamp {
        out.field("Age", out.age(created));
    }
    if let Some(deleted) = &meta.deletion_timestamp {
        out.field("Deletion Timestamp", &deleted.0);
    }
    if let Some(owners) = &meta.owner_references {
        let owners: Vec<String> = owners
            .iter()
            .map(|owner| format!("{}/{}", owner.kind, owner.name))
            .collect();
        if !owners.is_empty() {
            out.field("Controlled By", owners.join(", "));
        }
    }

    object.describe_body(&mut out);

    let kind = K::kind(&());
    let mut own_events: Vec<&Event> = events
        .iter()
        .filter(|event| {
            let involved = &event.involved_object;
            involved.kind.as_deref() == Some(kind.as_ref())
                && involved.name == meta.name
                && (involved.uid.is_none() || meta.uid.is_none() || involved.uid == meta.uid)
                && (involved.namespace.is_none() || involved.namespace == meta.namespace)
        })
        .collect();
    own_events.sort_by_key(|event| event_last_seen(event));
    write_events(&mut out, &own_events);

    out.finish()
}

/// Indentation-aware text sink that renders aligned `Key:  value` blocks and
/// tables the way kubectl's tabwriter does.
pub struct Writer {
    out: String,
    indent: usize,
    now: Timestamp,
    /// Pending `key: value` pairs of the current block, flushed with aligned
    /// values when the block ends.
    pending: Vec<(String, String)>,
}

impl Writer {
    fn new(now: Timestamp) -> Self {
        Self {
            out: String::new(),
            indent: 0,
            now,
            pending: Vec::new(),
        }
    }

    fn finish(mut self) -> String {
        self.flush();
        self.out
    }

    /// Queues a `key: value` line in the current aligned block.
    pub fn field(&mut self, key: &str, value: impl ToString) {
        self.pending.push((key.to_owned(), value.to_string()));
    }

    /// Queues `key: value` only when `value` is present.
    pub fn opt_field<T: ToString>(&mut self, key: &str, value: Option<&T>) {
        if let Some(value) = value {
            self.field(key, value.to_string());
        }
    }

    /// Queues a map as a `key:` line followed by one `k=v` line per entry,
    /// or `<none>` when empty.
    pub fn map(&mut self, key: &str, map: Option<&BTreeMap<String, String>>) {
        let value = match map {
            Some(map) if !map.is_empty() => map
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("\n"),
            _ => "<none>".to_owned(),
        };
        self.field(key, value);
    }

    /// Queues a list rendered one entry per line, or `<none>` when empty.
    pub fn list<T: ToString>(&mut self, key: &str, items: &[T]) {
        let value = if items.is_empty() {
            "<none>".to_owned()
        } else {
            items
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("\n")
        };
        self.field(key, value);
    }

    /// Ends the current aligned block and writes a bare `key:` line that
    /// introduces an indented sub-block.
    pub fn section(&mut self, key: &str) {
        self.flush();
        let indent = " ".repeat(self.indent);
        writeln!(self.out, "{indent}{key}:").expect("writing to String cannot fail");
    }

    /// Runs `f` with the indentation increased by one level. The current
    /// aligned block is flushed on both sides so alignment never crosses an
    /// indentation boundary.
    pub fn indented(&mut self, f: impl FnOnce(&mut Writer)) {
        self.flush();
        self.indent += 2;
        f(self);
        self.flush();
        self.indent -= 2;
    }

    /// Writes an aligned table with a kubectl-style dashed underline row.
    pub fn table(&mut self, header: &[&str], rows: &[Vec<String>]) {
        self.flush();
        let widths: Vec<usize> = (0..header.len())
            .map(|col| {
                rows.iter()
                    .map(|row| row.get(col).map_or(0, |cell| cell.chars().count()))
                    .chain(std::iter::once(header[col].len()))
                    .max()
                    .unwrap_or(0)
            })
            .collect();
        let indent = " ".repeat(self.indent);
        let write_row = |out: &mut String, cells: &[String]| {
            let mut line = String::new();
            for (col, cell) in cells.iter().enumerate() {
                if col + 1 == cells.len() {
                    line.push_str(cell);
                } else {
                    line.push_str(&format!("{:<width$}  ", cell, width = widths[col]));
                }
            }
            writeln!(out, "{indent}{}", line.trim_end()).expect("writing to String cannot fail");
        };
        write_row(
            &mut self.out,
            &header.iter().map(|h| h.to_string()).collect::<Vec<_>>(),
        );
        write_row(
            &mut self.out,
            &header
                .iter()
                .map(|h| "-".repeat(h.len()))
                .collect::<Vec<_>>(),
        );
        for row in rows {
            write_row(&mut self.out, row);
        }
    }

    /// The elapsed time between `time` and the writer's `now`, in kubectl's
    /// compact form (`5m12s`, `3d`).
    pub fn age(&self, time: &Time) -> String {
        format_age(self.now.duration_since(time.0))
    }

    fn flush(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let indent = " ".repeat(self.indent);
        let width = self
            .pending
            .iter()
            .map(|(key, _)| key.len() + 1)
            .max()
            .unwrap_or(0);
        for (key, value) in self.pending.drain(..) {
            let mut lines = value.split('\n');
            let first = lines.next().unwrap_or("");
            writeln!(
                self.out,
                "{indent}{:<width$}  {}",
                format!("{key}:"),
                first,
                width = width
            )
            .expect("writing to String cannot fail");
            for line in lines {
                writeln!(self.out, "{indent}{:<width$}  {}", "", line, width = width)
                    .expect("writing to String cannot fail");
            }
        }
    }
}

/// Formats a duration the way kubectl does: the two most significant units
/// below a day, or whole days beyond that.
fn format_age(duration: SignedDuration) -> String {
    let secs = duration.as_secs().max(0);
    let (days, hours, mins, secs) = (
        secs / 86_400,
        secs % 86_400 / 3_600,
        secs % 3_600 / 60,
        secs % 60,
    );
    if days > 0 {
        format!("{days}d")
    } else if hours > 0 {
        format!("{hours}h{mins}m")
    } else if mins > 0 {
        format!("{mins}m{secs}s")
    } else {
        format!("{secs}s")
    }
}

fn event_last_seen(event: &Event) -> Option<Timestamp> {
    event
        .series
        .as_ref()
        .and_then(|series| series.last_observed_time.as_ref())
        .map(|time| time.0)
        .or_else(|| event.last_timestamp.as_ref().map(|time| time.0))
        .or_else(|| event.event_time.as_ref().map(|time| time.0))
        .or_else(|| event.first_timestamp.as_ref().map(|time| time.0))
}

fn write_events(out: &mut Writer, events: &[&Event]) {
    if events.is_empty() {
        out.field("Events", "<none>");
        return;
    }
    out.section("Events");
    let rows: Vec<Vec<String>> = events
        .iter()
        .map(|event| {
            let count = event
                .series
                .as_ref()
                .and_then(|series| series.count)
                .or(event.count)
                .unwrap_or(1);
            let last = event_last_seen(event);
            let first = event
                .first_timestamp
                .as_ref()
                .map(|time| time.0)
                .or_else(|| event.event_time.as_ref().map(|time| time.0));
            let age = match (last, first) {
                (Some(last), Some(first)) if count > 1 => format!(
                    "{} (x{count} over {})",
                    out.age(&Time(last)),
                    out.age(&Time(first))
                ),
                (Some(last), _) => out.age(&Time(last)),
                (None, _) => "<unknown>".to_owned(),
            };
            let from = event
                .source
                .as_ref()
                .and_then(|source| match (&source.component, &source.host) {
                    (Some(component), Some(host)) => Some(format!("{component}, {host}")),
                    (Some(component), None) => Some(component.clone()),
                    (None, Some(host)) => Some(host.clone()),
                    (None, None) => None,
                })
                .or_else(|| event.reporting_component.clone())
                .unwrap_or_default();
            vec![
                event.type_.clone().unwrap_or_default(),
                event.reason.clone().unwrap_or_default(),
                age,
                from,
                event
                    .message
                    .as_deref()
                    .map(|message| message.trim().replace('\n', " "))
                    .unwrap_or_default(),
            ]
        })
        .collect();
    out.indented(|out| out.table(&["Type", "Reason", "Age", "From", "Message"], &rows));
}

fn format_selector(selector: &LabelSelector) -> String {
    let mut parts: Vec<String> = selector
        .match_labels
        .iter()
        .flatten()
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    for expression in selector.match_expressions.iter().flatten() {
        let values = expression
            .values
            .as_ref()
            .map(|values| values.join(","))
            .unwrap_or_default();
        parts.push(match expression.operator.as_str() {
            "In" => format!("{} in ({values})", expression.key),
            "NotIn" => format!("{} notin ({values})", expression.key),
            "Exists" => expression.key.clone(),
            "DoesNotExist" => format!("!{}", expression.key),
            other => format!("{} {other} ({values})", expression.key),
        });
    }
    if parts.is_empty() {
        "<none>".to_owned()
    } else {
        parts.join(",")
    }
}

fn format_labels(labels: Option<&BTreeMap<String, String>>) -> String {
    match labels {
        Some(labels) if !labels.is_empty() => labels
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(","),
        _ => "<none>".to_owned(),
    }
}

fn format_int_or_string(value: &IntOrString) -> String {
    match value {
        IntOrString::Int(int) => int.to_string(),
        IntOrString::String(string) => string.clone(),
    }
}

fn format_quantities(quantities: Option<&BTreeMap<String, Quantity>>) -> Vec<String> {
    quantities
        .into_iter()
        .flatten()
        .map(|(name, quantity)| format!("{name}: {}", quantity.0))
        .collect()
}

/// Renders an environment variable the way kubectl does, naming the source
/// of indirect values without ever printing a secret's contents.
fn format_env_var(env: &EnvVar) -> String {
    if let Some(value) = &env.value {
        return format!("{}: {value}", env.name);
    }
    let Some(from) = &env.value_from else {
        return format!("{}:", env.name);
    };
    let source = if let Some(secret) = &from.secret_key_ref {
        format!(
            "<set to the key '{}' in secret '{}'>",
            secret.key, secret.name
        )
    } else if let Some(config_map) = &from.config_map_key_ref {
        format!(
            "<set to the key '{}' of config map '{}'>",
            config_map.key, config_map.name
        )
    } else if let Some(field) = &from.field_ref {
        format!("({})", field.field_path)
    } else if let Some(resource) = &from.resource_field_ref {
        format!("({})", resource.resource)
    } else {
        "<unknown source>".to_owned()
    };
    format!("{}: {source}", env.name)
}

/// One line naming the type of a volume and its backing object, if any.
fn format_volume_source(volume: &Volume) -> String {
    if let Some(config_map) = &volume.config_map {
        format!("ConfigMap (name={})", config_map.name)
    } else if let Some(secret) = &volume.secret {
        format!(
            "Secret (name={})",
            secret.secret_name.as_deref().unwrap_or("<unknown>")
        )
    } else if let Some(claim) = &volume.persistent_volume_claim {
        format!("PersistentVolumeClaim (claim={})", claim.claim_name)
    } else if let Some(empty_dir) = &volume.empty_dir {
        format!(
            "EmptyDir (medium={}, size_limit={})",
            empty_dir.medium.as_deref().unwrap_or("default"),
            empty_dir
                .size_limit
                .as_ref()
                .map_or("<unset>", |limit| limit.0.as_str())
        )
    } else if let Some(host_path) = &volume.host_path {
        format!("HostPath (path={})", host_path.path)
    } else if volume.projected.is_some() {
        "Projected".to_owned()
    } else if volume.downward_api.is_some() {
        "DownwardAPI".to_owned()
    } else if volume.ephemeral.is_some() {
        "Ephemeral".to_owned()
    } else if let Some(csi) = &volume.csi {
        format!("CSI (driver={})", csi.driver)
    } else {
        "<other>".to_owned()
    }
}

fn write_container_state(out: &mut Writer, key: &str, state: &ContainerState) {
    if let Some(running) = &state.running {
        out.field(key, "Running");
        out.indented(|out| out.opt_field("Started", running.started_at.as_ref().map(|t| &t.0)));
    } else if let Some(waiting) = &state.waiting {
        out.field(key, "Waiting");
        out.indented(|out| {
            out.opt_field("Reason", waiting.reason.as_ref());
            out.opt_field("Message", waiting.message.as_ref());
        });
    } else if let Some(terminated) = &state.terminated {
        out.field(key, "Terminated");
        out.indented(|out| {
            out.opt_field("Reason", terminated.reason.as_ref());
            out.opt_field("Message", terminated.message.as_ref());
            out.field("Exit Code", terminated.exit_code);
            out.opt_field("Signal", terminated.signal.as_ref());
            out.opt_field("Started", terminated.started_at.as_ref().map(|t| &t.0));
            out.opt_field("Finished", terminated.finished_at.as_ref().map(|t| &t.0));
        });
    }
}

/// Writes one container's spec and, when the pod reports one, its status.
fn write_container(out: &mut Writer, container: &Container, status: Option<&ContainerStatus>) {
    out.section(&container.name);
    out.indented(|out| {
        if let Some(status) = status {
            out.opt_field("Container ID", status.container_id.as_ref());
        }
        out.opt_field("Image", container.image.as_ref());
        if let Some(status) = status {
            out.field("Image ID", &status.image_id);
        }
        let ports: Vec<String> = container
            .ports
            .iter()
            .flatten()
            .map(|port| {
                format!(
                    "{}/{}{}",
                    port.container_port,
                    port.protocol.as_deref().unwrap_or("TCP"),
                    port.name
                        .as_deref()
                        .map(|name| format!(" ({name})"))
                        .unwrap_or_default()
                )
            })
            .collect();
        out.list("Ports", &ports);
        if let Some(command) = &container.command {
            out.list("Command", command);
        }
        if let Some(args) = &container.args {
            out.list("Args", args);
        }
        if let Some(status) = status {
            if let Some(state) = &status.state {
                write_container_state(out, "State", state);
            }
            if let Some(last_state) = &status.last_state {
                if last_state.running.is_some()
                    || last_state.waiting.is_some()
                    || last_state.terminated.is_some()
                {
                    write_container_state(out, "Last State", last_state);
                }
            }
            out.field("Ready", status.ready);
            out.field("Restart Count", status.restart_count);
        }
        if let Some(resources) = &container.resources {
            let limits = format_quantities(resources.limits.as_ref());
            if !limits.is_empty() {
                out.list("Limits", &limits);
            }
            let requests = format_quantities(resources.requests.as_ref());
            if !requests.is_empty() {
                out.list("Requests", &requests);
            }
        }
        for (key, probe) in [
            ("Liveness", &container.liveness_probe),
            ("Readiness", &container.readiness_probe),
            ("Startup", &container.startup_probe),
        ] {
            if let Some(probe) = probe {
                let target = if let Some(http) = &probe.http_get {
                    format!(
                        "http-get {}://:{}{}",
                        http.scheme.as_deref().unwrap_or("HTTP").to_lowercase(),
                        format_int_or_string(&http.port),
                        http.path.as_deref().unwrap_or("/")
                    )
                } else if let Some(tcp) = &probe.tcp_socket {
                    format!("tcp-socket :{}", format_int_or_string(&tcp.port))
                } else if let Some(exec) = &probe.exec {
                    format!(
                        "exec {}",
                        exec.command
                            .as_ref()
                            .map(|c| c.join(" "))
                            .unwrap_or_default()
                    )
                } else if let Some(grpc) = &probe.grpc {
                    format!("grpc :{}", grpc.port)
                } else {
                    "<unknown>".to_owned()
                };
                out.field(
                    key,
                    format!(
                        "{target} delay={}s timeout={}s period={}s #success={} #failure={}",
                        probe.initial_delay_seconds.unwrap_or(0),
                        probe.timeout_seconds.unwrap_or(1),
                        probe.period_seconds.unwrap_or(10),
                        probe.success_threshold.unwrap_or(1),
                        probe.failure_threshold.unwrap_or(3),
                    ),
                );
            }
        }
        let env: Vec<String> = container.env.iter().flatten().map(format_env_var).collect();
        out.list("Environment", &env);
        let mounts: Vec<String> = container
            .volume_mounts
            .iter()
            .flatten()
            .map(|mount| {
                format!(
                    "{} from {} ({})",
                    mount.mount_path,
                    mount.name,
                    if mount.read_only.unwrap_or(false) {
                        "ro"
                    } else {
                        "rw"
                    }
                )
            })
            .collect();
        out.list("Mounts", &mounts);
    });
}

/// Writes the spec-only parts of a pod: containers without status, volumes,
/// scheduling constraints. Shared by pods and pod templates.
fn write_pod_spec(out: &mut Writer, spec: &PodSpec, statuses: &[ContainerStatus]) {
    let status_for = |name: &str| statuses.iter().find(|status| status.name == name);
    if let Some(init_containers) = &spec.init_containers {
        if !init_containers.is_empty() {
            out.section("Init Containers");
            out.indented(|out| {
                for container in init_containers {
                    write_container(out, container, status_for(&container.name));
                }
            });
        }
    }
    out.section("Containers");
    out.indented(|out| {
        for container in &spec.containers {
            write_container(out, container, status_for(&container.name));
        }
    });
    match &spec.volumes {
        Some(volumes) if !volumes.is_empty() => {
            out.section("Volumes");
            out.indented(|out| {
                for volume in volumes {
                    out.field(&volume.name, format_volume_source(volume));
                }
            });
        }
        _ => out.field("Volumes", "<none>"),
    }
    out.opt_field("Service Account", spec.service_account_name.as_ref());
    out.opt_field("Priority Class Name", spec.priority_class_name.as_ref());
    out.opt_field("Scheduler", spec.scheduler_name.as_ref());
    out.map("Node-Selectors", spec.node_selector.as_ref());
    let tolerations: Vec<String> = spec
        .tolerations
        .iter()
        .flatten()
        .map(|toleration| {
            let mut text = toleration.key.clone().unwrap_or_default();
            if let Some(value) = &toleration.value {
                text.push_str(&format!("={value}"));
            }
            if let Some(effect) = &toleration.effect {
                text.push_str(&format!(":{effect}"));
            }
            if let Some(op) = &toleration.operator {
                text.push_str(&format!(" op={op}"));
            }
            if let Some(seconds) = toleration.toleration_seconds {
                text.push_str(&format!(" for {seconds}s"));
            }
            text
        })
        .collect();
    out.list("Tolerations", &tolerations);
}

fn write_pod_template(out: &mut Writer, template: &PodTemplateSpec) {
    out.section("Pod Template");
    out.indented(|out| {
        let meta = template.metadata.as_ref();
        out.field(
            "Labels",
            format_labels(meta.and_then(|m| m.labels.as_ref())),
        );
        out.field(
            "Annotations",
            format_labels(meta.and_then(|m| m.annotations.as_ref())),
        );
        if let Some(spec) = &template.spec {
            write_pod_spec(out, spec, &[]);
        }
    });
}

impl DescribeResource for Pod {
    fn describe_body(&self, out: &mut Writer) {
        let status = self.status.as_ref();
        if let Some(spec) = &self.spec {
            out.opt_field("Priority", spec.priority.as_ref());
            match (&spec.node_name, status.and_then(|s| s.host_ip.as_ref())) {
                (Some(node), Some(host_ip)) => out.field("Node", format!("{node}/{host_ip}")),
                (Some(node), None) => out.field("Node", node),
                (None, _) => out.field("Node", "<none>"),
            }
        }
        if let Some(status) = status {
            out.opt_field("Start Time", status.start_time.as_ref().map(|t| &t.0));
            out.field("Status", status.phase.as_deref().unwrap_or("<unknown>"));
            out.opt_field("Reason", status.reason.as_ref());
            out.opt_field("Message", status.message.as_ref());
            out.field("IP", status.pod_ip.as_deref().unwrap_or("<none>"));
            let ips: Vec<String> = status
                .pod_ips
                .iter()
                .flatten()
                .map(|ip| ip.ip.clone())
                .collect();
            out.list("IPs", &ips);
        }
        if let Some(spec) = &self.spec {
            let mut statuses: Vec<ContainerStatus> = Vec::new();
            if let Some(status) = status {
                statuses.extend(status.init_container_statuses.iter().flatten().cloned());
                statuses.extend(status.container_statuses.iter().flatten().cloned());
            }
            write_pod_spec(out, spec, &statuses);
        }
        if let Some(status) = status {
            let rows: Vec<Vec<String>> = status
                .conditions
                .iter()
                .flatten()
                .map(|condition| vec![condition.type_.clone(), condition.status.clone()])
                .collect();
            out.section("Conditions");
            out.indented(|out| out.table(&["Type", "Status"], &rows));
            out.opt_field("QoS Class", status.qos_class.as_ref());
        }
    }
}

impl DescribeResource for Deployment {
    fn describe_body(&self, out: &mut Writer) {
        let status = self.status.as_ref();
        if let Some(spec) = &self.spec {
            out.field("Selector", format_selector(&spec.selector));
            out.field(
                "Replicas",
                format!(
                    "{} desired | {} updated | {} total | {} available | {} unavailable",
                    spec.replicas.unwrap_or(1),
                    status.and_then(|s| s.updated_replicas).unwrap_or(0),
                    status.and_then(|s| s.replicas).unwrap_or(0),
                    status.and_then(|s| s.available_replicas).unwrap_or(0),
                    status.and_then(|s| s.unavailable_replicas).unwrap_or(0),
                ),
            );
            if let Some(strategy) = &spec.strategy {
                out.opt_field("StrategyType", strategy.type_.as_ref());
                if let Some(rolling) = &strategy.rolling_update {
                    out.field(
                        "RollingUpdateStrategy",
                        format!(
                            "{} max unavailable, {} max surge",
                            rolling
                                .max_unavailable
                                .as_ref()
                                .map_or_else(|| "25%".to_owned(), format_int_or_string),
                            rolling
                                .max_surge
                                .as_ref()
                                .map_or_else(|| "25%".to_owned(), format_int_or_string),
                        ),
                    );
                }
            }
            out.field("MinReadySeconds", spec.min_ready_seconds.unwrap_or(0));
            write_pod_template(out, &spec.template);
        }
        if let Some(status) = status {
            let rows: Vec<Vec<String>> = status
                .conditions
                .iter()
                .flatten()
                .map(|condition| {
                    vec![
                        condition.type_.clone(),
                        condition.status.clone(),
                        condition.reason.clone().unwrap_or_default(),
                    ]
                })
                .collect();
            out.section("Conditions");
            out.indented(|out| out.table(&["Type", "Status", "Reason"], &rows));
        }
    }
}

impl DescribeResource for StatefulSet {
    fn describe_body(&self, out: &mut Writer) {
        let status = self.status.as_ref();
        if let Some(spec) = &self.spec {
            out.field("Selector", format_selector(&spec.selector));
            out.field(
                "Replicas",
                format!(
                    "{} desired | {} total | {} ready | {} updated",
                    spec.replicas.unwrap_or(1),
                    status.map_or(0, |s| s.replicas),
                    status.and_then(|s| s.ready_replicas).unwrap_or(0),
                    status.and_then(|s| s.updated_replicas).unwrap_or(0),
                ),
            );
            if let Some(strategy) = &spec.update_strategy {
                out.opt_field("Update Strategy", strategy.type_.as_ref());
                if let Some(partition) = strategy
                    .rolling_update
                    .as_ref()
                    .and_then(|rolling| rolling.partition)
                {
                    out.field("Partition", partition);
                }
            }
            write_pod_template(out, &spec.template);
            let claims: Vec<String> = spec
                .volume_claim_templates
                .iter()
                .flatten()
                .map(|claim| {
                    let requested = claim
                        .spec
                        .as_ref()
                        .and_then(|spec| spec.resources.as_ref())
                        .and_then(|resources| resources.requests.as_ref())
                        .and_then(|requests| requests.get("storage"))
                        .map_or("<unset>", |quantity| quantity.0.as_str());
                    format!(
                        "{} (storage={requested}, class={})",
                        claim.metadata.name.as_deref().unwrap_or("<unknown>"),
                        claim
                            .spec
                            .as_ref()
                            .and_then(|spec| spec.storage_class_name.as_deref())
                            .unwrap_or("<default>"),
                    )
                })
                .collect();
            out.list("Volume Claims", &claims);
        }
    }
}

impl DescribeResource for ReplicaSet {
    fn describe_body(&self, out: &mut Writer) {
        let status = self.status.as_ref();
        if let Some(spec) = &self.spec {
            out.field("Selector", format_selector(&spec.selector));
            out.field(
                "Replicas",
                format!(
                    "{} current / {} desired | {} ready | {} available",
                    status.map_or(0, |s| s.replicas),
                    spec.replicas.unwrap_or(1),
                    status.and_then(|s| s.ready_replicas).unwrap_or(0),
                    status.and_then(|s| s.available_replicas).unwrap_or(0),
                ),
            );
            if let Some(template) = &spec.template {
                write_pod_template(out, template);
            }
        }
        if let Some(status) = status {
            let rows: Vec<Vec<String>> = status
                .conditions
                .iter()
                .flatten()
                .map(|condition| {
                    vec![
                        condition.type_.clone(),
                        condition.status.clone(),
                        condition.reason.clone().unwrap_or_default(),
                    ]
                })
                .collect();
            if !rows.is_empty() {
                out.section("Conditions");
                out.indented(|out| out.table(&["Type", "Status", "Reason"], &rows));
            }
        }
    }
}

impl DescribeResource for DaemonSet {
    fn describe_body(&self, out: &mut Writer) {
        if let Some(spec) = &self.spec {
            out.field("Selector", format_selector(&spec.selector));
            if let Some(strategy) = &spec.update_strategy {
                out.opt_field("Update Strategy", strategy.type_.as_ref());
            }
        }
        if let Some(status) = &self.status {
            out.field(
                "Desired Number of Nodes Scheduled",
                status.desired_number_scheduled,
            );
            out.field(
                "Current Number of Nodes Scheduled",
                status.current_number_scheduled,
            );
            out.field(
                "Number of Nodes Scheduled with Up-to-date Pods",
                status.updated_number_scheduled.unwrap_or(0),
            );
            out.field(
                "Number of Nodes Scheduled with Available Pods",
                status.number_available.unwrap_or(0),
            );
            out.field("Number of Nodes Misscheduled", status.number_misscheduled);
            out.field("Pods Ready", status.number_ready);
        }
        if let Some(spec) = &self.spec {
            write_pod_template(out, &spec.template);
        }
    }
}

impl DescribeResource for Service {
    fn describe_body(&self, out: &mut Writer) {
        if let Some(spec) = &self.spec {
            out.field("Selector", format_labels(spec.selector.as_ref()));
            out.field("Type", spec.type_.as_deref().unwrap_or("ClusterIP"));
            out.opt_field("IP Family Policy", spec.ip_family_policy.as_ref());
            if let Some(families) = &spec.ip_families {
                out.field("IP Families", families.join(","));
            }
            out.field("IP", spec.cluster_ip.as_deref().unwrap_or("<none>"));
            if let Some(ips) = &spec.cluster_ips {
                out.field("IPs", ips.join(","));
            }
            if let Some(external_ips) = &spec.external_ips {
                out.field("External IPs", external_ips.join(","));
            }
            if let Some(ingress) = self
                .status
                .as_ref()
                .and_then(|status| status.load_balancer.as_ref())
                .and_then(|lb| lb.ingress.as_ref())
            {
                let addresses: Vec<String> = ingress
                    .iter()
                    .filter_map(|entry| entry.ip.clone().or_else(|| entry.hostname.clone()))
                    .collect();
                out.list("LoadBalancer Ingress", &addresses);
            }
            for port in spec.ports.iter().flatten() {
                let protocol = port.protocol.as_deref().unwrap_or("TCP");
                let name = port.name.as_deref().unwrap_or("<unset>");
                out.field("Port", format!("{name}  {}/{protocol}", port.port));
                out.field(
                    "TargetPort",
                    format!(
                        "{}/{protocol}",
                        port.target_port
                            .as_ref()
                            .map_or_else(|| port.port.to_string(), format_int_or_string)
                    ),
                );
                if let Some(node_port) = port.node_port {
                    out.field("NodePort", format!("{name}  {node_port}/{protocol}"));
                }
            }
            out.opt_field("Session Affinity", spec.session_affinity.as_ref());
            out.opt_field(
                "External Traffic Policy",
                spec.external_traffic_policy.as_ref(),
            );
            out.opt_field(
                "Internal Traffic Policy",
                spec.internal_traffic_policy.as_ref(),
            );
        }
    }
}

impl DescribeResource for Node {
    fn describe_body(&self, out: &mut Writer) {
        let roles: Vec<String> = self
            .metadata
            .labels
            .iter()
            .flatten()
            .filter_map(|(key, _)| key.strip_prefix("node-role.kubernetes.io/"))
            .map(ToOwned::to_owned)
            .collect();
        out.list("Roles", &roles);
        if let Some(spec) = &self.spec {
            let taints: Vec<String> = spec
                .taints
                .iter()
                .flatten()
                .map(|taint| {
                    format!(
                        "{}{}:{}",
                        taint.key,
                        taint
                            .value
                            .as_deref()
                            .map(|value| format!("={value}"))
                            .unwrap_or_default(),
                        taint.effect
                    )
                })
                .collect();
            out.list("Taints", &taints);
            out.field("Unschedulable", spec.unschedulable.unwrap_or(false));
            out.opt_field("ProviderID", spec.provider_id.as_ref());
            if let Some(cidrs) = &spec.pod_cidrs {
                out.field("PodCIDRs", cidrs.join(","));
            } else {
                out.opt_field("PodCIDR", spec.pod_cidr.as_ref());
            }
        }
        if let Some(status) = &self.status {
            let rows: Vec<Vec<String>> = status
                .conditions
                .iter()
                .flatten()
                .map(|condition| {
                    vec![
                        condition.type_.clone(),
                        condition.status.clone(),
                        condition
                            .last_heartbeat_time
                            .as_ref()
                            .map(|t| t.0.to_string())
                            .unwrap_or_default(),
                        condition
                            .last_transition_time
                            .as_ref()
                            .map(|t| t.0.to_string())
                            .unwrap_or_default(),
                        condition.reason.clone().unwrap_or_default(),
                        condition.message.clone().unwrap_or_default(),
                    ]
                })
                .collect();
            out.section("Conditions");
            out.indented(|out| {
                out.table(
                    &[
                        "Type",
                        "Status",
                        "LastHeartbeatTime",
                        "LastTransitionTime",
                        "Reason",
                        "Message",
                    ],
                    &rows,
                )
            });
            out.section("Addresses");
            out.indented(|out| {
                for address in status.addresses.iter().flatten() {
                    out.field(&address.type_, &address.address);
                }
            });
            out.section("Capacity");
            out.indented(|out| {
                for (name, quantity) in status.capacity.iter().flatten() {
                    out.field(name, &quantity.0);
                }
            });
            out.section("Allocatable");
            out.indented(|out| {
                for (name, quantity) in status.allocatable.iter().flatten() {
                    out.field(name, &quantity.0);
                }
            });
            if let Some(info) = &status.node_info {
                out.section("System Info");
                out.indented(|out| {
                    out.field("Machine ID", &info.machine_id);
                    out.field("System UUID", &info.system_uuid);
                    out.field("Boot ID", &info.boot_id);
                    out.field("Kernel Version", &info.kernel_version);
                    out.field("OS Image", &info.os_image);
                    out.field("Operating System", &info.operating_system);
                    out.field("Architecture", &info.architecture);
                    out.field("Container Runtime Version", &info.container_runtime_version);
                    out.field("Kubelet Version", &info.kubelet_version);
                });
            }
        }
    }
}

impl DescribeResource for PersistentVolumeClaim {
    fn describe_body(&self, out: &mut Writer) {
        let status = self.status.as_ref();
        if let Some(spec) = &self.spec {
            out.field(
                "StorageClass",
                spec.storage_class_name.as_deref().unwrap_or("<default>"),
            );
            out.field(
                "Status",
                status
                    .and_then(|s| s.phase.as_deref())
                    .unwrap_or("<unknown>"),
            );
            out.field("Volume", spec.volume_name.as_deref().unwrap_or("<unbound>"));
            let capacity = status
                .and_then(|s| s.capacity.as_ref())
                .and_then(|capacity| capacity.get("storage"))
                .map(|quantity| quantity.0.clone())
                .or_else(|| {
                    spec.resources
                        .as_ref()
                        .and_then(|resources| resources.requests.as_ref())
                        .and_then(|requests| requests.get("storage"))
                        .map(|quantity| format!("{} (requested)", quantity.0))
                });
            out.field("Capacity", capacity.unwrap_or_else(|| "<unset>".to_owned()));
            out.field(
                "Access Modes",
                status
                    .and_then(|s| s.access_modes.as_ref())
                    .or(spec.access_modes.as_ref())
                    .map(|modes| modes.join(","))
                    .unwrap_or_else(|| "<none>".to_owned()),
            );
            out.field(
                "VolumeMode",
                spec.volume_mode.as_deref().unwrap_or("Filesystem"),
            );
        }
    }
}

impl DescribeResource for PersistentVolume {
    fn describe_body(&self, out: &mut Writer) {
        let status = self.status.as_ref();
        if let Some(spec) = &self.spec {
            out.field(
                "StorageClass",
                spec.storage_class_name.as_deref().unwrap_or("<none>"),
            );
            out.field(
                "Status",
                status
                    .and_then(|s| s.phase.as_deref())
                    .unwrap_or("<unknown>"),
            );
            out.opt_field("Reason", status.and_then(|s| s.reason.as_ref()));
            out.opt_field("Message", status.and_then(|s| s.message.as_ref()));
            out.field(
                "Claim",
                spec.claim_ref
                    .as_ref()
                    .map(|claim| {
                        format!(
                            "{}/{}",
                            claim.namespace.as_deref().unwrap_or(""),
                            claim.name.as_deref().unwrap_or("")
                        )
                    })
                    .unwrap_or_else(|| "<none>".to_owned()),
            );
            out.field(
                "Reclaim Policy",
                spec.persistent_volume_reclaim_policy
                    .as_deref()
                    .unwrap_or("<unset>"),
            );
            out.field(
                "Access Modes",
                spec.access_modes
                    .as_ref()
                    .map(|modes| modes.join(","))
                    .unwrap_or_else(|| "<none>".to_owned()),
            );
            out.field(
                "VolumeMode",
                spec.volume_mode.as_deref().unwrap_or("Filesystem"),
            );
            out.field(
                "Capacity",
                spec.capacity
                    .as_ref()
                    .and_then(|capacity| capacity.get("storage"))
                    .map_or("<unset>", |quantity| quantity.0.as_str()),
            );
            let source = if let Some(csi) = &spec.csi {
                format!(
                    "CSI (driver={}, volumeHandle={})",
                    csi.driver, csi.volume_handle
                )
            } else if let Some(local) = &spec.local {
                format!("Local (path={})", local.path)
            } else if let Some(host_path) = &spec.host_path {
                format!("HostPath (path={})", host_path.path)
            } else if let Some(nfs) = &spec.nfs {
                format!("NFS (server={}, path={})", nfs.server, nfs.path)
            } else {
                "<other>".to_owned()
            };
            out.field("Source", source);
        }
    }
}

impl DescribeResource for Event {}
impl DescribeResource for ConfigMap {}
impl DescribeResource for ServiceAccount {}
impl DescribeResource for Endpoints {}
impl DescribeResource for NetworkPolicy {}
impl DescribeResource for Role {}
impl DescribeResource for RoleBinding {}
impl DescribeResource for StorageClass {}
impl DescribeResource for MutatingWebhookConfiguration {}
impl DescribeResource for ValidatingWebhookConfiguration {}
impl DescribeResource for CustomResourceDefinition {}
impl DescribeResource for Certificate {}
impl DescribeResource for Materialize {}

#[cfg(test)]
mod tests {
    use k8s_openapi::api::core::v1::{
        ContainerPort, ContainerStateRunning, EventSource, ObjectReference, PodStatus, ServicePort,
        ServiceSpec,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use maplit::btreemap;

    use super::*;

    fn now() -> Timestamp {
        "2026-01-02T03:04:05Z".parse().unwrap()
    }

    fn time(rfc3339: &str) -> Time {
        Time(rfc3339.parse().unwrap())
    }

    fn event(kind: &str, name: &str, reason: &str, message: &str, last: &str) -> Event {
        Event {
            metadata: ObjectMeta {
                name: Some(format!("{name}.{reason}")),
                namespace: Some("ns".to_owned()),
                ..Default::default()
            },
            involved_object: ObjectReference {
                kind: Some(kind.to_owned()),
                name: Some(name.to_owned()),
                namespace: Some("ns".to_owned()),
                ..Default::default()
            },
            type_: Some("Normal".to_owned()),
            reason: Some(reason.to_owned()),
            message: Some(message.to_owned()),
            source: Some(EventSource {
                component: Some("kubelet".to_owned()),
                host: None,
            }),
            last_timestamp: Some(time(last)),
            first_timestamp: Some(time(last)),
            count: Some(1),
            ..Default::default()
        }
    }

    #[mz_ore::test]
    fn format_age_matches_kubectl_units() {
        assert_eq!(format_age(SignedDuration::from_secs(7)), "7s");
        assert_eq!(format_age(SignedDuration::from_secs(5 * 60 + 12)), "5m12s");
        assert_eq!(
            format_age(SignedDuration::from_secs(3 * 3600 + 2 * 60)),
            "3h2m"
        );
        assert_eq!(format_age(SignedDuration::from_secs(3 * 86_400 + 5)), "3d");
        assert_eq!(format_age(SignedDuration::from_secs(-30)), "0s");
    }

    #[mz_ore::test]
    fn pod_describe_golden() {
        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("envd-0".to_owned()),
                namespace: Some("ns".to_owned()),
                labels: Some(btreemap! {
                    "app".to_owned() => "environmentd".to_owned(),
                    "materialize.cloud/mz-resource-id".to_owned() => "abc".to_owned(),
                }),
                creation_timestamp: Some(time("2026-01-02T03:00:05Z")),
                ..Default::default()
            },
            spec: Some(PodSpec {
                node_name: Some("worker-1".to_owned()),
                service_account_name: Some("envd".to_owned()),
                containers: vec![Container {
                    name: "environmentd".to_owned(),
                    image: Some("materialize/environmentd:v1".to_owned()),
                    ports: Some(vec![ContainerPort {
                        container_port: 6875,
                        name: Some("sql".to_owned()),
                        ..Default::default()
                    }]),
                    args: Some(vec!["--flag".to_owned()]),
                    env: Some(vec![
                        EnvVar {
                            name: "PLAIN".to_owned(),
                            value: Some("1".to_owned()),
                            ..Default::default()
                        },
                        EnvVar {
                            name: "SECRET".to_owned(),
                            value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                secret_key_ref: Some(
                                    k8s_openapi::api::core::v1::SecretKeySelector {
                                        name: "backend".to_owned(),
                                        key: "password".to_owned(),
                                        optional: None,
                                    },
                                ),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ]),
                    ..Default::default()
                }],
                volumes: Some(vec![Volume {
                    name: "scratch".to_owned(),
                    empty_dir: Some(Default::default()),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some("Running".to_owned()),
                host_ip: Some("10.0.0.1".to_owned()),
                pod_ip: Some("10.1.0.7".to_owned()),
                start_time: Some(time("2026-01-02T03:00:10Z")),
                container_statuses: Some(vec![ContainerStatus {
                    name: "environmentd".to_owned(),
                    image: "materialize/environmentd:v1".to_owned(),
                    image_id: "sha256:deadbeef".to_owned(),
                    ready: true,
                    restart_count: 2,
                    state: Some(ContainerState {
                        running: Some(ContainerStateRunning {
                            started_at: Some(time("2026-01-02T03:00:12Z")),
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }]),
                qos_class: Some("Burstable".to_owned()),
                ..Default::default()
            }),
        };
        let events = vec![
            event(
                "Pod",
                "envd-0",
                "Pulled",
                "Pulled image",
                "2026-01-02T03:00:08Z",
            ),
            event(
                "Pod",
                "envd-0",
                "Started",
                "Started container",
                "2026-01-02T03:00:12Z",
            ),
            // Belongs to a different object and must be left out.
            event(
                "Pod",
                "envd-1",
                "Started",
                "Started container",
                "2026-01-02T03:00:12Z",
            ),
        ];

        let expected = "\
Name:         envd-0
Namespace:    ns
Labels:       app=environmentd
              materialize.cloud/mz-resource-id=abc
Annotations:  <none>
Age:          4m0s
Node:         worker-1/10.0.0.1
Start Time:   2026-01-02T03:00:10Z
Status:       Running
IP:           10.1.0.7
IPs:          <none>
Containers:
  environmentd:
    Image:     materialize/environmentd:v1
    Image ID:  sha256:deadbeef
    Ports:     6875/TCP (sql)
    Args:      --flag
    State:     Running
      Started:  2026-01-02T03:00:12Z
    Ready:          true
    Restart Count:  2
    Environment:    PLAIN: 1
                    SECRET: <set to the key 'password' in secret 'backend'>
    Mounts:         <none>
Volumes:
  scratch:  EmptyDir (medium=default, size_limit=<unset>)
Service Account:  envd
Node-Selectors:   <none>
Tolerations:      <none>
Conditions:
  Type  Status
  ----  ------
QoS Class:  Burstable
Events:
  Type    Reason   Age    From     Message
  ----    ------   ---    ----     -------
  Normal  Pulled   3m57s  kubelet  Pulled image
  Normal  Started  3m53s  kubelet  Started container
";
        assert_eq!(describe(&pod, &events, now()), expected);
    }

    #[mz_ore::test]
    fn service_describe_golden() {
        let service = Service {
            metadata: ObjectMeta {
                name: Some("mzabc-environmentd".to_owned()),
                namespace: Some("ns".to_owned()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                type_: Some("ClusterIP".to_owned()),
                cluster_ip: Some("None".to_owned()),
                selector: Some(btreemap! {"app".to_owned() => "environmentd".to_owned()}),
                ports: Some(vec![
                    ServicePort {
                        name: Some("sql".to_owned()),
                        port: 6875,
                        target_port: Some(IntOrString::Int(6875)),
                        protocol: Some("TCP".to_owned()),
                        ..Default::default()
                    },
                    ServicePort {
                        name: Some("https".to_owned()),
                        port: 6876,
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            }),
            status: None,
        };
        let expected = "\
Name:         mzabc-environmentd
Namespace:    ns
Labels:       <none>
Annotations:  <none>
Selector:     app=environmentd
Type:         ClusterIP
IP:           None
Port:         sql  6875/TCP
TargetPort:   6875/TCP
Port:         https  6876/TCP
TargetPort:   6876/TCP
Events:       <none>
";
        assert_eq!(describe(&service, &[], now()), expected);
    }

    #[mz_ore::test]
    fn header_only_kind_describe_golden() {
        let config_map = ConfigMap {
            metadata: ObjectMeta {
                name: Some("listeners".to_owned()),
                namespace: Some("ns".to_owned()),
                annotations: Some(btreemap! {"k".to_owned() => "v".to_owned()}),
                ..Default::default()
            },
            ..Default::default()
        };
        let events = vec![event(
            "ConfigMap",
            "listeners",
            "Synced",
            "multi\nline",
            "2026-01-02T03:04:00Z",
        )];
        let expected = "\
Name:         listeners
Namespace:    ns
Labels:       <none>
Annotations:  k=v
Events:
  Type    Reason  Age  From     Message
  ----    ------  ---  ----     -------
  Normal  Synced  5s   kubelet  multi line
";
        assert_eq!(describe(&config_map, &events, now()), expected);
    }

    #[mz_ore::test]
    fn repeated_events_show_count_and_span() {
        let mut repeated = event("ConfigMap", "cm", "Synced", "again", "2026-01-02T03:04:00Z");
        repeated.first_timestamp = Some(time("2026-01-02T02:04:00Z"));
        repeated.count = Some(7);
        let config_map = ConfigMap {
            metadata: ObjectMeta {
                name: Some("cm".to_owned()),
                namespace: Some("ns".to_owned()),
                ..Default::default()
            },
            ..Default::default()
        };
        let rendered = describe(&config_map, &[repeated], now());
        assert!(
            rendered.contains("5s (x7 over 1h0m)"),
            "unexpected age column in:\n{rendered}"
        );
    }
}
