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

use std::{fmt, io};

use tracing::{Event, Subscriber};
use tracing_log::NormalizeEvent;
use tracing_subscriber::fmt::format::{JsonFields, Writer};
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

use crate::request_context;

pub(super) struct RequestJson;

struct IoWriter<'a, 'writer>(&'a mut Writer<'writer>);

impl io::Write for IoWriter<'_, '_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let text = std::str::from_utf8(bytes).map_err(io::Error::other)?;
        self.0.write_str(text).map_err(io::Error::other)?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn json(writer: &mut Writer<'_>, value: &impl serde::Serialize) -> fmt::Result {
    serde_json::to_writer(IoWriter(writer), value).map_err(|_| fmt::Error)
}

impl<S, N> FormatEvent<S, N> for RequestJson
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let normalized = event.normalized_metadata();
        let metadata = normalized.as_ref().unwrap_or_else(|| event.metadata());
        let mut timestamp = String::new();
        tracing_subscriber::fmt::time::SystemTime.format_time(&mut Writer::new(&mut timestamp))?;

        writer.write_str("{\"timestamp\":")?;
        json(&mut writer, &timestamp)?;
        write!(writer, ",\"level\":\"{}\",\"target\":", metadata.level())?;
        json(&mut writer, &metadata.target())?;
        writer.write_str(",\"fields\":")?;
        JsonFields::new().format_fields(writer.by_ref(), event)?;
        if let Some(context) = request_context::current() {
            writer.write_str(",\"request\":")?;
            json(&mut writer, &context)?;
        }
        writeln!(writer, "}}")
    }
}
