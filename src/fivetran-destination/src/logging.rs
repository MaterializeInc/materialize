// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::Serialize;
use tracing_core::{Event, Level, Subscriber};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{format, FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

use std::io;

/// Implements [`tracing_subscriber::fmt::FormatEvent`] according to the Fivetran SDK logging
/// format.
///
/// See: <https://github.com/fivetran/fivetran_sdk/blob/main/development-guide.md#logging>
pub struct FivetranLoggingFormat {
    origin: FivetranEventOrigin,
}

impl FivetranLoggingFormat {
    /// Returns a type that implements [`FormatEvent`] and is configured to log for a Fivetran
    /// Destination.
    pub fn destination() -> Self {
        FivetranLoggingFormat {
            origin: FivetranEventOrigin::SdkDestination,
        }
    }
}

impl<S, N> FormatEvent<S, N> for FivetranLoggingFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: format::Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        let mut message = String::new();
        ctx.format_fields(Writer::new(&mut message), event)?;

        let level = match *event.metadata().level() {
            // Mapping TRACE and DEBUG to "Info" isn't great, but Fivetran doesn't support any
            // other logging level.
            //
            // TODO(parkmycar): When we support pushing traces to our own logging infra, we should
            // filter out `TRACE` and `DEBUG` at this level.
            Level::TRACE | Level::DEBUG | Level::INFO => FivetranEventLevel::Info,
            Level::WARN => FivetranEventLevel::Warning,
            Level::ERROR => FivetranEventLevel::Severe,
        };

        let event = FivetranEvent {
            message,
            level,
            message_origin: self.origin,
        };
        let write_adapter = WriteAdapter::new(&mut writer);

        serde_json::to_writer(write_adapter, &event).map_err(|e| {
            // Last ditch effort to figure out why we failed to serialize, it's not guaranteed
            // we'll be able to see this.
            eprintln!("Failed to serialize logging event! err: {e}");
            std::fmt::Error
        })?;
        writer.write_char('\n')?;

        Ok(())
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
struct FivetranEvent {
    level: FivetranEventLevel,
    message: String,
    message_origin: FivetranEventOrigin,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "UPPERCASE")]
enum FivetranEventLevel {
    Info,
    Warning,
    Severe,
}

#[derive(Copy, Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum FivetranEventOrigin {
    SdkDestination,
    // TODO(parkmycar): One day we'll support this.
    // SdkConnector,
}

/// [`tracing_subscriber::fmt::format::Writer`] implements [`std::fmt::Write`] but
/// [`serde_json::to_writer`] requires a type that implements [`std::io::Write`] so this struct
/// acts as an adapter between the two `Write` traits.
struct WriteAdapter<'a> {
    writer: &'a mut dyn std::fmt::Write,
}

impl<'a> WriteAdapter<'a> {
    pub fn new(writer: &'a mut dyn std::fmt::Write) -> Self {
        WriteAdapter { writer }
    }
}

impl<'a> io::Write for WriteAdapter<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let s =
            std::str::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        self.writer
            .write_str(s)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(s.as_bytes().len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::FivetranLoggingFormat;

    use std::sync::{Arc, Mutex};

    use tracing_subscriber::fmt::MakeWriter;
    use tracing_subscriber::util::SubscriberInitExt;

    #[derive(Debug, Default, Clone)]
    struct TestWriter {
        writer: Arc<Mutex<Vec<u8>>>,
    }

    impl TestWriter {
        fn drain(&self) -> Vec<u8> {
            let mut guard = self.writer.lock().unwrap();
            guard.drain(..).collect()
        }
    }

    impl<'a> MakeWriter<'a> for TestWriter {
        type Writer = TestWriter;

        fn make_writer(&self) -> Self::Writer {
            TestWriter {
                writer: Arc::clone(&self.writer),
            }
        }
    }

    impl std::io::Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let mut guard = self.writer.lock().unwrap();
            guard.write(buf)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[mz_ore::test]
    fn smoketest_tracing_format() {
        let writer = TestWriter::default();

        let subscriber = tracing_subscriber::fmt::fmt()
            .with_writer(writer.clone())
            .with_ansi(false)
            .with_max_level(tracing_core::Level::TRACE)
            .event_format(FivetranLoggingFormat::destination())
            .finish();

        let _guard = subscriber.set_default();

        tracing::info!("hello world!");
        let msg = String::from_utf8(writer.drain()).unwrap();
        assert_eq!(
            msg.trim(),
            r#"{"level":"INFO","message":"hello world!","message-origin":"sdk_destination"}"#
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: `open` not available when isolation is enabled
    fn test_tracing_logging_levels() {
        let writer = TestWriter::default();

        let subscriber = tracing_subscriber::fmt::fmt()
            .with_writer(writer.clone())
            .with_ansi(false)
            .with_max_level(tracing_core::Level::TRACE)
            .event_format(FivetranLoggingFormat::destination())
            .finish();

        let _guard = subscriber.set_default();

        tracing::trace!("this is a low priority trace");
        let msg = String::from_utf8(writer.drain()).unwrap();
        insta::assert_snapshot!(
            msg,
            @r###"{"level":"INFO","message":"this is a low priority trace","message-origin":"sdk_destination"}"###
        );

        tracing::debug!(alert_count = 42, "good level for debug printing");
        let msg = String::from_utf8(writer.drain()).unwrap();
        insta::assert_snapshot!(
            msg,
            @r###"{"level":"INFO","message":"good level for debug printing alert_count=42","message-origin":"sdk_destination"}"###
        );

        #[derive(Debug)]
        #[allow(unused)]
        struct ComplexData {
            level: usize,
            msg: &'static str,
        }
        let data = ComplexData {
            level: 101,
            msg: "this is a nested message",
        };

        tracing::info!(?data, "hello world!");
        let msg = String::from_utf8(writer.drain()).unwrap();
        insta::assert_snapshot!(
            msg,
            @r###"{"level":"INFO","message":"hello world! data=ComplexData { level: 101, msg: \"this is a nested message\" }","message-origin":"sdk_destination"}"###
        );

        tracing::warn!("oh no something went wrong but we can try to recover");
        let msg = String::from_utf8(writer.drain()).unwrap();
        insta::assert_snapshot!(
            msg,
            @r###"{"level":"WARNING","message":"oh no something went wrong but we can try to recover","message-origin":"sdk_destination"}"###
        );

        tracing::error!("EEK! hopefully we never hit this");
        let msg = String::from_utf8(writer.drain()).unwrap();
        insta::assert_snapshot!(
            msg,
            @r###"{"level":"SEVERE","message":"EEK! hopefully we never hit this","message-origin":"sdk_destination"}"###
        );
    }
}
