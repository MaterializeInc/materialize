// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Rejecting produced records that would become errors in their export.
//!
//! A source with `VALIDATE ON PRODUCE` registers each topic with an [`ExportValidator`]. The
//! Kafka server runs it on the task of the producer's connection, so validation is spread over
//! the replica's async runtime and does not run on the source's worker. It decodes each record
//! with the same decoders as the export's dataflow, and applies the checks of the export's
//! envelope that decoding alone does not.

use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use mz_kafka_server::{ProducedRecord, RecordValidator, Rejection, ResponseError};
use mz_ore::error::ErrorExt;
use mz_repr::Row;
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::errors::{CsrConnectError, DecodeError};
use mz_storage_types::sources::SourceExportDataConfig;
use mz_storage_types::sources::envelope::SourceEnvelope;

use crate::decode::{DataDecoder, decode_delimited, get_decoder};
use crate::metrics::decode::DecodeMetricDefs;

/// How many invalid records of a batch are described to the producer. Bounds the size of the
/// response to a batch that is invalid throughout.
const MAX_RECORD_ERRORS: usize = 100;

/// Checks produced records against an export's encoding and envelope.
#[derive(Debug)]
pub(super) struct ExportValidator {
    data_config: SourceExportDataConfig,
    debug_name: String,
    metrics: DecodeMetricDefs,
    storage_configuration: StorageConfiguration,
    /// Idle decoders. Decoders are stateful, so each concurrent validation takes its own.
    idle: Mutex<Vec<Decoders>>,
}

#[derive(Debug)]
struct Decoders {
    key: Option<DataDecoder>,
    value: DataDecoder,
}

impl ExportValidator {
    /// Returns a validator for an export with `data_config`, or `None` if every record is valid
    /// for it.
    pub(super) fn new(
        data_config: SourceExportDataConfig,
        debug_name: String,
        metrics: DecodeMetricDefs,
        storage_configuration: StorageConfiguration,
    ) -> Option<Arc<dyn RecordValidator>> {
        // Without an encoding, the dataflow passes every key and value through as bytes, so no
        // record becomes an error.
        data_config.encoding.as_ref()?;
        Some(Arc::new(ExportValidator {
            data_config,
            debug_name,
            metrics,
            storage_configuration,
            idle: Mutex::new(Vec::new()),
        }))
    }

    async fn decoders(&self) -> Result<Decoders, CsrConnectError> {
        let idle = self.idle.lock().expect("lock poisoned").pop();
        if let Some(decoders) = idle {
            return Ok(decoders);
        }
        let encoding = self.data_config.encoding.clone().expect("checked in new");
        let key = match encoding.key {
            Some(key) => Some(
                get_decoder(
                    key,
                    &self.debug_name,
                    true,
                    self.metrics.clone(),
                    &self.storage_configuration,
                )
                .await?,
            ),
            None => None,
        };
        let value = get_decoder(
            encoding.value,
            &self.debug_name,
            true,
            self.metrics.clone(),
            &self.storage_configuration,
        )
        .await?;
        Ok(Decoders { key, value })
    }

    /// Returns the index and error of each invalid record, at most [`MAX_RECORD_ERRORS`], and
    /// the number of invalid records.
    async fn check(
        &self,
        decoders: &mut Decoders,
        records: &[ProducedRecord],
    ) -> Result<(Vec<(usize, String)>, usize), CsrConnectError> {
        let mut errors = Vec::new();
        let mut invalid = 0;
        for (i, record) in records.iter().enumerate() {
            // As in `render_decode_delimited`.
            let key = match (&mut decoders.key, &record.key) {
                (Some(decoder), Some(buf)) => decode_delimited(decoder, buf).await?.transpose(),
                _ => None,
            };
            let value = match &record.value {
                Some(buf) => decode_delimited(&mut decoders.value, buf)
                    .await?
                    .transpose(),
                None => None,
            };
            if let Some(error) = envelope_error(&self.data_config.envelope, key, value) {
                invalid += 1;
                if errors.len() < MAX_RECORD_ERRORS {
                    errors.push((i, error));
                }
            }
        }
        Ok((errors, invalid))
    }
}

impl RecordValidator for ExportValidator {
    fn validate<'a>(
        &'a self,
        records: &'a [ProducedRecord],
    ) -> BoxFuture<'a, Result<(), Rejection>> {
        Box::pin(async move {
            let result = match self.decoders().await {
                Ok(mut decoders) => {
                    let result = self.check(&mut decoders, records).await;
                    // A decoder that failed transiently may be in any state, so it is dropped.
                    if result.is_ok() {
                        self.idle.lock().expect("lock poisoned").push(decoders);
                    }
                    result
                }
                Err(e) => Err(e),
            };
            match result {
                Ok((_, 0)) => Ok(()),
                Ok((errors, invalid)) => Err(Rejection {
                    error: ResponseError::InvalidRecord,
                    message: Some(format!(
                        "{invalid} of {} records are invalid for the table of this topic",
                        records.len()
                    )),
                    record_errors: errors,
                }),
                // The schema registry is unavailable, or does not know a schema ID: the Avro
                // decoder retries both for a while and then fails transiently, as it does in the
                // dataflow. The error is retriable, so the producer sends the batch again.
                Err(e) => Err(Rejection {
                    error: ResponseError::KafkaStorageError,
                    message: Some(format!(
                        "cannot validate records: {}",
                        e.display_with_causes()
                    )),
                    record_errors: Vec::new(),
                }),
            }
        })
    }
}

/// Checks a batch with each of several validators, and rejects it if any does.
#[derive(Debug)]
pub(super) struct AllOf(pub(super) Vec<Arc<dyn RecordValidator>>);

impl RecordValidator for AllOf {
    fn validate<'a>(
        &'a self,
        records: &'a [ProducedRecord],
    ) -> BoxFuture<'a, Result<(), Rejection>> {
        Box::pin(async move {
            for validator in &self.0 {
                validator.validate(records).await?;
            }
            Ok(())
        })
    }
}

/// Returns why a record with the decoded `key` and `value` is an error in an export with
/// `envelope`, following `upsert_commands` and `raise_key_value_errors` in
/// `render::sources`.
fn envelope_error(
    envelope: &SourceEnvelope,
    key: Option<Result<Row, DecodeError>>,
    value: Option<Result<Row, DecodeError>>,
) -> Option<String> {
    // NOTE: `VALUE DECODING ERRORS = INLINE` turns value errors into rows, which are still
    // rejected here.
    match (key, value) {
        (_, Some(Err(e))) => Some(format!("value: {}", e.kind)),
        (Some(Err(e)), _) => Some(format!("key: {}", e.kind)),
        (None, Some(Ok(_))) if matches!(envelope, SourceEnvelope::Upsert(_)) => {
            Some("the record has no key, which ENVELOPE UPSERT requires".into())
        }
        (Some(Ok(_)), None) if matches!(envelope, SourceEnvelope::None(_)) => {
            Some("the record has a key but no value".into())
        }
        _ => None,
    }
}
