// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::io::Write;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use aws_sdk_s3::error::{CreateBucketError, CreateBucketErrorKind};
use aws_sdk_s3::model::{
    BucketLocationConstraint, CreateBucketConfiguration, Delete, NotificationConfiguration,
    ObjectIdentifier, QueueConfiguration,
};
use aws_sdk_s3::{ByteStream, SdkError};
use aws_sdk_sqs::model::{DeleteMessageBatchRequestEntry, QueueAttributeName};
use flate2::write::GzEncoder;
use flate2::Compression as Flate2Compression;

use ore::result::ResultExt;

use crate::action::file::{build_compression, Compression};
use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct CreateBucketAction {
    bucket_prefix: String,
}

pub fn build_create_bucket(mut cmd: BuiltinCommand) -> Result<CreateBucketAction, String> {
    let bucket_prefix = format!("testdrive-{}", cmd.args.string("bucket")?);
    cmd.args.done()?;
    Ok(CreateBucketAction { bucket_prefix })
}

#[async_trait]
impl Action for CreateBucketAction {
    async fn undo(&self, _state: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let bucket = format!("{}-{}", self.bucket_prefix, state.seed);
        println!("Creating S3 bucket {}", bucket);

        match state
            .s3_client
            .create_bucket()
            .bucket(&bucket)
            .set_create_bucket_configuration(match state.aws_region() {
                "us-east-1" => None,
                name => Some(
                    CreateBucketConfiguration::builder()
                        .location_constraint(BucketLocationConstraint::from(name))
                        .build(),
                ),
            })
            .send()
            .await
        {
            Ok(_)
            | Err(SdkError::ServiceError {
                err:
                    CreateBucketError {
                        kind: CreateBucketErrorKind::BucketAlreadyOwnedByYou(_),
                        ..
                    },
                ..
            }) => {
                state.s3_buckets_created.insert(bucket);
                Ok(())
            }
            Err(e) => Err(format!("creating bucket: {}", e)),
        }
    }
}

pub struct PutObjectAction {
    bucket_prefix: String,
    key: String,
    compression: Compression,
    contents: String,
}

pub fn build_put_object(mut cmd: BuiltinCommand) -> Result<PutObjectAction, String> {
    let bucket_prefix = format!("testdrive-{}", cmd.args.string("bucket")?);
    let key = cmd.args.string("key")?;
    let compression = build_compression(&mut cmd)?;
    let contents = cmd.input.join("\n");
    cmd.args.done()?;
    Ok(PutObjectAction {
        bucket_prefix,
        key,
        compression,
        contents,
    })
}

#[async_trait]
impl Action for PutObjectAction {
    async fn undo(&self, _state: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let bucket = format!("{}-{}", self.bucket_prefix, state.seed);
        println!("Put S3 object {}/{}", bucket, self.key);

        let buffer = self.contents.clone().into_bytes();
        let contents = match self.compression {
            Compression::None => Ok(buffer),
            Compression::Gzip => {
                let mut encoder = GzEncoder::new(Vec::new(), Flate2Compression::default());
                encoder
                    .write_all(buffer.as_ref())
                    .map_err(|e| format!("error writing bytes to encoder: {}", e))?;
                encoder
                    .finish()
                    .map_err(|e| format!("error compressing contents: {}", e))
            }
        }?;

        state
            .s3_client
            .put_object()
            .bucket(bucket)
            .body(ByteStream::from(contents))
            .content_type("application/octet-stream")
            .set_content_encoding(match self.compression {
                Compression::None => None,
                Compression::Gzip => Some("gzip".to_string()),
            })
            .key(&self.key)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("putting s3 object: {}", e))
    }
}

pub struct DeleteObjectAction {
    bucket_prefix: String,
    keys: Vec<String>,
}

pub fn build_delete_object(mut cmd: BuiltinCommand) -> Result<DeleteObjectAction, String> {
    let bucket_prefix = format!("testdrive-{}", cmd.args.string("bucket")?);
    cmd.args.done()?;
    Ok(DeleteObjectAction {
        bucket_prefix,
        keys: cmd.input,
    })
}

#[async_trait]
impl Action for DeleteObjectAction {
    async fn undo(&self, _state: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let bucket = format!("{}-{}", self.bucket_prefix, state.seed);
        println!("Deleting S3 objects {}: {}", bucket, self.keys.join(", "));
        let result = state
            .s3_client
            .delete_objects()
            .bucket(bucket)
            .delete(
                Delete::builder()
                    .set_objects(Some(
                        self.keys
                            .iter()
                            .cloned()
                            .map(|key| ObjectIdentifier::builder().key(key).build())
                            .collect(),
                    ))
                    .build(),
            )
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("deleting s3 objects: {}", e));
        result
    }
}

pub struct AddBucketNotifications {
    bucket_prefix: String,
    queue_prefix: String,
    events: Vec<String>,
    sqs_validation_timeout: Option<Duration>,
}

pub fn build_add_notifications(mut cmd: BuiltinCommand) -> Result<AddBucketNotifications, String> {
    let bucket_prefix = format!("testdrive-{}", cmd.args.string("bucket")?);
    let queue_prefix = format!("testdrive-{}", cmd.args.string("queue")?);
    let events = cmd
        .args
        .opt_string("events")
        .map(|a| a.split(',').map(|s| s.to_string()).collect())
        .unwrap_or_else(|| vec!["s3:ObjectCreated:*".to_string()]);
    let sqs_validation_timeout = cmd
        .args
        .opt_string("sqs-validation-timeout")
        .map(|t| repr::util::parse_duration(&t).map_err_to_string())
        .transpose()?;
    cmd.args.done()?;
    Ok(AddBucketNotifications {
        bucket_prefix,
        queue_prefix,
        events,
        sqs_validation_timeout,
    })
}

#[async_trait]
impl Action for AddBucketNotifications {
    async fn undo(&self, _state: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let bucket = format!("{}-{}", self.bucket_prefix, state.seed);
        let queue = format!("{}-{}", self.queue_prefix, state.seed);

        let result = state
            .sqs_client
            .create_queue()
            .queue_name(&queue)
            .send()
            .await;

        // get queue properties used for the rest of the mutations

        let queue_url = match result {
            Ok(r) => r
                .queue_url
                .expect("queue creation should always return the url"),
            Err(SdkError::ServiceError { err, .. }) if err.is_queue_name_exists() => {
                let resp = state
                    .sqs_client
                    .get_queue_url()
                    .queue_name(&queue)
                    .send()
                    .await
                    .map_err(|e| {
                        format!(
                            "when trying to get sqs queue url for already-existing queue: {}",
                            e
                        )
                    })?;
                resp.queue_url
                    .expect("successfully getting the url gets the url")
            }
            Err(e) => return Err(e.to_string()),
        };

        let queue_arn: String = state
            .sqs_client
            .get_queue_attributes()
            .queue_url(&queue_url)
            .attribute_names("QueueArn")
            .send()
            .await
            .map_err(|e| format!("getting queue {} attributes: {}", queue, e))?
            .attributes
            .ok_or_else(|| "the result should not be empty".to_string())?
            .remove(&QueueAttributeName::QueueArn)
            .ok_or_else(|| "QueueArn should be present in arn request".to_string())?;

        // Configure the queue to allow the S3 bucket to write to this queue
        state
            .sqs_client
            .set_queue_attributes()
            .queue_url(&queue_url)
            .attributes(
                "Policy",
                allow_s3_policy(&queue_arn, &bucket, &state.aws_account),
            )
            .send()
            .await
            .map_err(|e| format!("setting aws queue attributes: {}", e))?;

        state.sqs_queues_created.insert(queue_url.clone());

        // Configure the s3 bucket to write to the queue, without overwriting any existing configs
        let mut config = state
            .s3_client
            .get_bucket_notification_configuration()
            .bucket(&bucket)
            .send()
            .await
            .map_err(|e| format!("getting bucket notification_configuration: {}", e))?;

        {
            let queue_configs = config.queue_configurations.get_or_insert_with(Vec::new);

            queue_configs.push(
                QueueConfiguration::builder()
                    .set_events(Some(self.events.iter().map(|e| e.into()).collect()))
                    .queue_arn(queue_arn)
                    .build(),
            );
        }

        state
            .s3_client
            .put_bucket_notification_configuration()
            .bucket(&bucket)
            .notification_configuration(
                NotificationConfiguration::builder()
                    .set_topic_configurations(config.topic_configurations)
                    .set_queue_configurations(config.queue_configurations)
                    .set_lambda_function_configurations(config.lambda_function_configurations)
                    .build(),
            )
            .send()
            .await
            .map_err(|e| {
                format!(
                    "Putting s3 bucket configuration notification {} \n{:?}",
                    e, e
                )
            })?;

        let sqs_validation_timeout = self
            .sqs_validation_timeout
            .unwrap_or_else(|| cmp::max(state.default_timeout, Duration::from_secs(120)));

        // Wait until we are sure that the configuration has taken effect
        //
        // AWS doesn't specify anywhere how long it should take for
        // newly-configured buckets to start generating sqs notifications, so
        // we continuously put new objects into the bucket and wait for any
        // message to show up.

        let mut attempts = 0;
        let mut success = false;
        print!(
            "Verifying SQS notification configuration for up to {:?} ",
            sqs_validation_timeout
        );
        let start = Instant::now();
        while start.elapsed() < sqs_validation_timeout {
            state
                .s3_client
                .put_object()
                .bucket(&bucket)
                .body(ByteStream::from_static(&[]))
                .key(format!("sqs-test/{}", attempts))
                .send()
                .await
                .map_err(|e| format!("creating object to verify sqs: {}", e))?;
            attempts += 1;

            let resp = state
                .sqs_client
                .receive_message()
                .queue_url(&queue_url)
                .wait_time_seconds(1)
                .send()
                .await
                .map_err(|e| format!("reading from sqs for verification: {}", e))?;

            if let Some(ms) = resp.messages {
                if !ms.is_empty() {
                    let found_real_message = ms
                        .iter()
                        .any(|m| m.body.as_ref().unwrap().contains("ObjectCreated:Put"));
                    if found_real_message {
                        success = true;
                    }
                    state
                        .sqs_client
                        .delete_message_batch()
                        .queue_url(&queue_url)
                        .set_entries(Some(
                            ms.into_iter()
                                .enumerate()
                                .map(|(i, m)| {
                                    DeleteMessageBatchRequestEntry::builder()
                                        .id(i.to_string())
                                        .receipt_handle(m.receipt_handle.unwrap())
                                        .build()
                                })
                                .collect(),
                        ))
                        .send()
                        .await
                        .map_err(|e| format!("Deleting validation messages from sqs: {}", e))?;
                }
            }
            if success {
                break;
            }

            print!(".");
        }
        if success {
            println!(
                " Success! (in {} attempts and {:?})",
                attempts + 1,
                start.elapsed()
            );
            Ok(())
        } else {
            println!(
                " Error, never got messages (after {} attempts and {:?})",
                attempts + 1,
                start.elapsed()
            );
            Err("Never got messages on S3 bucket notification queue".to_string())
        }
    }
}

fn allow_s3_policy(queue_arn: &str, bucket: &str, self_account: &str) -> String {
    format!(
        r#"{{
 "Version": "2012-10-17",
 "Id": "AllowS3Pushing",
 "Statement": [
  {{
   "Sid": "AllowS3Pushing",
   "Effect": "Allow",
   "Principal": {{
    "AWS":"*"
   }},
   "Action": [
    "SQS:SendMessage"
   ],
   "Resource": "{queue_arn}",
   "Condition": {{
      "ArnLike": {{ "aws:SourceArn": "arn:aws:s3:*:*:{bucket}" }},
      "StringEquals": {{ "aws:SourceAccount": "{self_account}" }}
   }}
  }}
 ]
}}"#,
        queue_arn = queue_arn,
        bucket = bucket,
        self_account = self_account
    )
}
