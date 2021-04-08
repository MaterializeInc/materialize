// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::collections::HashMap;
use std::io::Write;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use flate2::write::GzEncoder;
use flate2::Compression as Flate2Compression;
use rusoto_core::{ByteStream, RusotoError};
use rusoto_s3::{
    CreateBucketConfiguration, CreateBucketError, CreateBucketRequest,
    GetBucketNotificationConfigurationRequest, PutBucketNotificationConfigurationRequest,
    PutObjectRequest, QueueConfiguration, S3,
};
use rusoto_sqs::{
    CreateQueueError, CreateQueueRequest, DeleteMessageBatchRequest,
    DeleteMessageBatchRequestEntry, GetQueueAttributesRequest, GetQueueUrlRequest,
    ReceiveMessageRequest, SetQueueAttributesRequest, Sqs,
};

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
            .create_bucket(CreateBucketRequest {
                bucket: bucket.clone(),
                create_bucket_configuration: match state.aws_region.name() {
                    "us-east-1" => None,
                    name => Some(CreateBucketConfiguration {
                        location_constraint: Some(name.to_string()),
                    }),
                },
                ..Default::default()
            })
            .await
        {
            Ok(_) | Err(RusotoError::Service(CreateBucketError::BucketAlreadyOwnedByYou(_))) => {
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
            .put_object(PutObjectRequest {
                bucket,
                body: Some(ByteStream::from(contents)),
                content_type: Some("application/octet-stream".to_string()),
                content_encoding: match self.compression {
                    Compression::None => None,
                    Compression::Gzip => Some("gzip".to_string()),
                },
                key: self.key.clone(),
                ..Default::default()
            })
            .await
            .map(|_| ())
            .map_err(|e| format!("putting s3 object: {}", e))
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
        .map(|t| parse_duration::parse(&t).map_err(|e| e.to_string()))
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
            .create_queue(CreateQueueRequest {
                queue_name: queue.clone(),
                ..Default::default()
            })
            .await;

        // get queue properties used for the rest of the mutations

        let queue_url = match result {
            Ok(r) => r
                .queue_url
                .expect("queue creation should always return the url"),
            Err(RusotoError::Service(CreateQueueError::QueueNameExists(q))) => {
                let resp = state
                    .sqs_client
                    .get_queue_url(GetQueueUrlRequest {
                        queue_name: q,
                        queue_owner_aws_account_id: None,
                    })
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

        let queue_arn = state
            .sqs_client
            .get_queue_attributes(GetQueueAttributesRequest {
                attribute_names: Some(vec!["QueueArn".to_string()]),
                queue_url: queue_url.clone(),
            })
            .await
            .map_err(|e| format!("getting queue {} attributes: {}", queue, e))?
            .attributes
            .ok_or_else(|| "the result should not be empty".to_string())?
            .remove("QueueArn")
            .ok_or_else(|| "QueueArn should be present in arn request".to_string())?;

        // Configure the queue to allow the S3 bucket to write to this queue
        let mut attributes = HashMap::new();
        attributes.insert(
            "Policy".to_string(),
            allow_s3_policy(&queue_arn, &bucket, &state.aws_account),
        );
        state
            .sqs_client
            .set_queue_attributes(SetQueueAttributesRequest {
                queue_url: queue_url.clone(),
                attributes,
            })
            .await
            .map_err(|e| format!("setting aws queue attributes: {}", e))?;

        state.sqs_queues_created.insert(queue_url.clone());

        // Configure the s3 bucket to write to the queue, without overwriting any existing configs
        let mut config = state
            .s3_client
            .get_bucket_notification_configuration(GetBucketNotificationConfigurationRequest {
                bucket: bucket.clone(),
                ..Default::default()
            })
            .await
            .map_err(|e| format!("getting bucket notification_configuration: {}", e))?;

        {
            let queue_configs = config.queue_configurations.get_or_insert_with(Vec::new);

            queue_configs.push(QueueConfiguration {
                events: self.events.clone(),
                queue_arn,
                ..Default::default()
            });
        }

        state
            .s3_client
            .put_bucket_notification_configuration(PutBucketNotificationConfigurationRequest {
                bucket: bucket.clone(),
                notification_configuration: config,
                ..Default::default()
            })
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
                .put_object(PutObjectRequest {
                    bucket: bucket.clone(),
                    body: Some(Vec::new().into()),
                    key: format!("sqs-test/{}", attempts),
                    ..Default::default()
                })
                .await
                .map_err(|e| format!("creating object to verify sqs: {}", e))?;
            attempts += 1;

            let resp = state
                .sqs_client
                .receive_message(ReceiveMessageRequest {
                    queue_url: queue_url.clone(),
                    wait_time_seconds: Some(1),
                    ..Default::default()
                })
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
                        .delete_message_batch(DeleteMessageBatchRequest {
                            queue_url: queue_url.to_string(),
                            entries: ms
                                .into_iter()
                                .enumerate()
                                .map(|(i, m)| DeleteMessageBatchRequestEntry {
                                    id: i.to_string(),
                                    receipt_handle: m.receipt_handle.unwrap(),
                                })
                                .collect(),
                        })
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
