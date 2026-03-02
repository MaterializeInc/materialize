use crate::location::{
    CaSResult, Consensus, Determinate, ExternalError, Indeterminate, ResultStream, SeqNo,
    VersionedData,
};
use anyhow::anyhow;
use async_trait::async_trait;
use aws_config::meta::region::ProvideRegion;
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::operation::create_table::CreateTableError;
use aws_sdk_dynamodb::operation::delete_item::builders::DeleteItemFluentBuilder;
use aws_sdk_dynamodb::operation::get_item::GetItemError;
use aws_sdk_dynamodb::operation::get_item::builders::GetItemFluentBuilder;
use aws_sdk_dynamodb::operation::put_item::PutItemError;
use aws_sdk_dynamodb::operation::put_item::builders::PutItemFluentBuilder;
use aws_sdk_dynamodb::operation::query::builders::QueryFluentBuilder;
use aws_sdk_dynamodb::operation::scan::ScanError;
use aws_sdk_dynamodb::operation::scan::builders::ScanFluentBuilder;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::operation::update_item::builders::UpdateItemFluentBuilder;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, ConsumedCapacity, KeySchemaElement, KeyType,
    ProvisionedThroughput, ReturnValue, ScalarAttributeType,
};
use aws_sdk_s3::error::SdkError;
use aws_types::region::Region;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use mz_ore::url::SensitiveUrl;
use std::collections::HashMap;
use std::pin::pin;

type AttrMap = HashMap<String, AttributeValue>;

const KEY: &str = "k";
const OFFSET: &str = "i";
const DATA: &str = "d";
const HEAD: &str = "h";
const TAIL: &str = "t";

fn parse_string(v: &AttributeValue) -> anyhow::Result<String> {
    let s = v.as_s().map_err(|e| anyhow!("non_string attr: {e:?}"))?;
    Ok(s.to_owned())
}
fn format_string(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn format_offset(value: Option<SeqNo>) -> AttributeValue {
    AttributeValue::N(value.map_or("-1".to_string(), |v| v.0.to_string()))
}

fn parse_seqno(v: &AttributeValue) -> anyhow::Result<SeqNo> {
    let n = v.as_n().map_err(|v| anyhow!("non-numeric seqno {v:?}"))?;
    Ok(SeqNo(
        n.parse()
            .map_err(|e| anyhow!("unparseable seqno {n}: {e}"))?,
    ))
}

fn format_seqno(value: SeqNo) -> AttributeValue {
    AttributeValue::N(value.0.to_string())
}

fn parse_data(v: &AttributeValue) -> anyhow::Result<Bytes> {
    let bytes = v
        .as_b()
        .map_err(|v| anyhow!("non-bytes data value {v:?}"))?;
    Ok(Bytes::copy_from_slice(bytes.as_ref()))
}

fn format_data(v: &[u8]) -> AttributeValue {
    AttributeValue::B(v.into())
}

fn parse_attr<T>(
    hash: &AttrMap,
    attr: &str,
    parse: impl FnOnce(&AttributeValue) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    let attr = hash
        .get(attr)
        .ok_or_else(|| anyhow!("missing attr {attr}"))?;
    parse(attr)
}

fn parse_state(hash: &AttrMap) -> anyhow::Result<State> {
    Ok(State {
        head: parse_attr(hash, HEAD, parse_seqno)?,
        tail: parse_attr(hash, TAIL, parse_seqno)?,
    })
}

fn parse_entry(hash: &AttrMap) -> anyhow::Result<VersionedData> {
    Ok(VersionedData {
        seqno: parse_attr(hash, OFFSET, parse_seqno)?,
        data: parse_attr(hash, DATA, parse_data)?,
    })
}

impl<E, R> From<SdkError<E, R>> for ExternalError
where
    ExternalError: From<E>,
{
    fn from(value: SdkError<E, R>) -> ExternalError {
        match value {
            SdkError::ServiceError(e) => e.into_err().into(),
            e @ SdkError::ConstructionFailure(_) => {
                Determinate::new(anyhow!("construction failure: {e}")).into()
            }
            e => Indeterminate::new(anyhow!("{e}")).into(),
        }
    }
}

impl From<CreateTableError> for ExternalError {
    fn from(value: CreateTableError) -> Self {
        Indeterminate::new(anyhow!(value)).into()
    }
}

impl From<GetItemError> for ExternalError {
    fn from(value: GetItemError) -> Self {
        Indeterminate::new(anyhow!(value)).into()
    }
}

impl From<PutItemError> for ExternalError {
    fn from(value: PutItemError) -> Self {
        if value.is_conditional_check_failed_exception() {
            Determinate::new(anyhow!(value)).into()
        } else {
            Indeterminate::new(anyhow!(value)).into()
        }
    }
}

impl From<ScanError> for ExternalError {
    fn from(value: ScanError) -> Self {
        Indeterminate::new(anyhow!(value)).into()
    }
}

impl From<UpdateItemError> for ExternalError {
    fn from(value: UpdateItemError) -> Self {
        if value.is_conditional_check_failed_exception() {
            Determinate::new(anyhow!(value)).into()
        } else {
            Indeterminate::new(anyhow!(value)).into()
        }
    }
}

#[derive(Debug, Clone)]
struct State {
    head: SeqNo,
    tail: SeqNo,
}

/// Dynamodb-backed consensus.
#[derive(Debug)]
pub struct DynamoConsensus {
    client: aws_sdk_dynamodb::Client,
    table_name: String,
}

impl DynamoConsensus {
    /// Opens the given location for non-exclusive read-write access.
    pub async fn open(url: SensitiveUrl) -> Result<Self, ExternalError> {
        let mut loader = mz_aws_util::defaults();
        if let Some(host) = url.host_str() {
            loader = loader.endpoint_url(format!(
                "http://{host}:{port}",
                port = url.port().unwrap_or(8000)
            ));
        }

        if let (username, Some(pass)) = (url.username(), url.password()) {
            let region: Box<dyn ProvideRegion> = Box::new(Region::new("local"));
            loader = loader
                .credentials_provider(Credentials::from_keys(username, pass, None))
                .region(region);
        };

        let config = loader.load().await;
        let client = aws_sdk_dynamodb::Client::new(&config);
        let table_name = url
            .path()
            .strip_prefix('/')
            .expect("path validation")
            .to_string();
        let this = Self { client, table_name };

        let result = this
            .client
            .create_table()
            .table_name(&this.table_name)
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(KEY)
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(OFFSET)
                    .key_type(KeyType::Range)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(KEY)
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(OFFSET)
                    .attribute_type(ScalarAttributeType::N)
                    .build()
                    .unwrap(),
            )
            .provisioned_throughput(
                ProvisionedThroughput::builder()
                    .read_capacity_units(1000)
                    .write_capacity_units(1000)
                    .build()
                    .unwrap(),
            )
            .send()
            .await;

        match result {
            Ok(_) => Ok(this),
            // OK if the table already exists - ideally it will be managed externally.
            Err(SdkError::ServiceError(e)) if e.err().is_resource_in_use_exception() => Ok(this),
            Err(e) => Err(e)?,
        }
    }

    fn consumed_capacity(&self, cap: Option<ConsumedCapacity>) {}

    async fn get(
        &self,
        key: &str,
        offset: Option<SeqNo>,
        mut builder: impl FnMut(GetItemFluentBuilder) -> GetItemFluentBuilder,
    ) -> anyhow::Result<Option<AttrMap>> {
        let get = self.client.get_item();
        let get = builder(get)
            .table_name(&self.table_name)
            .key(KEY, format_string(key))
            .key(OFFSET, format_offset(offset))
            .consistent_read(true)
            .send()
            .await?;

        self.consumed_capacity(get.consumed_capacity);

        Ok(get.item)
    }

    async fn put(
        &self,
        key: &str,
        offset: Option<SeqNo>,
        mut builder: impl FnMut(PutItemFluentBuilder) -> PutItemFluentBuilder,
    ) -> anyhow::Result<CaSResult> {
        let put = self.client.put_item();
        let put = builder(put)
            .table_name(&self.table_name)
            .item(KEY, format_string(key))
            .item(OFFSET, format_offset(offset))
            .return_values(ReturnValue::AllOld)
            .send()
            .await;

        let put = match put {
            Ok(out) => out,
            Err(SdkError::ServiceError(e)) if e.err().is_conditional_check_failed_exception() => {
                return Ok(CaSResult::ExpectationMismatch);
            }
            Err(e) => return Err(e.into()),
        };

        self.consumed_capacity(put.consumed_capacity);

        Ok(CaSResult::Committed)
    }

    async fn update(
        &self,
        key: &str,
        offset: Option<SeqNo>,
        mut builder: impl FnMut(UpdateItemFluentBuilder) -> UpdateItemFluentBuilder,
    ) -> anyhow::Result<Option<AttrMap>> {
        let put = self.client.update_item();
        let put = builder(put)
            .table_name(&self.table_name)
            .key(KEY, format_string(key))
            .key(OFFSET, format_offset(offset))
            .return_values(ReturnValue::AllNew)
            .send()
            .await;

        let update = match put {
            Ok(result) => result,
            Err(SdkError::ServiceError(s)) if s.err().is_conditional_check_failed_exception() => {
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        };

        self.consumed_capacity(update.consumed_capacity);

        Ok(update.attributes)
    }

    async fn delete(
        &self,
        key: &str,
        offset: Option<SeqNo>,
        mut builder: impl FnMut(DeleteItemFluentBuilder) -> DeleteItemFluentBuilder,
    ) -> anyhow::Result<Option<AttrMap>> {
        let delete = self.client.delete_item();
        let delete = builder(delete)
            .table_name(&self.table_name)
            .key(KEY, format_string(key))
            .key(OFFSET, format_offset(offset))
            .return_values(ReturnValue::AllNew)
            .send()
            .await;

        let delete = match delete {
            Ok(result) => result,
            Err(SdkError::ServiceError(s)) if s.err().is_conditional_check_failed_exception() => {
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        };

        self.consumed_capacity(delete.consumed_capacity);

        Ok(delete.attributes)
    }

    async fn get_state(&self, key: &str) -> Result<Option<State>, ExternalError> {
        let attrs = self.get(key, None, |b| b).await?;
        let Some(attrs) = attrs else {
            return Ok(None);
        };
        Ok(Some(parse_state(&attrs)?))
    }

    async fn put_state(
        &self,
        key: &str,
        prev: Option<State>,
        new: State,
    ) -> Result<CaSResult, ExternalError> {
        let result = self
            .put(key, None, |b| {
                let b = b
                    .item(HEAD, format_seqno(new.head))
                    .item(TAIL, format_seqno(new.tail));
                if let Some(prev) = &prev {
                    b.expression_attribute_values(format!(":{HEAD}"), format_seqno(prev.head))
                        .expression_attribute_values(format!(":{TAIL}"), format_seqno(prev.tail))
                        .condition_expression(format!("{HEAD} = :{HEAD} AND {TAIL} = :{TAIL}",))
                } else {
                    b.condition_expression(format!(
                        "attribute_not_exists({}) AND attribute_not_exists({})",
                        KEY, OFFSET
                    ))
                }
            })
            .await?;

        Ok(result)
    }

    fn scan(
        &self,
        mut builder: impl FnMut(ScanFluentBuilder) -> ScanFluentBuilder,
    ) -> impl Stream<Item = anyhow::Result<Vec<HashMap<String, AttributeValue>>>> {
        async_stream::try_stream! {
            let mut last_key = None;
            loop {
                let scan = self
                    .client
                    .scan();
                let scan = builder(scan)
                    .table_name(&self.table_name)
                    .set_exclusive_start_key(last_key.take())
                    .consistent_read(true); // TODO: consider lower consistency for some ops
                let result = scan.send().await?;
                yield result.items.unwrap_or_default();
                if result.last_evaluated_key.is_none() {
                    break;
                }
                last_key = result.last_evaluated_key;
            }
        }
    }

    fn query(
        &self,
        mut builder: impl FnMut(QueryFluentBuilder) -> QueryFluentBuilder,
    ) -> impl Stream<Item = anyhow::Result<Vec<HashMap<String, AttributeValue>>>> {
        async_stream::try_stream! {
            let mut last_key = None;
            loop {
                let query = self
                    .client
                    .query();
                let query = builder(query)
                    .table_name(&self.table_name)
                    .set_exclusive_start_key(last_key.take())
                    .consistent_read(true); // TODO: consider lower consistency for some ops
                let result = query.send().await?;
                yield result.items.unwrap_or_default();
                if result.last_evaluated_key.is_none() {
                    break;
                }
                last_key = result.last_evaluated_key;
            }
        }
    }
}

#[async_trait]
impl Consensus for DynamoConsensus {
    fn list_keys(&self) -> ResultStream<'_, String> {
        let scan = self.scan(|b| {
            b.projection_expression(format!("{}", KEY))
                .filter_expression(format!("{OFFSET} = :{OFFSET}"))
                .expression_attribute_values(format!(":{OFFSET}"), format_offset(None))
        });

        Box::pin(async_stream::try_stream! {
            for await result in scan {
                for item in result? {
                    let key: String = parse_attr(&item, KEY, parse_string)?;
                    yield key;
                }
            }
        })
    }

    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        let state = self.get_state(key).await?;
        let Some(state) = state else {
            return Ok(None);
        };
        let Some(attrs) = self.get(key, Some(state.head), |b| b).await? else {
            return Ok(None);
        };
        Ok(Some(parse_entry(&attrs)?))
    }

    async fn compare_and_set(
        &self,
        key: &str,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        if new.seqno.0 > i64::MAX as u64 {
            return Err(
                Determinate::new(anyhow!("unreasonably large seqno: {}", new.seqno)).into(),
            );
        }
        let previous_state = self.get_state(key).await?;
        let next_head = previous_state
            .as_ref()
            .map_or(SeqNo::minimum(), |s| s.head.next());

        if new.seqno != next_head {
            return Ok(CaSResult::ExpectationMismatch);
        }

        let data_result = self
            .put(key, Some(new.seqno), |b| {
                b.item(DATA, format_data(&new.data))
                    .condition_expression(format!(
                        "attribute_not_exists({KEY}) AND attribute_not_exists({OFFSET})",
                    ))
            })
            .await?;

        let tail = match &previous_state {
            Some(previous_state) => previous_state.tail,
            None => SeqNo::minimum(),
        };

        let state_result = self
            .put_state(
                key,
                previous_state,
                State {
                    head: new.seqno,
                    tail,
                },
            )
            .await?;

        // There are three possible cases here:
        // - We are the first to insert this seqno into the log. This is good!
        // - Someone else has claimed this seqno, and we will fail. (But we will still try and update
        //   the head pointer, in case whoever claimed it didn't manage to.)
        // - We're so out of date that this entry was truncated away, which is why we were able to
        //   claim it.
        let result = match (data_result, state_result) {
            (CaSResult::Committed, CaSResult::Committed) => {
                // We were the first to append, and successfully updated the state... the good case!
                CaSResult::Committed
            }
            (CaSResult::ExpectationMismatch, CaSResult::Committed) => {
                // Someone else did the append, and we committed their append. Nice of us, but
                // this specific operation is a failure.
                CaSResult::ExpectationMismatch
            }
            (CaSResult::Committed, CaSResult::ExpectationMismatch) => {
                // Dang! This means that we wrote successfully, but the head pointer was no longer
                // where we expected. This implies that the world has moved along without us...
                // TODO: delete the useless write.
                CaSResult::ExpectationMismatch
            }
            (CaSResult::ExpectationMismatch, CaSResult::ExpectationMismatch) => {
                // Utter failure: we neither appended successfully nor updated the state.
                CaSResult::ExpectationMismatch
            }
        };

        Ok(result)
    }

    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        let scan = self.query(|b| {
            b.key_condition_expression(format!("{KEY} = :{KEY} AND {OFFSET} >= :{OFFSET}"))
                .expression_attribute_values(format!(":{KEY}"), format_string(key))
                .expression_attribute_values(format!(":{OFFSET}"), format_offset(Some(from)))
        });

        let mut result = Vec::with_capacity(limit.min(1000));
        let mut scan = pin!(scan);

        while let Some(items) = scan.next().await {
            for item in items? {
                if result.len() == limit {
                    break;
                }
                result.push(parse_entry(&item)?);
            }
        }

        let Some(state) = self.get_state(key).await? else {
            return Ok(vec![]);
        };
        dbg!(&state, from, limit);
        result.retain(|d| d.seqno >= state.tail);
        result.truncate(limit);

        Ok(result)
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<Option<usize>, ExternalError> {
        let state = self
            .get_state(key)
            .await?
            .ok_or_else(|| anyhow!("truncating empty state: {}", key))?;

        if state.head < seqno {
            return Err(ExternalError::Determinate(
                anyhow!(
                    "attempted to truncate {key} to {seqno}, which is before head: {}",
                    state.head,
                )
                .into(),
            ));
        }

        if state.tail >= seqno {
            return Ok(Some(0));
        }

        let mut new_state = state.clone();
        new_state.tail = seqno;

        match self.put_state(key, Some(state), new_state).await? {
            CaSResult::ExpectationMismatch => {
                return Err(Determinate::new(anyhow!("conflict")).into());
            }
            CaSResult::Committed => {}
        }

        // TODO: actual deletes!

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::location::tests::consensus_impl_test;
    use url::Url;

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    async fn test_dynamo_consensus() {
        let url =
            SensitiveUrl(Url::parse("dynamodb://key:secret@localhost:8000/table_name").unwrap());
        consensus_impl_test(|| DynamoConsensus::open(url.clone()))
            .await
            .unwrap();
    }
}
