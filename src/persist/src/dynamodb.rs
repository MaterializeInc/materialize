use crate::location::{
    CaSResult, Consensus, Determinate, ExternalError, Indeterminate, ResultStream, SeqNo,
    VersionedData,
};
use anyhow::{Context, anyhow};
use async_trait::async_trait;
use aws_config::meta::region::ProvideRegion;
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::operation::create_table::CreateTableError;
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
    AttributeDefinition, AttributeValue, BillingMode, ConsumedCapacity, DeleteRequest,
    KeySchemaElement, KeyType, ReturnValue, ReturnValuesOnConditionCheckFailure,
    ScalarAttributeType, WriteRequest,
};
use aws_sdk_s3::error::SdkError;
use aws_types::region::Region;
use bytes::Bytes;
use futures_util::Stream;
use mz_ore::task::AbortOnDropHandle;
use mz_ore::url::SensitiveUrl;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::{Arc, Mutex};
use tracing::warn;

type AttrMap = HashMap<String, AttributeValue>;

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Debug)]
enum Offset {
    State,
    Entry(SeqNo),
}

const KEY: &str = "k";
const OFFSET: &str = "i";
const DATA: &str = "d";
const HEAD: &str = "h";
const TAIL: &str = "t";

fn parse_key(v: &AttributeValue) -> anyhow::Result<String> {
    let s = v.as_s().map_err(|e| anyhow!("non_string attr: {e:?}"))?;
    Ok(s.to_owned())
}
fn format_key(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn format_offset(value: Offset) -> AttributeValue {
    let value = match value {
        Offset::State => "-1".to_string(),
        Offset::Entry(seqno) => seqno.0.to_string(),
    };
    AttributeValue::N(value)
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

fn parse_state(hash: &Option<AttrMap>) -> anyhow::Result<Option<State>> {
    let Some(hash) = hash else {
        return Ok(None);
    };
    Ok(Some(State {
        head: parse_attr(hash, HEAD, parse_seqno)?,
        tail: parse_attr(hash, TAIL, parse_seqno)?,
    }))
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

#[derive(Debug, Clone, Copy)]
struct State {
    head: SeqNo,
    tail: SeqNo,
}

impl State {
    // /// Update the cache after successfully observing the head.
    // fn observe_head(&mut self, next_head: Offset) {
    //     self.head = self.head.max(next_head);
    // }
    //
    // /// Update the cache after successfully observing the tail.
    // fn observe_tail(&mut self, next_tail: Offset) {
    //     self.tail = self.tail.max(next_tail);
    //     // By construction, the tail cannot be any larger than the head.
    //     self.head = self.head.max(next_tail);
    // }
}

/// Dynamodb-backed consensus.
#[derive(Debug)]
pub struct DynamoConsensus {
    client: aws_sdk_dynamodb::Client,
    table_name: String,
    pointers: Arc<Mutex<BTreeMap<String, State>>>,
    delete_tx: tokio::sync::mpsc::Sender<(String, Range<SeqNo>)>,
    _delete_handle: AbortOnDropHandle<()>,
}

#[derive(Debug)]
struct UpdateResult<T> {
    cas_result: CaSResult,
    previous_state: T,
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

        for (key, val) in url.query_pairs() {
            match &*key {
                "region" => {
                    let region: Box<dyn ProvideRegion> = Box::new(Region::new(val.into_owned()));
                    loader = loader.region(region);
                }
                other => {
                    return Err(ExternalError::Determinate(
                        anyhow!("unexpected query param: {other}").into(),
                    ));
                }
            }
        }

        let config = loader.load().await;
        let client = aws_sdk_dynamodb::Client::new(&config);
        let table_name = url
            .path()
            .strip_prefix('/')
            .expect("path validation")
            .to_string();

        let (tx, mut rx) = tokio::sync::mpsc::channel::<(String, Range<SeqNo>)>(16);

        let handle = mz_ore::task::spawn(|| "", {
            let client = client.clone();
            let table_name = table_name.clone();
            async move {
                let mut events = vec![];

                let mut batch_delete: HashMap<String, Vec<WriteRequest>> = Default::default();
                let mut delete_count = 0;
                while rx.recv_many(&mut events, 25).await > 0 {
                    for (key, range) in events.drain(..) {
                        for seqno in range.start.0..range.end.0 {
                            let deletes = batch_delete.entry(table_name.clone()).or_default();
                            deletes.push(
                                WriteRequest::builder()
                                    .delete_request(
                                        DeleteRequest::builder()
                                            .key(KEY, format_key(&key))
                                            .key(OFFSET, format_offset(Offset::Entry(SeqNo(seqno))))
                                            .build()
                                            .expect("required params specified"),
                                    )
                                    .build(),
                            );
                            delete_count += 1;
                            if delete_count == 25 {
                                let result = client
                                    .batch_write_item()
                                    .set_request_items(Some(std::mem::take(&mut batch_delete)))
                                    .send()
                                    .await;
                                match result {
                                    Ok(data) => {
                                        if let Some(unprocessed) = data.unprocessed_items {
                                            batch_delete = unprocessed;
                                        }
                                    }
                                    Err(e) => {
                                        warn!("ignoring batch_write_item failure: {}", e);
                                    }
                                }
                                delete_count = 0;
                            }
                        }
                    }
                }
            }
        });
        let this = Self {
            client,
            table_name,
            pointers: Arc::new(Mutex::new(BTreeMap::new())),
            delete_tx: tx,
            _delete_handle: handle.abort_on_drop(),
        };

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
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await;

        match result {
            Ok(_) => Ok(this),
            // OK if the table already exists - ideally it will be managed externally.
            Err(SdkError::ServiceError(e)) if e.err().is_resource_in_use_exception() => Ok(this),
            Err(e) => Err(e)?,
        }
    }

    fn consumed_capacity(&self, _cap: Option<ConsumedCapacity>) {
        // TODO: capacity metrics!
    }

    fn cached_state(&self, key: &str) -> Option<State> {
        self.pointers.lock().unwrap().get(key).copied()
    }

    fn update_cached_state(&self, key: &str, state: Option<State>) {
        let Some(state) = state else { return };
        let mut guard = self.pointers.lock().unwrap();
        guard
            .entry(key.to_string())
            .and_modify(|old| {
                if old.head <= state.head && old.tail <= state.tail {
                    *old = state;
                }
            })
            .or_insert(state);
    }

    async fn get(
        &self,
        key: &str,
        offset: Offset,
        mut builder: impl FnMut(GetItemFluentBuilder) -> GetItemFluentBuilder,
    ) -> anyhow::Result<Option<AttrMap>> {
        let get = self.client.get_item();
        let get = builder(get)
            .table_name(&self.table_name)
            .key(KEY, format_key(key))
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
        offset: Offset,
        mut builder: impl FnMut(PutItemFluentBuilder) -> PutItemFluentBuilder,
    ) -> anyhow::Result<CaSResult> {
        let put = self.client.put_item();
        let put = builder(put)
            .table_name(&self.table_name)
            .item(KEY, format_key(key))
            .item(OFFSET, format_offset(offset))
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
        offset: Offset,
        mut builder: impl FnMut(UpdateItemFluentBuilder) -> UpdateItemFluentBuilder,
    ) -> anyhow::Result<UpdateResult<Option<AttrMap>>> {
        let put = self.client.update_item();
        let put = builder(put)
            .table_name(&self.table_name)
            .key(KEY, format_key(key))
            .key(OFFSET, format_offset(offset))
            .return_values(ReturnValue::AllOld)
            .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
            .send()
            .await;

        match put {
            Ok(out) => {
                self.consumed_capacity(out.consumed_capacity);
                Ok(UpdateResult {
                    cas_result: CaSResult::Committed,
                    previous_state: out.attributes,
                })
            }
            Err(SdkError::ServiceError(s)) => match s.into_err() {
                UpdateItemError::ConditionalCheckFailedException(e) => Ok(UpdateResult {
                    cas_result: CaSResult::ExpectationMismatch,
                    previous_state: e.item,
                }),
                e => Err(e.into()),
            },
            Err(e) => Err(e.into()),
        }
    }

    async fn get_state(&self, key: &str) -> Result<Option<State>, ExternalError> {
        let attrs = self.get(key, Offset::State, |b| b).await?;
        let state = parse_state(&attrs)?;
        self.update_cached_state(key, state);
        Ok(state)
    }

    async fn update_head(&self, key: &str, new: SeqNo) -> anyhow::Result<Option<State>> {
        let updated = self
            .update(key, Offset::State, |b| match new.previous() {
                None => b
                    .condition_expression(format!(
                        "attribute_not_exists({HEAD}) AND attribute_not_exists({TAIL})"
                    ))
                    .update_expression(format!("SET {HEAD}=:n, {TAIL}=:n"))
                    .expression_attribute_values(":n", format_seqno(new)),
                Some(prev) => b
                    .condition_expression(format!("{HEAD} = :p"))
                    .update_expression(format!("SET {HEAD}=:n"))
                    .expression_attribute_values(":p", format_seqno(prev))
                    .expression_attribute_values(":n", format_seqno(new)),
            })
            .await?;

        let previous_state = parse_state(&updated.previous_state)?;
        let current_state = if updated.cas_result == CaSResult::Committed {
            match previous_state {
                None => Some(State {
                    head: new,
                    tail: new,
                }),
                Some(prev) => Some(State { head: new, ..prev }),
            }
        } else {
            previous_state
        };

        self.update_cached_state(key, current_state);
        Ok(previous_state)
    }

    async fn update_tail(&self, key: &str, new: SeqNo) -> anyhow::Result<Option<State>> {
        let attrs = self
            .update(key, Offset::State, |b| {
                b.condition_expression(format!("{HEAD} >= :t AND {TAIL} < :t"))
                    .update_expression(format!("SET {TAIL}=:t"))
                    .expression_attribute_values(":t", format_seqno(new))
            })
            .await?;

        let previous_state = parse_state(&attrs.previous_state)?;
        let current_state = if attrs.cas_result == CaSResult::Committed {
            match previous_state {
                None => None,
                Some(prev) => Some(State { tail: new, ..prev }),
            }
        } else {
            previous_state
        };
        self.update_cached_state(key, current_state);
        Ok(previous_state)
    }

    fn scan(
        &self,
        mut builder: impl FnMut(ScanFluentBuilder) -> ScanFluentBuilder,
    ) -> impl Stream<Item = anyhow::Result<Vec<AttrMap>>> {
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
}

#[async_trait]
impl Consensus for DynamoConsensus {
    fn list_keys(&self) -> ResultStream<'_, String> {
        let scan = self.scan(|b| {
            b.projection_expression(KEY)
                .filter_expression(format!("{OFFSET} = :{OFFSET}"))
                .expression_attribute_values(format!(":{OFFSET}"), format_offset(Offset::State))
        });

        Box::pin(async_stream::try_stream! {
            for await result in scan {
                for item in result? {
                    let key: String = parse_attr(&item, KEY, parse_key)?;
                    yield key;
                }
            }
        })
    }

    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        // Similar to `scan`, this code is a little tricky...
        // the obvious thing to do is to fetch the state, then fetch whatever's at the head
        // pointer, but we risk returning the wrong answer if it's been truncated away in
        // between. Instead, we fetch the data first and then the state, using our state cache
        // for our initial guess and looping if the data's missing or doesn't match.
        let mut candidate_state = None;
        loop {
            // Fetch the log entry, then the state.
            let fetch_state = candidate_state.or_else(|| self.cached_state(key));
            let attrs = match fetch_state {
                Some(state) => self.get(key, Offset::Entry(state.head), |b| b).await?,
                None => None,
            };
            let Some(state) = self.get_state(key).await? else {
                // Easy: log doesn't exist; nothing to return.
                return Ok(None);
            };
            let Some(attrs) = attrs else {
                continue;
            };
            let entry = parse_entry(&attrs)?;
            if entry.seqno < state.tail {
                candidate_state = None;
            }
            let candidate_state = *candidate_state.get_or_insert(state);
            if candidate_state.head == entry.seqno {
                return Ok(Some(entry));
            }
        }
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

        let cached_offsets = self.cached_state(key);
        let cached_next = cached_offsets.map_or(SeqNo::minimum(), |s| s.head.next());
        match cached_next.cmp(&new.seqno) {
            Ordering::Less => {
                // Either our cache is stale (likely) or the caller is trying to skip sequence numbers
                // (possible but unusual). Fetch the latest head from the database to check.
                // TODO: we don't need to fetch the data here, just the next pointer.
                let state = self.get_state(key).await?;
                let next = state.map_or(SeqNo::minimum(), |s| s.head.next());
                if next != new.seqno {
                    return Ok(CaSResult::ExpectationMismatch);
                }
            }
            Ordering::Equal => {
                // Great - the last offset we observed is exactly the one we expected.
            }
            Ordering::Greater => {
                // We've already seen a seqno bigger than this; this data can never be appended.
                // Bail early.
                return Ok(CaSResult::ExpectationMismatch);
            }
        }

        let data_result = self
            .put(key, Offset::Entry(new.seqno), |b| {
                b.item(DATA, format_data(&new.data))
                    .condition_expression(format!(
                        "attribute_not_exists({KEY}) AND attribute_not_exists({OFFSET})",
                    ))
            })
            .await?;

        let previous_state = self.update_head(key, new.seqno).await?;

        if data_result == CaSResult::ExpectationMismatch {
            return Ok(CaSResult::ExpectationMismatch);
        }

        match previous_state {
            None => Ok(CaSResult::Committed),
            Some(state) => {
                if new.seqno < state.tail {
                    if let Err(e) = self
                        .delete_tx
                        .try_send((key.to_string(), new.seqno..new.seqno.next()))
                    {
                        warn!("failed to clean up data: {}", e);
                    }
                    Err(anyhow!("truncated past the inserted record: must retry").into())
                } else {
                    Ok(CaSResult::Committed)
                }
            }
        }
    }

    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        if limit == 0 {
            return Ok(vec![]);
        }

        // Scan is a little tricky, largely because we can't do a transactional read of many entries
        // - we have to stream them in - and because we can't trust data that is from before the tail.
        let mut result = Vec::with_capacity(limit.min(128));
        let mut continue_from = from;
        let mut continue_to = None;
        'scan: loop {
            let limit_remaining = limit - result.len();
            let fetch_limit = limit_remaining
                .clamp(1, 1024)
                .try_into()
                .expect("constant upper bound");
            let query = self
                .client
                .query()
                .table_name(&self.table_name)
                .consistent_read(true)
                .key_condition_expression(format!("{KEY} = :k AND {OFFSET} >= :o"))
                .expression_attribute_values(":k", format_key(key))
                .expression_attribute_values(":o", format_offset(Offset::Entry(from)))
                .limit(fetch_limit)
                .send()
                .await
                .context("scan query")?;
            let Some(latest_state) = self.get_state(key).await? else {
                return Ok(vec![]);
            };
            if latest_state.head < from {
                // The caller is scanning starting from past the head! That's empty for sure.
                return Ok(vec![]);
            }

            if continue_from < latest_state.tail {
                // There's a gap in the log - we've truncated past this point already.
                // Drop the current results and continue from the new tail.
                result.clear();
                continue_from = latest_state.tail;
                continue_to = None;
            }

            // We only need to fetch and parse data that's not past the target upper bound.
            let continue_to = continue_to.get_or_insert(latest_state.head);

            for item in query.items.unwrap_or_default() {
                let entry = parse_entry(&item)?;
                let seqno = entry.seqno;
                if seqno < continue_from {
                    continue;
                }
                result.push(entry);
                if seqno >= *continue_to || result.len() == limit {
                    break 'scan;
                }
                continue_from = seqno.next();
            }
        }

        Ok(result)
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<Option<usize>, ExternalError> {
        let state = self.update_tail(key, seqno).await?;

        match state {
            None => Err(anyhow!("attempted to truncate an empty state").into()),
            Some(previous) => {
                if previous.head < seqno {
                    Err(anyhow!("attempted to truncate past the head").into())
                } else {
                    if previous.tail < seqno {
                        if let Err(e) = self
                            .delete_tx
                            .try_send((key.to_string(), previous.tail..seqno))
                        {
                            warn!("failed to clean up data: {}", e);
                        }
                    }
                    Ok(None)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::location::tests::consensus_impl_test;
    use url::Url;

    /// The dynamodb url to use in tests. For example, if running `dynamodb-local` on the default
    /// port locally, you might choose `dynamodb://key:secret@localhost:8000/table_name?region=local`.
    const TEST_URL_ENV: &'static str = "MZ_PERSIST_EXTERNAL_STORAGE_TEST_DYNAMO_URL";

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    async fn test_dynamo_consensus() {
        let Ok(url_string) = std::env::var(TEST_URL_ENV) else {
            return;
        };
        let url = SensitiveUrl(Url::parse(&url_string).unwrap());
        consensus_impl_test(|| DynamoConsensus::open(url.clone()))
            .await
            .unwrap();
    }
}
