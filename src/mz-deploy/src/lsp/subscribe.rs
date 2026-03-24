//! SUBSCRIBE lifecycle management for the LSP worksheet.
//!
//! Encapsulates the state machine for streaming SUBSCRIBE queries:
//! at-most-one active subscribe at a time, with cancel-before-start
//! semantics. The background FETCH loop runs on a dedicated database
//! connection and pushes batches to the VS Code client via custom
//! LSP notifications.
//!
//! ## Lifecycle
//!
//! ```text
//! SubscribeManager::start()
//!   ├─ cancel() any existing subscribe
//!   ├─ Apply session settings to dedicated connection
//!   ├─ Spawn background task: run_subscribe_loop()
//!   │    └─ BEGIN → DECLARE CURSOR → loop { FETCH ALL }
//!   │         ├─ group_subscribe_rows() → Vec<SubscribeBatch>
//!   │         └─ send SubscribeBatchNotification per batch
//!   └─ Store SubscribeHandle, return SubscribeStarted
//!
//! SubscribeManager::cancel()
//!   ├─ Take handle from mutex
//!   ├─ Send cancel via cancel_query_with_tls_policy()
//!   └─ Drop task handle
//! ```
//!
//! ## Batch Processing
//!
//! Each iteration of the FETCH loop processes one batch of rows:
//!
//! 1. **FETCH ALL** — Blocks until Materialize returns rows from the cursor.
//! 2. **Extract** — [`extract_subscribe_rows`] pulls column names from the
//!    first `SimpleQueryMessage::Row` and collects every row as
//!    `Vec<Option<String>>`. Columns 0–2 are Materialize system columns
//!    (`mz_timestamp`, `mz_progressed`, `mz_diff`); columns 3+ are data.
//! 3. **Strip system columns** — Data column names are sliced from index 3
//!    onward, dropping the three system columns.
//! 4. **Group by timestamp** — [`worksheet::group_subscribe_rows`] groups
//!    rows by `mz_timestamp` and classifies each group:
//!    - All rows have `mz_progressed = "t"` → **progress-only** batch
//!      (empty diffs, signals "still processing").
//!    - Otherwise → **data batch** with [`SubscribeDiffRow`](worksheet::SubscribeDiffRow)s,
//!      each carrying `diff` (+1 insert, −1 delete) and the data column
//!      values as JSON.
//! 5. **Column names sent once** — The first data batch includes column
//!    names in `SubscribeBatch::columns`; subsequent batches set it to
//!    `None`. A `columns_sent` flag tracks this across iterations.
//! 6. **Notify** — Each [`SubscribeBatch`](worksheet::SubscribeBatch) is
//!    sent as a `"mz-deploy/subscribeBatch"` JSON-RPC notification.
//!
//! **Error handling:** Any FETCH error (cancellation, connection drop,
//! transaction abort) is treated as normal shutdown — the loop issues
//! `ROLLBACK` and returns `Ok(())`. There is no reliable way to distinguish
//! user cancellation from other failures via the error message alone.
//!
//! ## Integration
//!
//! The [`Backend`](super::server::Backend) holds one `SubscribeManager` and
//! delegates subscribe-related requests to it. Connection creation and session
//! snapshot extraction stay in `server.rs`; this module owns everything from
//! "I have a connected client" onwards.

use super::server::cancel_query_with_tls_policy;
use super::worksheet;
use std::sync::Arc;
use tower_lsp::Client;

/// Custom notification for SUBSCRIBE batch delivery.
struct SubscribeBatchNotification;

impl tower_lsp::lsp_types::notification::Notification for SubscribeBatchNotification {
    type Params = worksheet::SubscribeBatch;
    const METHOD: &'static str = "mz-deploy/subscribeBatch";
}

/// Custom notification for SUBSCRIBE completion.
struct SubscribeCompleteNotification;

impl tower_lsp::lsp_types::notification::Notification for SubscribeCompleteNotification {
    type Params = worksheet::SubscribeComplete;
    const METHOD: &'static str = "mz-deploy/subscribeComplete";
}

/// Handle for a running SUBSCRIBE background task.
struct SubscribeHandle {
    task: mz_ore::task::JoinHandle<()>,
    cancel_token: tokio_postgres::CancelToken,
    /// Host of the subscribe connection (for TLS policy on cancel).
    host: String,
}

/// Current session settings to apply to a new subscribe connection.
///
/// Extracted from the main worksheet connection so the subscribe
/// connection uses the same database/schema/cluster context.
pub(super) struct SessionSnapshot {
    pub database: Option<String>,
    pub schema: Option<String>,
    pub cluster: Option<String>,
}

/// Manages the lifecycle of at-most-one active SUBSCRIBE query.
///
/// Owns the subscribe task handle behind a tokio mutex. All subscribe
/// operations — start, cancel — go through this struct.
pub(super) struct SubscribeManager {
    handle: tokio::sync::Mutex<Option<SubscribeHandle>>,
}

impl SubscribeManager {
    pub fn new() -> Self {
        Self {
            handle: tokio::sync::Mutex::new(None),
        }
    }

    /// Cancel any active subscribe. Returns `true` if one was cancelled.
    pub async fn cancel(&self) -> bool {
        let handle = {
            let mut guard = self.handle.lock().await;
            guard.take()
        };
        if let Some(handle) = handle {
            let _ = cancel_query_with_tls_policy(&handle.cancel_token, &handle.host).await;
            drop(handle.task);
            true
        } else {
            false
        }
    }

    /// Start a new subscribe.
    ///
    /// Cancels any existing subscribe, applies session settings to the
    /// dedicated connection, spawns the background FETCH loop, and returns
    /// `SubscribeStarted` with the subscribe ID.
    ///
    /// `subscribe_sql` is the validated SQL with `WITH (PROGRESS)` already
    /// injected. `pg_client` and `host` are from the dedicated connection
    /// opened by the caller.
    pub async fn start(
        &self,
        subscribe_sql: String,
        session: SessionSnapshot,
        pg_client: Arc<tokio_postgres::Client>,
        host: String,
        lsp_client: Client,
    ) -> std::result::Result<worksheet::SubscribeStarted, worksheet::WorksheetError> {
        // Cancel any existing subscribe.
        self.cancel().await;

        // Apply session settings to the dedicated connection.
        let set_sql = worksheet::build_set_commands(&worksheet::SetSessionParams {
            database: session.database,
            schema: session.schema,
            cluster: session.cluster,
        });
        if !set_sql.is_empty() {
            let _ = pg_client.simple_query(&set_sql).await;
        }

        let subscribe_id = format!(
            "sub-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
        );

        let cancel_token = pg_client.cancel_token();
        let task_host = host.clone();

        // Spawn the background FETCH loop.
        let client = lsp_client;
        let sub_id = subscribe_id.clone();
        let task = mz_ore::task::spawn(|| "mz-deploy-subscribe", async move {
            let result = run_subscribe_loop(&pg_client, &subscribe_sql, &sub_id, &client).await;

            // Send completion notification.
            let complete = worksheet::SubscribeComplete {
                subscribe_id: sub_id,
                error: result.err().map(|e| e.to_string()),
            };
            client
                .send_notification::<SubscribeCompleteNotification>(complete)
                .await;
        });

        // Store the handle.
        {
            let mut guard = self.handle.lock().await;
            *guard = Some(SubscribeHandle {
                task,
                cancel_token,
                host: task_host,
            });
        }

        Ok(worksheet::SubscribeStarted { subscribe_id })
    }
}

/// Extract column names and row data from `FETCH ALL` results.
fn extract_subscribe_rows(
    messages: Vec<tokio_postgres::SimpleQueryMessage>,
) -> (Vec<String>, Vec<Vec<Option<String>>>) {
    let mut column_names = Vec::new();
    let mut rows = Vec::new();
    for msg in messages {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            if column_names.is_empty() {
                column_names = row.columns().iter().map(|c| c.name().to_string()).collect();
            }
            let values: Vec<Option<String>> = (0..row.columns().len())
                .map(|i| row.get(i).map(|s: &str| s.to_string()))
                .collect();
            rows.push(values);
        }
    }
    (column_names, rows)
}

/// Run the SUBSCRIBE cursor FETCH loop, sending batches via notifications.
///
/// Opens a transaction, declares a cursor for the subscribe query, and
/// polls with `FETCH ALL` in a loop. Each batch of rows is grouped by
/// `mz_timestamp` and pushed to the client as [`SubscribeBatchNotification`].
///
/// Returns `Ok(())` if the query was cancelled normally, or `Err` on failure.
async fn run_subscribe_loop(
    pg_client: &tokio_postgres::Client,
    subscribe_sql: &str,
    subscribe_id: &str,
    lsp_client: &Client,
) -> std::result::Result<(), String> {
    // Begin transaction and declare cursor.
    pg_client
        .simple_query("BEGIN")
        .await
        .map_err(|e| format!("BEGIN failed: {e}"))?;

    let declare_sql = format!("DECLARE subscribe_cursor CURSOR FOR {subscribe_sql}");
    pg_client
        .simple_query(&declare_sql)
        .await
        .map_err(|e| format!("DECLARE CURSOR failed: {e}"))?;

    let mut columns_sent = false;

    loop {
        let messages = match pg_client.simple_query("FETCH ALL subscribe_cursor").await {
            Ok(msgs) => msgs,
            Err(_) => {
                // Any FETCH error is treated as normal shutdown — the most
                // common cause is query cancellation, but connection drops
                // and transaction aborts also land here. There's no reliable
                // way to distinguish "user cancelled" from other errors via
                // the error message text alone.
                let _ = pg_client.simple_query("ROLLBACK").await;
                return Ok(());
            }
        };

        // Extract column names and row data.
        let (column_names, rows) = extract_subscribe_rows(messages);

        if rows.is_empty() {
            continue;
        }

        // Data column names (skip mz_timestamp, mz_progressed, mz_diff).
        let data_columns: Vec<String> = if column_names.len() > 3 {
            column_names[3..].to_vec()
        } else {
            Vec::new()
        };

        let include_columns = !columns_sent;
        let batches =
            worksheet::group_subscribe_rows(&rows, subscribe_id, include_columns, &data_columns);

        for batch in batches {
            if !columns_sent && batch.columns.is_some() {
                columns_sent = true;
            }
            lsp_client
                .send_notification::<SubscribeBatchNotification>(batch)
                .await;
        }
    }
}
