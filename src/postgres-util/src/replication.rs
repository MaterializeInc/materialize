//! Utilities for working with the PostgreSQL replication copy both format.

use std::io::{Error, ErrorKind};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes, BytesMut};
use futures_util::{ready, SinkExt, Stream};
use pin_project_lite::pin_project;
use postgres_types::PgLsn;
use tokio_postgres::CopyBothDuplex;

use crate::replication::types::{LogicalReplicationMessage, ReplicationMessage};

pub mod types;

const STANDBY_STATUS_UPDATE_TAG: u8 = b'r';
const HOT_STANDBY_FEEDBACK_TAG: u8 = b'h';

pin_project! {
    /// A type which deserializes the postgres replication protocol. This type can be used with
    /// both physical and logical replication to get access to the byte content of each replication
    /// message.
    pub struct ReplicationStream {
        #[pin]
        stream: CopyBothDuplex<Bytes>,
    }
}

impl ReplicationStream {
    /// Creates a new ReplicationStream that will wrap the underlying CopyBoth stream
    pub fn new(stream: CopyBothDuplex<Bytes>) -> Self {
        Self { stream }
    }

    /// Send standby update to server.
    pub async fn standby_status_update(
        self: Pin<&mut Self>,
        write_lsn: PgLsn,
        flush_lsn: PgLsn,
        apply_lsn: PgLsn,
        ts: i64,
        reply: u8,
    ) -> Result<(), tokio_postgres::Error> {
        let mut this = self.project();

        let mut buf = BytesMut::new();
        buf.put_u8(STANDBY_STATUS_UPDATE_TAG);
        buf.put_u64(write_lsn.into());
        buf.put_u64(flush_lsn.into());
        buf.put_u64(apply_lsn.into());
        buf.put_i64(ts);
        buf.put_u8(reply);

        this.stream.send(buf.freeze()).await
    }

    /// Send hot standby feedback message to server.
    pub async fn hot_standby_feedback(
        self: Pin<&mut Self>,
        timestamp: i64,
        global_xmin: u32,
        global_xmin_epoch: u32,
        catalog_xmin: u32,
        catalog_xmin_epoch: u32,
    ) -> Result<(), tokio_postgres::Error> {
        let mut this = self.project();

        let mut buf = BytesMut::new();
        buf.put_u8(HOT_STANDBY_FEEDBACK_TAG);
        buf.put_i64(timestamp);
        buf.put_u32(global_xmin);
        buf.put_u32(global_xmin_epoch);
        buf.put_u32(catalog_xmin);
        buf.put_u32(catalog_xmin_epoch);

        this.stream.send(buf.freeze()).await
    }
}

impl Stream for ReplicationStream {
    type Item = Result<ReplicationMessage<Bytes>, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match ready!(this.stream.poll_next(cx)) {
            Some(Ok(buf)) => Poll::Ready(Some(
                ReplicationMessage::parse(&buf).map_err(|e| Error::new(ErrorKind::InvalidInput, e)),
            )),
            Some(Err(err)) => Poll::Ready(Some(Err(Error::new(ErrorKind::Other, err)))),
            None => Poll::Ready(None),
        }
    }
}

pin_project! {
    /// A type which deserializes the postgres logical replication protocol. This type gives access
    /// to a high level representation of the changes in transaction commit order.
    pub struct LogicalReplicationStream {
        #[pin]
        stream: ReplicationStream,
    }
}

impl LogicalReplicationStream {
    /// Creates a new LogicalReplicationStream that will wrap the underlying CopyBoth stream
    pub fn new(stream: CopyBothDuplex<Bytes>) -> Self {
        Self {
            stream: ReplicationStream::new(stream),
        }
    }

    /// Send standby update to server.
    pub async fn standby_status_update(
        self: Pin<&mut Self>,
        write_lsn: PgLsn,
        flush_lsn: PgLsn,
        apply_lsn: PgLsn,
        ts: i64,
        reply: u8,
    ) -> Result<(), tokio_postgres::Error> {
        let this = self.project();
        this.stream
            .standby_status_update(write_lsn, flush_lsn, apply_lsn, ts, reply)
            .await
    }

    /// Send hot standby feedback message to server.
    pub async fn hot_standby_feedback(
        self: Pin<&mut Self>,
        timestamp: i64,
        global_xmin: u32,
        global_xmin_epoch: u32,
        catalog_xmin: u32,
        catalog_xmin_epoch: u32,
    ) -> Result<(), tokio_postgres::Error> {
        let this = self.project();
        this.stream
            .hot_standby_feedback(
                timestamp,
                global_xmin,
                global_xmin_epoch,
                catalog_xmin,
                catalog_xmin_epoch,
            )
            .await
    }
}

impl Stream for LogicalReplicationStream {
    type Item = Result<ReplicationMessage<LogicalReplicationMessage>, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match ready!(this.stream.poll_next(cx)) {
            Some(Ok(ReplicationMessage::XLogData(body))) => {
                let body = body.map_data(|buf| LogicalReplicationMessage::parse(&buf))?;
                Poll::Ready(Some(Ok(ReplicationMessage::XLogData(body))))
            }
            Some(Ok(ReplicationMessage::PrimaryKeepAlive(body))) => {
                Poll::Ready(Some(Ok(ReplicationMessage::PrimaryKeepAlive(body))))
            }
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
            None => Poll::Ready(None),
        }
    }
}
