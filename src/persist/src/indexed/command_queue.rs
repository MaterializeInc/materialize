// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A (optionally persistent) SPSC queue of commands waiting to be processed and
//! committed to blob storage.

use std::ops::Range;

use abomonation_derive::Abomonation;

use crate::indexed::{Error, Id};
use crate::storage::{Buffer, SeqNo};
use crate::Data;

#[derive(Clone, Debug, Abomonation)]
enum IndexedCommand<K, V> {
    /// Pairs of stream id and the updates themselves.
    Write(Vec<(Id, Vec<((K, V), u64, isize)>)>),
    /// A set of stream ids, and the timestamp those ids should be sealed up to.
    Seal(Vec<Id>, u64),
    /// Register a new stream.
    Register(Id, String),
    /// Destroy a stream.
    Destroy(Id, String),
    /// The timestamp this ID can be compacted up to.
    AllowCompaction(Id, u64),
}

struct CommandQueue<K, V, U: Buffer> {
    // The underlying Buffer, used to persist the commands we want persisted.
    // TODO: should this be an Option<U> for cases when we don't want to
    // persist any commands?
    inner: U,
    seqnos: Range<SeqNo>,
    // Send commands to the reader.
    tx: crossbeam_channel::Sender<(SeqNo, IndexedCommand<K, V>)>,
    // Reader can tell us what sequence numbers are safe to truncate up to.
    rx: crossbeam_channel::Receiver<SeqNo>,
    writer_truncation_bound: SeqNo,
    reader_truncation_bound: SeqNo,
}

struct CommandQueueReader<K, V> {
    // Data from the CommandQueue writer.
    rx: crossbeam_channel::Receiver<(SeqNo, IndexedCommand<K, V>)>,
    // Current range of sequence numbers we have data for
    seqno: Range<SeqNo>,
    // Current list of commands.
    dataz: Vec<(SeqNo, IndexedCommand<K, V>)>,
    // Channel to send updates about what sequence numbers can be compacted up to.
    tx: crossbeam_channel::Sender<SeqNo>,
}

impl<K: Data, V: Data, U: Buffer> CommandQueue<K, V, U> {
    /// Create a new CommandQueue and CommandQueueReader, and inform the reader
    /// of everything previously persisted in the queue.
    ///
    /// WIP: TODO: not sure where the best place to put something like this is?
    pub fn new(mut buffer: U) -> Result<(Self, CommandQueueReader<K, V>), Error> {
        // TODO: Is an unbounded channel the right thing to do here?
        let (data_tx, data_rx) = crossbeam_channel::unbounded();
        let (truncate_tx, truncate_rx) = crossbeam_channel::unbounded();

        let mut dataz = vec![];
        let desc = buffer.snapshot(|seqno, data| {
            let mut buf = data.to_vec();
            let (entry, remaining) =
                unsafe { abomonation::decode::<IndexedCommand<K, V>>(&mut buf) }
                    .ok_or_else(|| Error::from(format!("invalid buffer entry")))?;
            if !remaining.is_empty() {
                return Err(format!("invalid buffer entry").into());
            }
            dataz.push((seqno, entry.clone()));
            Ok(())
        })?;

        let command_queue = CommandQueue {
            inner: buffer,
            seqnos: desc.clone(),
            tx: data_tx,
            rx: truncate_rx,
            reader_truncation_bound: desc.start,
            writer_truncation_bound: desc.start,
        };

        let command_queue_reader = CommandQueueReader {
            rx: data_rx,
            seqno: desc,
            dataz,
            tx: truncate_tx,
        };

        Ok((command_queue, command_queue_reader))
    }

    /// Synchronously appends an commands to the queue and sends to the reader.
    ///
    /// Optionally persists commands that need to be persisted.
    fn write_sync(&mut self, command: IndexedCommand<K, V>) -> Result<SeqNo, Error> {
        let seqno = match &command {
            persist @ IndexedCommand::Write(_)
            | persist @ IndexedCommand::Register(_, _)
            | persist @ IndexedCommand::Destroy(_, _) => {
                let mut command_bytes = Vec::new();
                unsafe { abomonation::encode(persist, &mut command_bytes) }
                    .expect("write to Vec is infallible");
                let seqno = self.inner.write_sync(command_bytes)?;
                self.seqnos.end = SeqNo(seqno.0 + 1);

                seqno
            }
            IndexedCommand::Seal(_, _) | IndexedCommand::AllowCompaction(_, _) => self.seqnos.end,
        };

        if let Err(crossbeam_channel::SendError(_)) = self.tx.send((seqno, command)) {
            Err(Error::from("write_sync failed because receiver shut down"))
        } else {
            Ok(seqno)
        }
    }

    /// Removes all entries with a SeqNo strictly less than an upper bound provided
    /// by the writer, that have also been allowed to be truncated by the reader.
    fn truncate(&mut self, upper: SeqNo) -> Result<(), Error> {
        self.writer_truncation_bound = upper;
        for allowed in self.rx.try_iter() {
            self.reader_truncation_bound = allowed;
        }

        let lower = if self.writer_truncation_bound < self.reader_truncation_bound {
            self.writer_truncation_bound
        } else {
            self.reader_truncation_bound
        };

        self.inner.truncate(lower)?;
        self.seqnos = lower..self.seqnos.end;

        Ok(())
    }

    /// Returns a consistent snapshot of all persisted but not yet truncated
    /// commands.
    ///
    /// - Invariant: all returned commands must have a sequence number within
    ///   the declared [lower, upper) range of sequence numbers.
    /// - Invariant: all commands needs to be shown in order.
    fn snapshot<F>(&self, logic: F) -> Result<Range<SeqNo>, Error>
    where
        F: FnMut(SeqNo, &IndexedCommand<K, V>) -> Result<(), Error>,
    {
        unimplemented!()
    }

    /// Synchronously closes the buffer, releasing exclusive-writer locks and
    /// causing all future commands to error.
    ///
    /// Implementations must be idempotent. Returns true if the buffer had not
    /// previously been closed.
    fn close(&mut self) -> Result<bool, Error> {
        unimplemented!()
    }
}

impl<K: Data, V: Data> CommandQueueReader<K, V> {
    /// Returns a consistent snapshot of all written but not yet truncated
    /// entries.
    ///
    /// - Invariant: all returned entries must have a sequence number within
    ///   the declared [lower, upper) range of sequence numbers.
    /// - Invariant: all data needs to be shown in order.
    fn snapshot<F>(&self, logic: F) -> Result<Range<SeqNo>, Error>
    where
        F: FnMut(SeqNo, &IndexedCommand<K, V>) -> Result<(), Error>,
    {
        unimplemented!()
    }

    /// Removes all entries with a SeqNo strictly less than the given upper
    /// bound.
    fn allow_truncation(&mut self, upper: SeqNo) -> Result<(), Error> {
        unimplemented!()
    }

    /// Synchronously closes the buffer reader, causing all future commands to error.
    ///
    /// Implementations must be idempotent. Returns true if the buffer had not
    /// previously been closed.
    fn close(&mut self) -> Result<bool, Error> {
        unimplemented!()
    }
}
