// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Organize and maintain a compacted representation of persisted data.
//!
//! The `Compacter` task keeps track of the persisted updates for each persisted relation and
//! periodically compacts that representation to use space proportional to the number of
//! distinct rows in the relation. In order to do so, the `Compacter` maintains a `Trace` for
//! each persisted relation. Note that the `Compacter` is currently a thread running on the
//! Materialize process, but there's no conceptual reason it couldn't be a separate process, or
//! even on a separate machine as long as it had access to the storage for the WAL (e.g. with a
//! shared EBS volume).
//!
//! A `Trace` is basically a list of `Batch`s that represent a contiguous time interval, and a
//! compaction frontier.
//!
//! A `Batch` is a consolidated list of updates that occured between times [lower, upper)
//! where each update is of the form `(Row, time, diff)` and each `(Row, time)` pair occurs
//! exactly once and all diffs are nonzero.
//!
//! Note that all `Batch`s keep their data on persistent storage. No data resides in memory
//! (except currently we load all the data from batches into memory for compaction and on
//!  restart but this will get fixed!).
//!
//! The `Coordinator` thread tells the `Compacter` when it needs to
//!  * start keeping track of a new relation
//!  * stop keeping track of a relation
//!  * resume keeping track of a relation with an already initialized Trace (on restart)
//!  * advance a relation's compaction frontier. Note that this doesn't automatically trigger
//!    any actual compaction. That happens later (keep reading).
//!
//! The `Compacter` task periodically checks each relation's WAL directory to look
//! for finished log segments, converts them to `Batch`s (basically consolidates the
//! updates for a range of times) and adds them to the relation's `Trace`.
//!
//! When a `Trace` contains too many `Batches`, the Trace physically
//! combines all of them into a single large batch with updates
//! forwarded to the compaction frontier.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context};
use lazy_static::lazy_static;
use log::error;
use regex::Regex;
use timely::progress::timestamp::Timestamp as TimelyTimestamp;
use tokio::select;
use tokio::sync::mpsc;

use dataflow_types::Update;
use expr::GlobalId;
use repr::Timestamp;

use crate::wal::{encode_progress, encode_update, read_segment, Message};

// How frequently the `Compacter` checks to see if `Batch`s should be compacted.
// TODO: Lets add some jitter to compaction so we aren't compacting every single
// relation at the same time maybe?
static COMPACTER_INTERVAL: Duration = Duration::from_secs(300);

/// Instructions that the Coordinator sends the Compacter.
#[derive(Debug)]
pub enum CompacterMessage {
    Add(GlobalId),
    Drop(GlobalId),
    Resume(GlobalId, Trace),
    AllowCompaction(GlobalId, Timestamp),
}

/// A Batch contains all of the updates that originated within some time range [lower, upper)
/// but the data live the file at `path`.
///
/// The data stored in each batch are triples of (Row, Timestamp, Diff) such that there is
/// exactly one copy of each (Row, Timestamp) in each batch. Batches also have a header
/// and a footer indicating the upper and lower bound timestamps.
/// TODO: Batches are meant to mimic differential / cdcv2 batches but do not do so
/// currently. Let's fix that. Specifically, introduce a `since` field, and counts
/// for the number of updates at each time.
/// TODO: Differential has a struct called `Description` that we should eventually re-use
/// here.
#[derive(Clone, Debug, PartialEq)]
struct Batch {
    upper: Timestamp,
    lower: Timestamp,
    path: PathBuf,
}

impl Batch {
    /// Create a batch from a finished log segment file.
    ///
    /// Reads in the contents at `log_segment_path` into memory, consolidates them,
    /// (i. e. keeps a single copy per (Row, time) update), and writes that data, along
    /// with the corresponding [lower, upper) frontiers, to a new file. Returns a
    /// new `Batch` that points to the newly created file.
    fn create(
        log_segment_path: &Path,
        trace_path: &Path,
        expected_lower: Timestamp,
    ) -> Result<Self, anyhow::Error> {
        let messages = read_segment(log_segment_path)?;
        Batch::create_from_messages(messages, trace_path, expected_lower, None, None)
    }

    /// Read in and consolidate a list of messages, and write them to a new batch file
    /// in `trace_path`.
    ///
    /// Will also compact updates up to `compaction_frontier` if provided.
    fn create_from_messages(
        messages: Vec<Message>,
        trace_path: &Path,
        expected_lower: Timestamp,
        expected_upper: Option<Timestamp>,
        compaction_frontier: Option<Timestamp>,
    ) -> Result<Self, anyhow::Error> {
        let (lower, upper) = get_lower_and_upper_bounds(&messages)?;

        if lower != expected_lower {
            bail!("Expected lower of {}, found {}", expected_lower, lower);
        }

        if let Some(expected_upper) = expected_upper {
            if upper != expected_upper {
                bail!("Expected upper of {}, found {}", expected_upper, upper);
            }
        }

        let mut time_data = BTreeMap::new();
        for message in messages.iter() {
            match message {
                Message::Progress(time) => {
                    assert!(*time >= lower);
                    assert!(*time <= upper);
                }
                Message::Data(Update {
                    row,
                    timestamp,
                    diff,
                }) => {
                    let time = if let Some(frontier) = compaction_frontier {
                        std::cmp::max(frontier, *timestamp)
                    } else {
                        *timestamp
                    };

                    // Note that we only assert that the time is valid wrt to the lower bound
                    // because the compaction frontier could advance ahead of the upper bound.
                    assert!(time >= lower);
                    let entry = time_data.entry((time, row)).or_insert(0);
                    *entry += diff;

                    if *entry == 0 {
                        time_data.remove(&(time, row));
                    }
                }
            }
        }

        // Now let's prepare the output
        let mut buf = Vec::new();

        // Frame each batch with its lower and upper bound timestamp.
        // TODO: match the behavior of CDCv2 updates with a count of messages
        // at each timestamp.
        encode_progress(lower, &mut buf)?;
        for ((timestamp, row), diff) in time_data.into_iter() {
            // TODO: this shouldn't happen anymore. Let's complain if it does.
            if diff == 0 {
                continue;
            }

            encode_update(row, timestamp, diff, &mut buf)?;
        }

        encode_progress(upper, &mut buf)?;

        let batch_name = format!("batch-{}-{}", lower, upper);
        let batch_path = trace_path.join(&batch_name);
        let batch_tmp_path = trace_path.join(format!("{}-tmp", batch_name));
        // Write the file first suffixed with "-tmp" and then rename to guard against
        // partial writes.
        let mut batch_tmp_file = File::create(&batch_tmp_path)
            .with_context(|| format!("failed to open batch file {}", batch_tmp_path.display()))?;
        batch_tmp_file
            .write_all(&buf)
            .with_context(|| format!("failed to write batch file {}", batch_tmp_path.display()))?;
        batch_tmp_file
            .flush()
            .with_context(|| format!("failed to flush batch file {}", batch_tmp_path.display()))?;
        batch_tmp_file
            .sync_all()
            .with_context(|| format!("failed to sync batch file {}", batch_tmp_path.display()))?;
        // TODO: We need to fsync the parent directory here to durably persist this
        // rename.
        fs::rename(&batch_tmp_path, &batch_path).with_context(|| {
            format!(
                "failed to rename batch file from: {} to: {}",
                batch_tmp_path.display(),
                batch_path.display()
            )
        })?;

        Ok(Batch {
            upper,
            lower,
            path: batch_path,
        })
    }

    /// Reintroduce a batch based on an available file in the trace
    /// directory.
    fn reinit(path: PathBuf) -> Result<Self, anyhow::Error> {
        let batch_name = path
            .file_name()
            .expect("batch name known to exist")
            .to_str()
            .expect("batch name known to be valid utf8");
        let parts: Vec<_> = batch_name.split('-').collect();
        // TODO: return an error here instead of asserting.
        assert!(parts.len() == 3);
        Ok(Self {
            upper: parts[2].parse()?,
            lower: parts[1].parse()?,
            path,
        })
    }

    /// Read the data from a batch stored on disk into memory.
    fn read(&self) -> Result<Vec<Message>, anyhow::Error> {
        read_segment(&self.path)
    }

    /// Physically concatenate all `Batch`s together into a single `Batch`
    /// and consolidate updates up to the frontier if provided.
    fn compact(
        batches: &[Batch],
        trace_path: &Path,
        compaction_frontier: Option<Timestamp>,
    ) -> Result<Self, anyhow::Error> {
        let mut messages = vec![];

        if batches.len() < 2 {
            bail!(
                "Need to provide at least two batches to compact together. Received {}",
                batches.len()
            );
        }

        let lower = batches.first().expect("known to exist").lower;
        let upper = batches.last().expect("known to exist").upper;

        // Check that the provided batches span a contiguous time interval.
        let mut previous_upper = lower;
        for batch in batches {
            assert!(batch.lower == previous_upper);
            previous_upper = batch.upper;
        }

        assert!(previous_upper == upper);

        for batch in batches {
            messages.append(&mut read_segment(&batch.path)?);
        }

        Batch::create_from_messages(
            messages,
            trace_path,
            lower,
            Some(upper),
            compaction_frontier,
        )
    }
}

/// A Trace is an on-disk representation of data meant to mimic a differential Trace.
///
/// A Trace checks the `wal_path` and looks for WAL segments that it can consolidate
/// into Batches (stored in the `trace_path`). Once it exceeds a certain number of Batches
/// it tries to physically and logically compact them into a single batch that is
/// compacted up to the `compaction` frontier.
#[derive(Debug)]
pub struct Trace {
    // Directory where `Batch` data are stored. The `Trace` will create this directory,
    // write files into it as updates come in, reads from it to compact and on restart,
    // and will delete it when the underlying relation is dropped.
    trace_path: PathBuf,
    // Directory where WAL segments are stored. The `Trace` assumes this directory already
    // exists by the time the `Trace` is created, and the `Trace` will read from it to find
    // newly minted log segments, delete segments as they are converted into `Batch`s and
    // delete the directory when the underlying relation is dropped.
    wal_path: PathBuf,
    // List of `Batch`s in this trace. The `Batch`s have to be sorted by the disjoint time intervals
    // they are responsible for and together, all of the `Batch`s must cover [T::min, upper_bound).
    batches: Vec<Batch>,
    // Compaction frontier for this trace, as indicated by the `Coordinator`.
    compaction: Option<Timestamp>,
}

impl Trace {
    /// Create a new `Trace` for relation `id`.
    ///
    /// Note that the `wal_path` has to already have been created, but the `trace_path` cannot
    /// already exist before we instantiate this `Trace`.
    fn create(id: GlobalId, trace_path: PathBuf, wal_path: PathBuf) -> Result<Self, anyhow::Error> {
        let _ = fs::read_dir(&wal_path).with_context(|| {
            format!(
                "trying to ensure wal directory {} exists for trace of relation {}",
                id,
                wal_path.display()
            )
        })?;

        // Create a new directory to store the trace
        fs::create_dir(&trace_path).with_context(|| {
            format!("trying to create trace directory: {}", trace_path.display())
        })?;

        Ok(Self {
            trace_path,
            wal_path,
            batches: Vec::new(),
            compaction: None,
        })
    }

    /// Remove all on-disk data for this trace.
    ///
    /// Importantly, we also delete the WAL directory here (the WAL writer
    /// only gets to add new files and can't do anything else). It's important
    /// for the `Trace` to do this, as the `Trace` reads from the `wal_path`
    /// independently of the WAL writer, and if the WAL writer were to delete
    /// the WAL directory, we would have to have either tighter coordination
    /// between the two, or continually on guard for having had the directory
    /// deleted while we were trying to read it.
    fn destroy(self) -> Result<(), anyhow::Error> {
        fs::remove_dir_all(&self.trace_path).with_context(|| {
            format!(
                "failed to remove trace directory {}",
                self.trace_path.display()
            )
        })?;
        fs::remove_dir_all(&self.wal_path).with_context(|| {
            format!("failed to remove wal directory {}", self.wal_path.display())
        })?;

        Ok(())
    }

    /// Checks if there are finished WAL segments and if so, forms them into batches.
    fn consume_wal(&mut self) -> Result<(), anyhow::Error> {
        let mut expected_lower = self
            .batches
            .last()
            .map(|batch| batch.upper)
            .unwrap_or_else(TimelyTimestamp::minimum);
        let finished_segments = self.find_finished_wal_segments()?;

        for segment in finished_segments {
            // Check that the new batch starts at the previous upper bound.
            let batch = Batch::create(&segment, &self.trace_path, expected_lower)?;
            expected_lower = batch.upper;
            self.batches.push(batch);
            // We only delete the WAL segment after the new `Batch` has been
            // durably persisted.
            // TODO: Need to fsync wal directory here to persist the removal.
            // Maybe we should do all of the deletes at once.
            fs::remove_file(&segment).with_context(|| {
                format!(
                    "failed to remove consumed wal segment {}",
                    segment.display()
                )
            })?;
        }

        Ok(())
    }

    fn find_finished_wal_segments(&self) -> Result<Vec<PathBuf>, anyhow::Error> {
        lazy_static! {
            static ref FINISHED_WAL_SEGMENT_REGEX: Regex =
                Regex::new("^log-[0-9]+-final$").unwrap();
        }

        let mut segments = read_dir_regex(&self.wal_path, &FINISHED_WAL_SEGMENT_REGEX)?;
        // Sort the segments by their WAL sequence number.
        // TODO: we need to check that the sequence numbers are contiguous and
        // directly follow the last sequence number consumed.
        segments.sort_by_key(|segment| {
            segment
                .to_str()
                .unwrap()
                .split('-')
                .nth(1)
                .unwrap()
                .parse::<usize>()
                .unwrap()
        });

        Ok(segments)
    }

    /// Checks for the unfinished WAL segment.
    ///
    /// This code assumes that the WAL writer always creates a new segment atomically
    /// with marking the old one finished.
    /// TODO: this assumption is inaccurate and kind of hard to justify, especially if
    /// we wanted to later support "static" or "closed" tables.
    fn find_unfinished_wal_segment(&self) -> Result<PathBuf, anyhow::Error> {
        lazy_static! {
            static ref UNFINISHED_WAL_SEGMENT_REGEX: Regex = Regex::new("^log-[0-9]+$").unwrap();
        }

        let mut segments = read_dir_regex(&self.wal_path, &UNFINISHED_WAL_SEGMENT_REGEX)?;
        match segments.len() {
            1 => Ok(segments.pop().unwrap()),
            0 => {
                bail!(
                    "Expected at least a single unfinished wal segment at {}. Found none.",
                    self.wal_path.display()
                )
            }
            l => {
                bail!(
                    "Expected only a single unfinished wal segment at {}. Found {}",
                    self.wal_path.display(),
                    l
                )
            }
        }
    }

    /// Recover all of the `Batch`s we previously knew about (to be used after a
    /// restart)
    ///
    /// This function also removes `Batch`s that are strict subsets of other `Batch`s.
    fn find_batches(&self) -> Result<Vec<Batch>, anyhow::Error> {
        lazy_static! {
            static ref BATCH_REGEX: Regex = Regex::new("^batch-[0-9]+-[0-9]+$").unwrap();
        }

        let batches = read_dir_regex(&self.trace_path, &BATCH_REGEX)?;

        let mut batches: Vec<Batch> = batches
            .into_iter()
            .map(Batch::reinit)
            .collect::<Result<_, _>>()
            .unwrap();

        // Sort `Batch`s by (lower increasing, upper decreasing) so that we can greedily
        // take the `Batch`s that span the largest intervals. We know that we can do this
        // since we control `Batch` compaction, and all `Batch`s either cover disjoint time
        // intervals or are strict subsets of another containing `Batch`.
        batches.sort_by(|a, b| a.lower.cmp(&b.lower).then(a.upper.cmp(&b.upper).reverse()));
        let mut batches_to_keep = vec![];
        let mut batches_to_remove = vec![];
        let mut previous_upper = TimelyTimestamp::minimum();
        let mut max_upper = TimelyTimestamp::minimum();

        // Greedily select the `Batch`s covering the largest time intervals.
        for batch in batches {
            max_upper = std::cmp::max(batch.upper, max_upper);
            if batch.lower == previous_upper {
                previous_upper = batch.upper;
                batches_to_keep.push(batch);
            } else {
                batches_to_remove.push(batch);
            }
        }

        // Verify that all batches span disjoint intervals and that together they
        // span a contiguous interval from [0, upper)
        for batch in &batches_to_keep {
            if batch.lower != previous_upper {
                bail!(
                    "Non-contiguous batch data on restart. Expected lower bound {} received {}",
                    previous_upper,
                    batch.lower
                );
            }
            previous_upper = batch.upper;
        }

        // Double check that our assumptions about the structure of `Batch`s hold.
        if previous_upper != max_upper {
            bail!(
                "Corrupted data. Expected to have data up to time {}, but only have data up to {}",
                max_upper,
                previous_upper
            );
        }

        for batch in batches_to_remove {
            fs::remove_file(&batch.path).with_context(|| {
                format!("failed to remove replaced batch {}", batch.path.display())
            })?;
        }

        Ok(batches_to_keep)
    }

    /// Try to compact all of the batches we know about into a single batch from
    /// [lower, upper) with updates forwarded up to the compaction frontier.
    ///
    /// TODO: the approach to compacting is likely very suboptimal.
    fn compact(&mut self) -> Result<(), anyhow::Error> {
        self.consume_wal()?;

        if self.batches.len() > 10 {
            let batches = std::mem::replace(&mut self.batches, vec![]);
            let batch = Batch::compact(&batches, &self.trace_path, self.compaction)?;
            self.batches.push(batch);

            // TODO: This seems like potentially a place with a weird failure mode, because
            // we might crash before we delete all of the now irrelevant `Batch`s.
            for batch in batches {
                // TODO: need to fsync() the parent directory here to persist this removal.
                fs::remove_file(&batch.path).with_context(|| {
                    format!("failed to remove replaced batch {}", batch.path.display())
                })?;
            }
        }

        Ok(())
    }

    /// Re-initialize a trace based on the available batch files on disk.
    pub fn resume(
        id: GlobalId,
        traces_path: &Path,
        wals_path: &Path,
    ) -> Result<Self, anyhow::Error> {
        // Need to instantiate a new trace and figure out what batches
        // we have access to.
        let trace_path = traces_path.join(id.to_string());
        let wal_path = wals_path.join(id.to_string());
        let mut ret = Self {
            trace_path,
            wal_path,
            batches: Vec::new(),
            compaction: None,
        };

        // Reload the `Batch`s we had previously written to `trace_path`.
        ret.batches = ret.find_batches()?;

        Ok(ret)
    }

    /// Read in the data for this relation, from all available batches and
    /// WAL segments back into memory.
    pub fn read(&self) -> Result<Vec<Message>, anyhow::Error> {
        let mut out = vec![];

        for batch in self.batches.iter() {
            let mut messages = batch.read()?;
            out.append(&mut messages);
        }

        let mut expected_lower = self
            .batches
            .last()
            .map(|batch| batch.upper)
            .unwrap_or_else(TimelyTimestamp::minimum);

        let finished_segments = self.find_finished_wal_segments()?;
        let unfinished_segment = self.find_unfinished_wal_segment()?;

        // Read messages in sorted by time. Each batch is assumed to have sorted data by
        // time, as does each wal segment.
        for segment in finished_segments {
            let mut messages = read_segment(&segment)?;
            let (lower, upper) = get_lower_and_upper_bounds(&messages)?;

            if lower != expected_lower {
                if lower > expected_lower || upper > expected_lower {
                    // We've recived a segment that covers either a non-contiguous interval of time (some times are
                    // missing) or a partially overlapping interval of time. Either way, we need to error out.
                    bail!("Received batch with [lower, upper) bounds [{}, {}), expected lower bound: {}",
                        lower,
                        upper,
                        expected_lower
                    );
                } else {
                    // We've already read this segment and made a batch, can now remove it.
                    fs::remove_file(&segment).with_context(|| {
                        format!(
                            "failed to remove already consumed wal segment {}",
                            segment.display()
                        )
                    })?;
                }
            } else {
                out.append(&mut messages);
                expected_lower = upper;
            }
        }

        let mut messages = read_segment(&unfinished_segment)?;
        let segment_lower_bound = get_lower_bound(&messages)?;

        if segment_lower_bound != expected_lower {
            bail!(
                "Received in progress segment with lower bound {} expected {}",
                segment_lower_bound,
                expected_lower
            );
        }

        out.append(&mut messages);

        // Remove duplicated progress messages across wal segments.
        out.dedup();
        Ok(out)
    }
}

/// The `Compacter` is (currently) a tokio task that receives instructions from
/// the `Coordinator` and maintains `Trace`s for various relations.
pub struct Compacter {
    rx: mpsc::UnboundedReceiver<CompacterMessage>,
    traces: HashMap<GlobalId, Trace>,
    traces_path: PathBuf,
    wals_path: PathBuf,
}

impl Compacter {
    pub fn new(
        rx: mpsc::UnboundedReceiver<CompacterMessage>,
        traces_path: PathBuf,
        wals_path: PathBuf,
    ) -> Result<Self, anyhow::Error> {
        Ok(Self {
            rx,
            traces: HashMap::new(),
            traces_path,
            wals_path,
        })
    }

    async fn compact(&mut self) -> Result<(), anyhow::Error> {
        let mut interval = tokio::time::interval(COMPACTER_INTERVAL);
        loop {
            select! {
                data = self.rx.recv() => {
                    if let Some(data) = data {
                        self.handle_message(data)?
                    } else {
                        break;
                    }
                }
                _ = interval.tick() => {
                    for (_, trace) in self.traces.iter_mut() {
                        // Check to see if the WAL still exists
                        // if so, check to see if there are any pending log segments to ingest
                        // finally, check to see if we can compact the data.
                        trace.compact()?;
                    }
                }
            }
        }

        Ok(())
    }

    fn handle_message(&mut self, message: CompacterMessage) -> Result<(), anyhow::Error> {
        match message {
            CompacterMessage::Add(id) => {
                if self.traces.contains_key(&id) {
                    bail!(
                        "asked to create trace for relation {} which already exists.",
                        id
                    );
                }
                let trace_path = self.traces_path.join(id.to_string());
                let wal_path = self.wals_path.join(id.to_string());

                let trace = Trace::create(id, trace_path, wal_path)?;
                self.traces.insert(id, trace);
            }
            CompacterMessage::Drop(id) => {
                if !self.traces.contains_key(&id) {
                    bail!(
                        "asked to drop trace for relation {} which doesn't exist.",
                        id
                    );
                }

                let trace = self.traces.remove(&id).expect("trace known to exist");
                trace.destroy()?;
            }
            CompacterMessage::Resume(id, trace) => {
                if self.traces.contains_key(&id) {
                    bail!(
                        "asked to resume trace for relation {} which already exists.",
                        id
                    );
                }
                self.traces.insert(id, trace);
            }
            CompacterMessage::AllowCompaction(id, frontier) => {
                // We might get a lot of messages for relations we don't
                // know about here so ignore those.
                if let Some(trace) = self.traces.get_mut(&id) {
                    if let Some(compaction_frontier) = trace.compaction {
                        assert!(frontier >= compaction_frontier);
                    }
                    trace.compaction = Some(frontier);
                }
            }
        };
        Ok(())
    }

    pub async fn run(&mut self) {
        let ret = self.compact().await;

        match ret {
            Ok(_) => (),
            Err(e) => {
                error!("Compacter thread encountered an error: {:#}", e);
                error!("Shutting down compacter thread. No further updates will be persisted.");
            }
        }
    }
}

/// Read a directory and return all (non-subdirectory) files matching `regex`.
fn read_dir_regex(path: &Path, regex: &Regex) -> Result<Vec<PathBuf>, anyhow::Error> {
    let entries = std::fs::read_dir(path).with_context(|| {
        format!(
            "failed to read {} looking for {}",
            path.display(),
            regex.as_str()
        )
    })?;
    let mut results = vec![];
    for entry in entries {
        if let Ok(file) = entry {
            let path = file.path();
            let file_name = path.file_name();
            if file_name.is_none() {
                continue;
            }

            let file_name = file_name.unwrap().to_str();

            if file_name.is_none() {
                continue;
            }

            let file_name = file_name.unwrap();
            if regex.is_match(&file_name) {
                results.push(path.to_path_buf());
            }
        }
    }

    Ok(results)
}

/// Find the [lower, upper) timestamp bounds for the data contained in `messages`.
///
/// The first and last messages are required to be instances of `Message::Progress`
/// that denote those bounds.
fn get_lower_and_upper_bounds(
    messages: &[Message],
) -> Result<(Timestamp, Timestamp), anyhow::Error> {
    if messages.len() < 2 {
        bail!(
            "Only received {} messages, expected at least 2",
            messages.len()
        );
    }

    // The first and last messages in this list have to be progress messages that indicate the [lower, upper) bounds
    // for the new `Batch`.
    let lower = match messages.first().expect("known to exist") {
        Message::Progress(time) => *time,
        Message::Data(_) => bail!(
            "Invalid data in segment, expected first message to be a progress message, found data"
        ),
    };

    let upper = match messages.last().expect("known to exist") {
        Message::Progress(time) => *time,
        Message::Data(_) => bail!(
            "Invalid data in segment, expected last message to be a progress message, found data"
        ),
    };

    if lower > upper {
        bail!("Invalid lower {} and upper {} bounds.", lower, upper);
    }

    Ok((lower, upper))
}

/// Find the lower timestamp bound for data in `messages`.
///
/// The first message is required to be an instance of `Message::Progress` that
/// denotes this bound.
fn get_lower_bound(messages: &[Message]) -> Result<Timestamp, anyhow::Error> {
    if messages.is_empty() {
        bail!("Received no messages, expected at least 1");
    }

    // The first message has to be a progress message that indicates a lower bound for timestamps for this list of messages.
    let lower = match messages.first().expect("known to exist") {
        Message::Progress(time) => *time,
        Message::Data(_) => bail!(
            "Invalid data in segment, expected first message to be a progress message, found data"
        ),
    };

    Ok(lower)
}
