# Persist 101
Persist is a data store backed by immutable blobs.
## Semantics
**First, _what_ is Persist?**

It's a durable store of time-varying collections.

Data is organized into collections called "shards".
Individual records within a collection are _(key, value, multiplicity)_.

For the time window between a shard's **"since"** (lower bound) and its **"frontier"** (upper bound), we know the history of the collection and can query its contents.
- Before the lower bound, we no longer remember the history of the collection.
- At or beyond the frontier, the collection's state is still being decided.

We can only write (append) to a collection _at or beyond_ the frontier. We cannot alter history before that point.

## Data Layout in Blob Storage
**What does it actually look like?**
### Layout: Shard > Batch > Run > Part
Within a shard, records are organized into:
- Batches. Together, they cover the entire `[Since, Frontier)` time range without overlapping. Each timestamp within the range belongs to a single batch.
- Runs. Each batch is made up of one or more runs. Runs are an artifact of how batches are written, not a semantic separation of records.
- Parts. Each run is made up of one or more parts. Each _part_ of a _run_ is responsible for a range of keys.
  Within a part, records are typically sorted by key, and within a run, parts are sorted by their key ranges (which do not overlap).
	- _Parts are internally sorted by key only if they've been through compaction. For large shards, most parts have been through compaction._
#### Data Layout in a Part File
Part files are Parquet, where the columns are:
- `t` - timestamp
- `d` - diff
- `k_s` - Arrow-encoded Parquet "key", which is either:
	- `ok` - struct with one field per relation column
	- `err` - binary-encoded `DataflowError`
- `v_s` - unused
Our Parquet writer is simple:
- only one row group
- no statistics (Predicate push-down chooses which part files to read based on stats we write into shard state--outside the part files themselves.)

_Note: There is also a legacy format with columns `k, v, t, d` and a migration format with `k, v, t, d, k_s, v_s`._
#### Interlude on Future Work: Predicate Push-Down _within_ a Part
Today, we always read and decode an entire part file.
If we stop doing that, maybe we can support point (key + time) lookups and improve predicate push-down.

Claude's diagram of Parquet chunks and pages within our one row group:
```
File
└── Row group            (a horizontal slice of rows, all columns)
    ├── Column chunk "t" (all of t's values for those rows)
    │   ├── Page         (~1 MB of t values)
    │   ├── Page
    │   └── Page
    ├── Column chunk "d"
    │   └── Page ...
    └── Column chunk "k_s"
        └── Page ..
```

Steps to doing better:
- Split the file into multiple row groups.
- Enable `Chunk` or `Page` statistics, which gives us a page index (in the parquet footer) with stats (e.g. column min/max/nulls) for those segmentations of the part file.
- Add "Range Read" to our Blob Store interface to read from specific offsets within a file. Then, only read the row groups or pages whose stats fit the predicate.
## Writers
**What uses Persist?**
### Materialized View sink
Multiple replicas can write to the same shard, including replicas running different versions of the code.
- We can't assume all writers agree on the collection's contents.
	- e.g. After a bugfix, newer replicas will disagree with older replicas.
- A writer can't assume the previous batches in Persist were written by writers it agrees with.

Therefore, if a writer wants the shard to match its own view of the collection, it must _read the shard_, calculate the diff, and commit the diff.
We call this behavior _self-correction_ because the winning writer erases any accumulated mistakes from the collection.

_On each replica:_

1. One worker chooses the time range for a batch.
    - It also _selects which worker_ is responsible for `compare_and_append`ing the batch.
2. All workers write their own parts for the batch.
3. The _selected worker_ `compare_and_append`s all the workers' runs.

### Source exports' Persist sinks
For each of a _source's_ (e.g. database) _exports_ (e.g. table), we run a `persist_sink` dataflow to record that export's collection of records.

_On each replica:_ 
1. One worker chooses the time range for a batch.
2. All workers write their own parts for the batch.
   - _N.B._ Because sources don't guarantee key ordering, each part can span the entire key range. Therefore, each part is its own run.
   - _Fun fact:_ Workers write their parts as single-timestamp batches, which the leader worker consolidates into one Persist batch, typically spanning multiple timestamps.
3. One worker `compare_and_append`s the batch with each worker's run(s).
   - _Multiple Replicas:_ Kafka and load-generator sources run on multiple replicas, which compete for a successful `compare_and_append`.
   Postgres, MySQL, and SQL Server only run on a single replica.

_Fun fact x2:_ The source exports' Persist sink implementation is derived from the materialized view sink, with the self-correction step removed.
### Tables, `txn-wal`
### More Writers
TODO
These other components write to Persist but are not drivers of its design:
- Sinks: only record progress
- Builtin tables during 0dt upgrade: because the new generation can't modify `txn-wal`. (doc says it's a temporary hack?)
- COPY FROM: uses the table path
- Webhook sources
- Storage-controller collections
- Catalog
- Expression cache
- Builtin schema migration shard
- Dropped-shard cleanup
## Readers
TODO
### Reading a Snapshot
- Get all _batches_ for the shard, up to the _time_ we want to read.
- In parallel, read all the _parts_ for all the _runs_ in those batches.
TODO
### Interlude on Future Work: Consolidate on Read
TODO
## Consensus
TODO