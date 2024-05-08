# Restructure + genericize subsource planning

-   Associated:
    -   https://github.com/MaterializeInc/materialize/issues/20208
    -   https://github.com/MaterializeInc/materialize/pull/26881
    -   https://github.com/MaterializeInc/materialize/issues/26999

## The Problems

-   To complete #20208, we have to know the schema of the Kafka topic. This
    schema is most readily knowable after planning the `SOURCE`.
-   Subsource planning (generating `CreateSubsourceStatement`) currently happens
    during purification before planning the `SOURCE`.
-   Purification's current structure also means that we have to implement some
    bits in purification (for new sources) and some in sequencing (when
    reconciling additional subsource's definitions with the current source).
    This would be cleaner if we described the semantics of e.g. source options
    somewhere like `SourceConnection`.

## Success Criteria

-   We generate `CreateSubourceStatement`s _after_ planning the
    `CreateSourceStatement`. This unblocks the ability to theoretically create
    subsources for Kafka's output. Doing this, as far as I envision, also
    genericizes subsource planning.
-   We demonstrate, using PostgreSQL sources as an example, that it is possible
    to separate "planning" and purification's async concerns, e.g.
    `PurificationError` wraps some other error that expresses the semantics of
    handling PostgreSQL `TEXT COLUMNS`.

## Out of Scope

-   Actually creating subsources for Kafka sources.
-   Schema migrations
-   Fully separating concerns w/r/t purification, i.e. I am not planning to
    address [MySQL source's blending of
    concerns](https://github.com/MaterializeInc/materialize/issues/26999),
    though I think that the spirit of this change should include that task.
-   Supporting `ALTER SOURCE...ADD SUBSOURCE` for MySQL, though that should be
    relatively simple to do.

## Solution Proposal

1.  Modularize source planning. Source planning will become more complex and the
    current code structure stretches many people's ability to comprehend what's
    happening.
2.  Move the API for planning that occurs during purification into a spot more
    semantically appropriate, e.g. let the `SouceConnection` trait define how
    its options should be allowed to be used. e.g. we should provide common
    errors that both `PurificationError` (for new sources) and `AdapterError`
    (for additional subsources) can wrap to signal that there are issues with
    source definitions.
3.  Restructure purification such that we track the names of the sources we've
    been requested to create, as well as the "upstream" objects to which they
    refer––i.e. instead of generating `Vec<CreateSubsourceStatement>` we should
    generate `BTreeMap<SubsourceName, UpstreamObjectName>`.
4.  Add a fn to the `SourceConnection` trait like:

    ```rust
    fn get_output_schema_for_name(
        &self,
        upstream_object_name: UpstreamObjectName
    ) -> Result<RelationDesc, Err>
    ```

    This will let us ask for the given schema of a subsource after the source
    itself is planned.

    `UpstreamObjectName` in the code is likely to just be `UnresolvedItemName`.

5.  After planning the primary source, plan all subsources by generating
    `CreateSubourceStatement`s using the name, reference mapping; we'll get the
    schema for the new objects by looking them up in the `Source`.

### `ALTER SOURCE...ADD SUBSOURCE`

We should also refactor the way that `ALTER SOURCE...ADD SUBSOURCE` works to
instead describe the data it wishes add to the source, and then merge it into
the existing source's "external metadata," e.g. the `Vec<PostgresTableDesc>`,
obviously error checking for duplicate references, etc.

To support this, we can extend the `SourceConnection` trait further to
describe the kind of transformations we will allow to each type of source.
For PG and MySQL this is simple to envision as something like:

```rust
fn merge_external_data<T, O>(
    &mut self, metadata: Vec<T>, options: Vec<O>
) -> Result<(), Err>
```

Then, being able to add subsources to a source will just require implementing a
merge algorithm for the new metadata and options.

This is a little more fraught with updating Kafka metadata, so I'm not totally
sure how we should proceed there; this issue's scope is limited to the existing
source/subsource structure. I don't think this enters any traps that preclude
generalizing this concept for Kafka.

Once we've merged the new and existing data into the `Source`, we can use the
same code path we use when creating the initial set of subsources to add the
subsequent sets.

#### Mind Palace

There's a world in which creating a new source also uses `merge_external_data`;
the only distinction being that new sources start from the empty state, and
modified sources start with whatever state they had when they were modified.

I am not firmly committed to this idea, but the gist of it seems sound. If we
upheld this contract, writing `CREATE SOURCE` _could_ also immediately get you
`ALTER SOURCE...ADD SUBSOURCE`, at least for sources like PG and MySQL.

One of the implications this has that I think I've been skirting around, is that
you want to know what the "current" state of the `GenericSourceConnection` is to
make this happen. This, then, is not something that happens in the "async stage"
of purification, but can only be done when it rejoins the main coordinator
thread. This then feels to me like we want to split up purification into an
async stage where only async work is performed, and then a normalization stage
that occurs on the main coord thread. None of this normalization work is to
expensive w/r/t time, so it doesn't feel that bad to me.

But, again, this is just how I'm thinking of this code and not something that I
am certain will show up in the final commit.

## Minimal Viable Prototype

idk, man

## Alternatives

🍝🍝 more 🍝 spaghetti 🍝🍝

## Open questions

What have I overlooked that will receive blocking feedback on?
