# Getting Started

If you're reading this, chances are that you're new to Materialize. Hello!

In this doc, we'll walk you through a few things you might want to do in this
repo to ensure you can readily start banging on Materialize.

## What is Materialize?

To get a better sense of what the product is, its internal topography, etc.
check out our high-level [architecture doc](architecture.md).

## Set up Materialize

The first thing you should do is check out the incredible setup scripts that
`quodlibetor` has created at
[mtrlz-setup](https://github.com/MaterializeInc/mtrlz-setup).

This includes setting up a lot of things you need to get work done, not the
least of which are:

- Materialize
- MySQL (an upstream database)
- Debezium (to enable CDC for MySQL)
- Kafka (to get MySQL data from Debezium into Materialize)

## Demoing Materialize

For some lightweight, proof-of-concept type demos, check out [demo.md](demo.md)

Once you've finished those and want to see a more robust example of what
Materialize can do, check out [Setting Up MySQL w/
Debezium](setup-mysql-debezium.md); you already have the first bits done, so
skip to the **Loading TPCH** section.

## If you liked content like this...

...you'll love the rest of the `doc` dir.
