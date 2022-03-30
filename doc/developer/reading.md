# Reading list

For the books listed below that are not freely available, Materialize engineers
are welcome to expense physical or electronic copies.

## Required reading

Employed engineers should read the following within their first few weeks at
Materialize:

* Napa: Powering Scalable Data Warehousing with Robust Query Performance at Google
  [[pdf](http://www.vldb.org/pvldb/vol14/p2986-sankaranarayanan.pdf)]

  An academic paper about Napa, an internal Google system that is quite similar
  to Materialize in both goals and architecture.

* Shared Arrangements: practical inter-query sharing for streaming dataflows
  [[pdf](https://vldb.org/pvldb/vol13/p1793-mcsherry.pdf)]

  A paper by [@frankmcsherry](https://github.com/frankmcsherry) et al. about
  how arrangements improve performance in stream processors.

* Scalability! But at what COST?
  [[pdf](https://www.usenix.org/system/files/conference/hotos15/hotos15-paper-mcsherry.pdf)]

  A paper by [@frankmcsherry](https://github.com/frankmcsherry) et al. about
  how many scalable big data systems fail to outperform a single-node system.

* How to do distributed locking
  [[web](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)]

  A blog post that addresses a common misconception about how locks work in
  distributed systems. You can read Martin Kleppmann's book *Designing
  Data-Intensive Applications*, listed below, instead.

* [Developer guide: submitting and reviewing changes](guide-changes.md)

  Our policies for submitting and reviewing PRs. It applies to our internal
  repositories too.

## Recommended reading

* *Designing Data-Intensive Applications* [[marketing website]](https://dataintensive.net)

  This book is a crash course in designing, implementing, and using databases.
  If you don't have a database background, you should *strongly* consider
  reading this book, though make your own assessment about which chapters are
  most relevant to you.

  There is a copy of this book in the office that you can borrow.

* Life in Differential Dataflow [[web](https://materialize.com/life-in-differential-dataflow/)]

  A blog post by Materialize engineer [@ruchirK](https://github.com/ruchirK)
  that introduces Differential Dataflow.

* *Programming Rust* [[marketing website](https://www.oreilly.com/library/view/programming-rust-2nd/9781492052586/)]

  Our recommended book for those new to Rust. You can also try the freely
  available book [*The Rust Programming Language*](https://doc.rust-lang.org/book/),
  but most engineers have preferred *Programming Rust*.

* *Rust for Rustaceans* [[marketing website](https://rust-for-rustaceans.com)]

  Our recommended book for learning more advanced Rust techniques once you've
  learned the basics. Some engineers have had success skipping *Programming
  Rust* and jumping right into this book instead.
