---
title: "Timely and Differential Dataflow"
description: "Learn about Materialize's underlying dataflow engine"
menu:
  main:
    parent: 'overview'
    weight: 4
---

Materialize's underlying dataflow engine is built atop two open-source Rust
projects:

  * [Timely Dataflow](https://github.com/TimelyDataflow/timely-dataflow)
  * [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow)

Timely and Differential Dataflow together form a novel incremental compute
engine that XXX: more words about how Materialize is blazing fast because of TD.
Their primary author is Materialize cofounder [Frank McSherry](https://github.com/frankmcsherry).

Materialize would like to recognized several developers who have made
substantial contributions to Timely and Differential Dataflow:

  * [Andrea Lattuada](https://github.com/utaal)
  * TODO: anyone else?

To learn more about Timely and Differential:

  * Read the [Naiad: A Timely Dataflow System][naiad] paper which introduced
    the concept of Timely Dataflow.
  * Check out the [Timely Dataflow book] and the [Differential Dataflow book].
  * Browse Frank McSherry's [blog posts][frank-blog], most of which cover
    various details of Timely and Differential.

[frank-blog]: https://github.com/frankmcsherry/blog
[naiad]: https://sigops.org/s/conferences/sosp/2013/papers/p439-murray.pdf
[Timely Dataflow book]: https://timelydataflow.github.io/timely-dataflow/
[Differential Dataflow book]: https://timelydataflow.github.io/differential-dataflow/
