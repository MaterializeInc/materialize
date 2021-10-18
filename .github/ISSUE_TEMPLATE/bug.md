---
name: Bug
about: >
  A defect in an existing feature. For example, "consistency is violated if
  Kafka Connect restarts."
labels: C-bug
---

<!--
Thanks for your feedback on Materialize! Please include the following
information in your bug report.
-->

### What version of Materialize are you using?

From the shell:

```
$ materialized -v

```

Or from the SQL console:

```
materialize=> select mz_version();

```

### How did you install Materialize?

<!-- Choose one. -->

* [ ] Docker image
* [ ] Linux release tarball
* [ ] APT package
* [ ] macOS release tarball
* [ ] Homebrew tap
* [ ] Built from source
* [ ] Materialize Cloud

### What was the issue?

### Is the issue reproducible? If so, please provide reproduction instructions.

### Please attach any applicable log files.

<!--
Consider including:
    * mzdata/materialized.log
    * Kafka logs, if using Kafka sources or sinks
    * Kinesis logs, if using Kinesis sources
-->
