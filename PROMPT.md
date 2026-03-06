Looks like there are bugs in the recently added subscribe-based frontend
read-then-write (RTW) implementation. Specifically, I want you do invetigate
why this test is failing and fix it.

bin/cargo-test -p mz-environmentd --test sql -- dont_drop_sinks_twice --nocapture

You might have to iterate and attempt multiple ways of fixing, re-running that
test, etc. At the end, when you fix the test run all the tests in that package
to ensure we didn't break anything. And if we did we need to fix that as well.

* Code should be simple and clean, never over-complicate things.
* At the end I want the minimal set of fixes, if we iterate and try multiple things, clean those up and fix only the minimum required to make all tests pass
* Each solid progress should be committed as a jj change
* Before committing, you should test that what you produced is high quality and that it works.
* Code should be very well commented, it needs to explain what is fixed and how and why
* At the end of this file, create a work in progress log, where you note what you already did, what is missing. Always update this log.
* Read this file again after each context compaction.
