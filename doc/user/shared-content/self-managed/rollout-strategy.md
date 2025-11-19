The following procedure uses the default `rolloutStrategy` of `WaitUntilReady`.
When using `WaitUntilReady`, new instances are created and all dataflows are
determined to be ready before cutover and terminating the old version. As such,
this strategy temporarily requires twice the resources during the transition.
Ensure you have enough resources to support having both the old and new
Materialize instances running.
