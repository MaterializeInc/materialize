// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

impl Applier {
    pub async fn new(
        cfg: PersistConfig,
        shard_id: ShardId,
        metrics: Arc<Metrics>,
        state_versions: Arc<StateVersions>,
    ) -> Result<Self, Box<CodecMismatch>> {
        let shard_metrics = metrics.shards.shard(&shard_id);
        let state = metrics
            .cmds
            .init_state
            .run_cmd(&shard_metrics, |_cas_mismatch_metric| {
                // No cas_mismatch retries because we just use the returned
                // state on a mismatch.
                state_versions.maybe_init_shard(&shard_metrics)
            })
            .await?;
        Ok(Machine {
            cfg,
            metrics,
            shard_metrics,
            state_versions,
            state,
        })
    }

    async fn apply_unbatched_cmd<
        R,
        E,
        WorkFn: FnMut(SeqNo, &PersistConfig, &mut StateCollections<T>) -> ControlFlow<E, R>,
    >(
        &mut self,
        cmd: &CmdMetrics,
        mut work_fn: WorkFn,
    ) -> Result<(SeqNo, Result<R, E>, RoutineMaintenance), Indeterminate> {
        let is_write = cmd.name == self.metrics.cmds.compare_and_append.name;
        let is_rollup = cmd.name == self.metrics.cmds.add_and_remove_rollups.name;
        let shard_metrics = Arc::clone(&self.shard_metrics);
        cmd.run_cmd(&shard_metrics, |cas_mismatch_metric| async move {
            let mut garbage_collection;
            let mut expiry_metrics;

            loop {
                let was_tombstone_before = self.state.collections.is_tombstone();

                let (work_ret, mut new_state) = match self
                    .state
                    .clone_apply(&self.cfg, &mut work_fn)
                {
                    Continue(x) => x,
                    Break(err) => {
                        return Ok((self.state.seqno(), Err(err), RoutineMaintenance::default()))
                    }
                };
                expiry_metrics = new_state.expire_at((self.cfg.now)());

                // Sanity check that all state transitions have special case for
                // being a tombstone. The ones that do will return a Break and
                // return out of this method above. The one exception is adding
                // a rollup, because we want to be able to add a rollup for the
                // tombstone state.
                //
                // TODO: Even better would be to write the rollup in the
                // tombstone transition so it's a single terminal state
                // transition, but it'll be tricky to get right.
                if was_tombstone_before && !is_rollup {
                    panic!(
                        "cmd {} unexpectedly tried to commit a new state on a tombstone: {:?}",
                        cmd.name, self.state
                    );
                }

                // Find out if this command has been selected to perform gc, so
                // that it will fire off a background request to the
                // GarbageCollector to delete eligible blobs and truncate the
                // state history. This is dependant both on `maybe_gc` returning
                // Some _and_ on this state being successfully compare_and_set.
                //
                // NB: Make sure this overwrites `garbage_collection` on every
                // run though the loop (i.e. no `if let Some` here). When we
                // lose a CaS race, we might discover that the winner got
                // assigned the gc.
                garbage_collection = new_state.maybe_gc(is_write);

                // NB: Make sure this is the very last thing before the
                // `try_compare_and_set_current` call. (In particular, it needs
                // to come after anything that might modify new_state, such as
                // `maybe_gc`.)
                let diff = StateDiff::from_diff(&self.state, &new_state);
                // Sanity check that our diff logic roundtrips and adds back up
                // correctly.
                #[cfg(any(test, debug_assertions))]
                {
                    if let Err(err) =
                        StateDiff::validate_roundtrip(&self.metrics, &self.state, &diff, &new_state)
                    {
                        panic!("validate_roundtrips failed: {}", err);
                    }
                }

                // SUBTLE! Unlike the other consensus and blob uses, we can't
                // automatically retry indeterminate ExternalErrors here. However,
                // if the state change itself is _idempotent_, then we're free to
                // retry even indeterminate errors. See
                // [Self::apply_unbatched_idempotent_cmd].
                let expected = self.state.seqno();
                let cas_res = self
                    .state_versions
                    .try_compare_and_set_current(
                        &cmd.name,
                        &self.shard_metrics,
                        Some(expected),
                        &new_state,
                        &diff,
                    )
                    .await?;
                match cas_res {
                    Ok(()) => {
                        assert!(
                            self.state.seqno <= new_state.seqno,
                            "state seqno regressed: {} vs {}",
                            self.state.seqno,
                            new_state.seqno
                        );
                        self.state = new_state;

                        self.metrics
                            .lease
                            .timeout_read
                            .inc_by(u64::cast_from(expiry_metrics.readers_expired));

                        if let Some(gc) = garbage_collection.as_ref() {
                            debug!("Assigned gc request: {:?}", gc);
                        }

                        let maintenance = RoutineMaintenance {
                            garbage_collection,
                            write_rollup: self.state.need_rollup(),
                        };

                        return Ok((self.state.seqno(), Ok(work_ret), maintenance));
                    }
                    Err(diffs_to_current) => {
                        cas_mismatch_metric.0.inc();

                        let seqno_before = self.state.seqno;
                        let diffs_apply = diffs_to_current
                            .first()
                            .map_or(true, |x| x.seqno == seqno_before.next());
                        if diffs_apply {
                            self.metrics.state.update_state_fast_path.inc();
                            self.state.apply_encoded_diffs(
                                &self.cfg,
                                &self.metrics,
                                &diffs_to_current,
                            )
                        } else {
                            // Otherwise, we've gc'd the diffs we'd need to
                            // advance self.state to where diffs_to_current
                            // starts so we need a new rollup.
                            self.metrics.state.update_state_slow_path.inc();
                            debug!(
                                "update_state didn't hit update_state fast path {} {:?}",
                                self.state.seqno,
                                diffs_to_current.first().map(|x| x.seqno),
                            );

                            // SUBTLE: Consensus::compare_and_set guarantees
                            // that we get back everything in
                            // `[max(expected.next(),earliest),current]` on
                            // expectation mismatch. If `diffs_apply` is false
                            // (this branch), then we know `earliest >
                            // expected.next()` and so `diffs_to_current` is
                            // already the full set of all live diffs. Sanity
                            // check this deduction with an assert and then use
                            // it to `fetch_current_state`.
                            assert!(
                                diffs_to_current
                                    .first()
                                    .map_or(false, |x| x.seqno > expected.next()),
                                "{:?} vs {}",
                                diffs_to_current.first(),
                                expected.next()
                            );
                            let all_live_diffs = diffs_to_current;
                            self.state = self
                                .state_versions
                                .fetch_current_state(&self.state.shard_id, all_live_diffs)
                                .await
                                .expect("shard codecs should not change");
                        }

                        // Intentionally don't backoff here. It would only make
                        // starvation issues even worse.
                        continue;
                    }
                }
            }
        })
        .await
    }

    pub async fn fetch_and_update_state(&mut self) {
        let seqno_before = self.state.seqno;
        self.state_versions
            .fetch_and_update_to_current(&mut self.state)
            .await
            .expect("shard codecs should not change");
        assert!(
            seqno_before <= self.state.seqno,
            "state seqno regressed: {} vs {}",
            seqno_before,
            self.state.seqno
        );
    }
}
