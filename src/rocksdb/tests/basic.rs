// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::metrics::{CounterVecExt, HistogramVecExt};
use mz_rocksdb::config::SharedWriteBufferManager;
use mz_rocksdb::{
    InstanceOptions, RocksDBConfig, RocksDBInstance, RocksDBInstanceMetrics, RocksDBSharedMetrics,
};
use mz_rocksdb_types::RocksDBTuningParameters;
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, Opts};

fn shared_metrics_for_tests() -> Result<Box<RocksDBSharedMetrics>, anyhow::Error> {
    let fake_hist_vec =
        HistogramVec::new(HistogramOpts::new("fake", "fake_help"), &["fake_label"])?;

    Ok(Box::new(RocksDBSharedMetrics {
        multi_get_latency: fake_hist_vec.get_delete_on_drop_histogram(vec!["one".to_string()]),
        multi_put_latency: fake_hist_vec.get_delete_on_drop_histogram(vec!["four".to_string()]),
    }))
}

fn instance_metrics_for_tests() -> Result<Box<RocksDBInstanceMetrics>, anyhow::Error> {
    let face_counter_vec =
        IntCounterVec::new(Opts::new("fake_counter", "fake_help"), &["fake_label"])?;

    Ok(Box::new(RocksDBInstanceMetrics {
        multi_get_size: face_counter_vec.get_delete_on_drop_counter(vec!["two".to_string()]),
        multi_get_result_count: face_counter_vec
            .get_delete_on_drop_counter(vec!["three".to_string()]),
        multi_get_result_bytes: face_counter_vec
            .get_delete_on_drop_counter(vec!["four".to_string()]),
        multi_get_count: face_counter_vec.get_delete_on_drop_counter(vec!["five".to_string()]),
        multi_put_count: face_counter_vec.get_delete_on_drop_counter(vec!["six".to_string()]),
        multi_put_size: face_counter_vec.get_delete_on_drop_counter(vec!["seven".to_string()]),
    }))
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rocksdb_create_default_env` on OS `linux`
async fn basic() -> Result<(), anyhow::Error> {
    // If the test aborts, this may not be cleaned up.
    let t = tempfile::tempdir()?;

    let mut instance = RocksDBInstance::<String, String>::new(
        t.path(),
        t.path(),
        InstanceOptions::defaults_with_env(rocksdb::Env::new()?),
        RocksDBConfig::new(Default::default(), None),
        shared_metrics_for_tests()?,
        instance_metrics_for_tests()?,
        bincode::DefaultOptions::new(),
    )
    .await?;

    let mut ret = vec![Default::default(); 1];
    instance
        .multi_get(vec!["one".to_string()], ret.iter_mut(), |value| value)
        .await?;

    assert_eq!(
        ret.into_iter()
            .map(|v| v.map(|v| v.value))
            .collect::<Vec<_>>(),
        vec![None]
    );

    instance
        .multi_put(vec![
            ("one".to_string(), Some("onev".to_string())),
            // Deleting a non-existent key shouldn't do anything
            ("two".to_string(), None),
        ])
        .await?;

    let mut ret = vec![Default::default(); 2];
    instance
        .multi_get(
            vec!["one".to_string(), "two".to_string()],
            ret.iter_mut(),
            |value| value,
        )
        .await?;

    assert_eq!(
        ret.into_iter()
            .map(|v| v.map(|v| v.value))
            .collect::<Vec<_>>(),
        vec![Some("onev".to_string()), None]
    );

    instance
        .multi_put(vec![
            // Double-writing a key should keep the last one.
            ("two".to_string(), Some("twov1".to_string())),
            ("two".to_string(), Some("twov2".to_string())),
        ])
        .await?;

    let mut ret = vec![Default::default(); 2];
    instance
        .multi_get(
            vec!["one".to_string(), "two".to_string()],
            ret.iter_mut(),
            |value| value,
        )
        .await?;

    assert_eq!(
        ret.into_iter()
            .map(|v| v.map(|v| v.value))
            .collect::<Vec<_>>(),
        vec![Some("onev".to_string()), Some("twov2".to_string())]
    );

    instance.close().await?;

    Ok(())
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rocksdb_create_default_env` on OS `linux`
async fn shared_write_buffer_manager() -> Result<(), anyhow::Error> {
    // If the test aborts, this may not be cleaned up.
    let t = tempfile::tempdir()?;

    let write_buffer_memory_bytes = 10000;

    let shared_write_buffer_manager: SharedWriteBufferManager = SharedWriteBufferManager::default();
    let mut tuning_parameters: RocksDBTuningParameters = RocksDBTuningParameters {
        write_buffer_manager_memory_bytes: Some(write_buffer_memory_bytes),
        ..Default::default()
    };
    let mut rocksdb_config = RocksDBConfig::new(shared_write_buffer_manager.clone(), None);
    rocksdb_config.apply(tuning_parameters.clone());

    let instance1 = RocksDBInstance::<String, String>::new(
        t.path().join("1").as_path(),
        t.path().join("1").as_path(),
        InstanceOptions::defaults_with_env(rocksdb::Env::new()?),
        rocksdb_config.clone(),
        shared_metrics_for_tests()?,
        instance_metrics_for_tests()?,
        bincode::DefaultOptions::new(),
    )
    .await?;

    assert!(shared_write_buffer_manager.get().is_some());
    {
        // Arc will be dropped by the end of this scope
        let buf = shared_write_buffer_manager.get().unwrap();
        assert!(buf.enabled());
        assert_eq!(write_buffer_memory_bytes, buf.buffer_size());
    }

    let updated_bytes = 20000;
    tuning_parameters.write_buffer_manager_memory_bytes = Some(updated_bytes);
    rocksdb_config.apply(tuning_parameters);

    let instance2 = RocksDBInstance::<String, String>::new(
        t.path().join("2").as_path(),
        t.path().join("2").as_path(),
        InstanceOptions::defaults_with_env(rocksdb::Env::new()?),
        rocksdb_config.clone(),
        shared_metrics_for_tests()?,
        instance_metrics_for_tests()?,
        bincode::DefaultOptions::new(),
    )
    .await?;

    instance1.close().await?;
    // The shared write buffer manager should still have a reference
    // since instance2 is still active and should be using existing write_buffer_manager
    assert!(shared_write_buffer_manager.get().is_some());
    {
        let buf = shared_write_buffer_manager.get().unwrap();
        assert!(buf.enabled());
        assert_eq!(write_buffer_memory_bytes, buf.buffer_size());
    }

    instance2.close().await?;
    // After both the instances are closed, the shared write buffer manager
    // should now be cleaned up
    assert!(shared_write_buffer_manager.get().is_none());

    let instance3 = RocksDBInstance::<String, String>::new(
        t.path().join("3").as_path(),
        t.path().join("3").as_path(),
        InstanceOptions::defaults_with_env(rocksdb::Env::new()?),
        rocksdb_config,
        shared_metrics_for_tests()?,
        instance_metrics_for_tests()?,
        bincode::DefaultOptions::new(),
    )
    .await?;

    assert!(shared_write_buffer_manager.get().is_some());
    {
        let buf = shared_write_buffer_manager.get().unwrap();
        assert!(buf.enabled());
        // The new instance will now use the updated write buffer manager
        assert_eq!(updated_bytes, buf.buffer_size());
    }

    instance3.close().await?;
    assert!(shared_write_buffer_manager.get().is_none());

    Ok(())
}
