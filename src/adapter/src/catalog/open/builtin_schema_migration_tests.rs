// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Turmoil-based tests for builtin schema migrations.

#![allow(clippy::unwrap_used)]

use std::cell::RefCell;
use std::rc::Rc;

use mz_catalog::builtin::{BuiltinSource, BuiltinTable};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_types::PersistLocation;
use mz_repr::{RelationDesc, SqlScalarType};
use mz_storage_client::controller::IntrospectionType;
use rand::rngs::SmallRng;
use rand::seq::IteratorRandom;
use rand::{Rng, SeedableRng};
use semver::Version;
use tracing::info;

use super::*;

#[mz_ore::test]
fn hydration_history_forced_migration_policy() {
    for table in [
        &*MZ_OBJECT_HYDRATION_HISTORY,
        &*MZ_REPLICA_HYDRATION_HISTORY,
    ] {
        let hydration_history = Builtin::Table(table);

        assert!(participates_in_forced_migration(
            &hydration_history,
            Mechanism::Evolution
        ));
        assert!(!participates_in_forced_migration(
            &hydration_history,
            Mechanism::Replacement
        ));
    }
}

/// A builtin that the source version never had has no shard registered. An `Evolution` step for
/// it, registered at a version above the source, must be skipped rather than bail out: the leader
/// allocates the shard at the target schema during bootstrap, and a read-only process that bailed
/// here would crash-loop. Regression test for SQL-713 (upgrade from before
/// `mz_replica_hydration_history` existed to a version that evolves it).
#[test] // allow(test-attribute)
#[cfg_attr(miri, ignore)] // too slow
fn test_evolution_skips_builtin_without_shard() {
    configure_tracing_for_turmoil();

    let mut sim = turmoil::Builder::new().build();
    let persist_location = init_persist(&mut sim);

    let (old_object, old_builtin) = make_builtin_table("old_table".into());
    let (new_object, new_builtin) = make_builtin_table("new_table".into());
    let mut system_objects = BTreeMap::new();
    // Present in the source version with the pre-evolution schema: the durable fingerprint is
    // stale and a shard exists, so the step must evolve it.
    system_objects.insert(
        old_object.clone(),
        ObjectInfo {
            global_id: GlobalId::System(1),
            shard_id: Some(ShardId::new()),
            builtin: evolve_builtin_desc(old_builtin),
            fingerprint: old_builtin.fingerprint(),
        },
    );
    // Introduced after the source version. Catalog open has just added its mapping at the current
    // fingerprint and no shard exists for it yet, which is what a real upgrade from before the
    // builtin existed looks like.
    system_objects.insert(
        new_object.clone(),
        ObjectInfo {
            global_id: GlobalId::System(2),
            shard_id: None,
            builtin: new_builtin,
            fingerprint: new_builtin.fingerprint(),
        },
    );

    let source_version = Version::new(0, 1, 0);
    let target_version = Version::new(0, 3, 0);
    let steps = vec![
        MigrationStep {
            version: Version::new(0, 2, 0),
            object: old_object.clone(),
            mechanism: Mechanism::Evolution,
        },
        MigrationStep {
            version: Version::new(0, 3, 0),
            object: new_object.clone(),
            mechanism: Mechanism::Evolution,
        },
    ];

    let migration_shard_id = ShardId::new();
    let result = Rc::new(RefCell::new(None));

    for (name, read_only) in [("read-only", true), ("leader", false)] {
        let system_objects = system_objects.clone();
        let steps = steps.clone();
        let host_result = Rc::clone(&result);
        let source_version = source_version.clone();
        let target_version = target_version.clone();
        let persist_location = persist_location.clone();
        sim.host(name, move || {
            let system_objects = system_objects.clone();
            let steps = steps.clone();
            let result = Rc::clone(&host_result);
            let source_version = source_version.clone();
            let target_version = target_version.clone();
            let persist_location = persist_location.clone();
            async move {
                let persist_cache = PersistClientCache::new_for_turmoil();
                let persist_client = persist_cache.open(persist_location).await.unwrap();
                let migration = Migration {
                    source_version,
                    target_version,
                    deploy_generation: 1,
                    system_objects,
                    migration_shard: migration_shard_id,
                    config: BuiltinItemMigrationConfig {
                        persist_client,
                        read_only,
                        force_migration: None,
                    },
                };
                let res = migration.run(&steps).await;
                result
                    .borrow_mut()
                    .replace((read_only, res.map(|r| r.new_fingerprints)));
                Ok(())
            }
        });
        sim.bounce(name);
        while result.borrow().is_none() {
            sim.step().unwrap();
        }
        let (was_read_only, res) = result.borrow_mut().take().unwrap();
        assert_eq!(was_read_only, read_only);
        let fingerprints = res.unwrap_or_else(|e| panic!("{name}: migration failed: {e}"));
        assert!(
            fingerprints.contains_key(&old_object),
            "{name}: the builtin with a shard must be evolved"
        );
        assert!(
            !fingerprints.contains_key(&new_object),
            "{name}: the builtin without a shard must be skipped"
        );
    }
}

#[test] // allow(test-attribute)
#[cfg_attr(miri, ignore)] // too slow
fn test_builtin_schema_migration() {
    const NUM_VERSIONS: u64 = 10;
    const NUM_BUILTINS: u64 = 5;
    const MAX_PROCESSES_PER_VERSION: u64 = 3;
    const MAX_MIGRATIONS_PER_VERSION: u64 = 2;
    const CRASH_PROBABILITY: f64 = 0.01;

    configure_tracing_for_turmoil();

    let seed = std::env::var("SEED")
        .ok()
        .and_then(|x| x.parse().ok())
        .unwrap_or_else(rand::random);

    info!("initializing rng with seed {seed}");
    let mut rng = SmallRng::seed_from_u64(seed);

    let mut sim = turmoil::Builder::new()
        .enable_random_order()
        .rng_seed(rng.random())
        .build();

    // Keep a list of all hosts, so we can randomly crash them.
    let mut all_hosts = BTreeSet::<String>::new();

    let persist_location = init_persist(&mut sim);
    all_hosts.extend(["consensus".into(), "blob".into()]);

    // Generate system objects at version 0.
    let mut system_objects = BTreeMap::new();
    for i in 0..NUM_BUILTINS {
        let (object, builtin) = match rng.random::<bool>() {
            true => make_builtin_table(format!("table{i}")),
            false => make_builtin_source(format!("source{i}")),
        };
        let info = ObjectInfo {
            global_id: GlobalId::System(i),
            shard_id: Some(ShardId::new()),
            builtin,
            fingerprint: builtin.fingerprint(),
        };
        system_objects.insert(object, info);
    }

    // State shared among the processes and the harness.
    let context = Rc::new(RefCell::new(Context {
        persist_location,
        source_version: Version::new(0, 0, 0),
        leader_version: Version::new(0, 0, 0),
        new_fingerprints: BTreeMap::new(),
    }));

    let migration_shard_id = ShardId::new();
    let mut migrations = Vec::new();
    let mut processes_by_version = BTreeMap::<Version, Vec<Process>>::new();

    // For each version, generate a varying number of schema migrations and spawn a varying number
    // of processes.
    for generation in 1..=NUM_VERSIONS {
        let version = Version::new(0, generation, 0);

        for _ in 0..rng.random_range(0..=MAX_MIGRATIONS_PER_VERSION) {
            let (object, info) = system_objects.iter_mut().choose(&mut rng).unwrap();

            info.builtin = evolve_builtin_desc(info.builtin);

            let mechanism = match rng.random::<bool>() {
                true => Mechanism::Evolution,
                false => Mechanism::Replacement,
            };
            let step = MigrationStep {
                version: version.clone(),
                object: SystemObjectDescription {
                    object_type: object.object_type,
                    schema_name: object.schema_name.clone(),
                    object_name: object.object_name.clone(),
                },
                mechanism,
            };
            migrations.push(step);
        }

        for i in 0..rng.random_range(0..=MAX_PROCESSES_PER_VERSION) {
            let version = version.clone();
            let system_objects = system_objects.clone();
            let migrations = migrations.clone();
            let context = Rc::clone(&context);

            let name = format!("{version}-{i}");
            let result = Rc::new(RefCell::new(None));
            let proc = Process {
                name: name.clone(),
                result: Rc::clone(&result),
            };
            processes_by_version
                .entry(version.clone())
                .or_default()
                .push(proc);

            sim.host(name.clone(), move || {
                let version = version.clone();
                let mut system_objects = system_objects.clone();
                let migrations = migrations.clone();
                let result = Rc::clone(&result);

                let ctx = context.borrow().clone();

                // Update durable fingerprints with results from past migrations.
                for (object, fingerprint) in &ctx.new_fingerprints {
                    let info = system_objects.get_mut(object).unwrap();
                    info.fingerprint = fingerprint.clone();
                }

                async move {
                    let read_only = ctx.leader_version != version;

                    let persist_cache = PersistClientCache::new_for_turmoil();
                    let persist_client = persist_cache.open(ctx.persist_location).await.unwrap();

                    let migration = Migration {
                        source_version: ctx.source_version,
                        target_version: version,
                        deploy_generation: generation,
                        system_objects,
                        migration_shard: migration_shard_id,
                        config: BuiltinItemMigrationConfig {
                            persist_client,
                            read_only,
                            force_migration: None,
                        },
                    };

                    let mut res = migration.run(&migrations).await.unwrap();

                    // Apply the cleanup action, possibly removing entries from the migration
                    // shard. We have not finalized the shards, but we don't care about leakage
                    // here.
                    let cleanup = std::mem::replace(&mut res.cleanup_action, async {}.boxed());
                    cleanup.await;

                    *result.borrow_mut() = Some(res);

                    Ok(())
                }
            });

            all_hosts.insert(name);
        }
    }

    info!(?system_objects, ?migrations, "running migrations");

    // Run upgrades to each version in turn.
    for (version, procs) in processes_by_version {
        info!("upgrading to version {version}");

        // Promote the processes at the target version.
        context.borrow_mut().leader_version = version.clone();
        for proc in &procs {
            proc.bounce(&mut sim);
            sim.bounce(proc.name.clone());
        }

        // Run until all processes at the target version have produced a result.
        while procs.iter().any(|p| p.result.borrow().is_none()) {
            sim.step().unwrap();

            // Randomly crash running processes.
            if rng.random_bool(CRASH_PROBABILITY) {
                let victim = all_hosts.iter().choose(&mut rng).unwrap().clone();
                if sim.is_host_running(victim.clone()) {
                    sim.bounce(victim);
                }
            }
        }

        // Assert that all processes have arrived at the same result.
        //
        // `shards_to_finalize` can differ because later processes might run the migration
        // shard cleanup when an earlier process has already removed the stale entries.
        let mut results = procs.iter().map(|p| p.result.borrow_mut().take().unwrap());
        let mut result = results.next().unwrap();
        results.for_each(|r| {
            assert_eq!(r.new_shards, result.new_shards);
            assert_eq!(r.new_fingerprints, result.new_fingerprints);
        });

        // Shut down the processes at the current version.
        // This simulates them getting fenced out by the next leader.
        for proc in &procs {
            sim.crash(proc.name.clone());
        }

        // Bump the source version.
        let mut ctx = context.borrow_mut();
        ctx.source_version = version;
        ctx.new_fingerprints.append(&mut result.new_fingerprints);
    }
}

/// Fuzz test builtin schema migration using turmoil.
#[test] // allow(test-attribute)
#[ignore = "runs forever"]
fn fuzz_builtin_schema_migration() {
    loop {
        test_builtin_schema_migration();
    }
}

/// Convert an owned value into a static reference to that value, leaking it in the process.
///
/// When setting up the test inputs (specifically `Builtin` values) we often need static references
/// when we only have owned types. To keep things simple, we liberally make use of `Box::leak` to
/// get these. We're not too worried about leaking memory in tests, though it's probably still a
/// bad idea to use this in hot loops.
fn leak<T>(x: T) -> &'static mut T {
    Box::leak(Box::new(x))
}

#[derive(Clone)]
struct Context {
    persist_location: PersistLocation,
    source_version: Version,
    leader_version: Version,
    new_fingerprints: BTreeMap<SystemObjectDescription, String>,
}

struct Process {
    name: String,
    result: Rc<RefCell<Option<MigrationRunResult>>>,
}

impl Process {
    fn bounce(&self, sim: &mut turmoil::Sim<'_>) {
        sim.bounce(self.name.clone());
        *self.result.borrow_mut() = None;
    }
}

fn make_builtin_table(name: String) -> (SystemObjectDescription, &'static Builtin<NameReference>) {
    let object = SystemObjectDescription {
        schema_name: "schema".into(),
        object_type: CatalogItemType::Table,
        object_name: name.clone(),
    };

    let builtin = BuiltinTable {
        name: name.leak(),
        schema: "schema",
        oid: 1,
        desc: RelationDesc::builder()
            .with_column("a", SqlScalarType::String.nullable(false))
            .finish(),
        column_comments: BTreeMap::new(),
        is_retained_metrics_object: false,
        access: Vec::new(),
        ontology: None,
    };
    let builtin = leak(Builtin::Table(leak(builtin)));

    (object, builtin)
}

fn make_builtin_source(name: String) -> (SystemObjectDescription, &'static Builtin<NameReference>) {
    let object = SystemObjectDescription {
        schema_name: "schema".into(),
        object_type: CatalogItemType::Table,
        object_name: name.clone(),
    };

    let builtin = BuiltinSource {
        name: name.leak(),
        schema: "schema",
        oid: 1,
        data_source: IntrospectionType::Frontiers.into(),
        desc: RelationDesc::builder()
            .with_column("a", SqlScalarType::String.nullable(false))
            .finish(),
        column_comments: BTreeMap::new(),
        is_retained_metrics_object: false,
        access: Vec::new(),
        ontology: None,
    };
    let builtin = leak(Builtin::Source(leak(builtin)));

    (object, builtin)
}

fn evolve_builtin_desc(builtin: &Builtin<NameReference>) -> &'static Builtin<NameReference> {
    let evolve = |desc: RelationDesc| {
        let name = format!("c{}", desc.len());
        let column = RelationDesc::builder()
            .with_column(name, SqlScalarType::Int64.nullable(true))
            .finish();
        desc.concat(column)
    };

    match builtin {
        Builtin::Table(t) => {
            let new = BuiltinTable {
                desc: evolve(t.desc.clone()),
                ..(*t).clone()
            };
            leak(Builtin::Table(leak(new)))
        }
        Builtin::Source(s) => {
            let new = BuiltinSource {
                desc: evolve(s.desc.clone()),
                ..(*s).clone()
            };
            leak(Builtin::Source(leak(new)))
        }
        _ => unimplemented!(),
    }
}

/// Configure tracing for turmoil tests.
///
/// Log events are written to stdout and include the logical time of the simulation.
fn configure_tracing_for_turmoil() {
    use std::sync::Once;
    use tracing::level_filters::LevelFilter;
    use tracing_subscriber::fmt::time::FormatTime;

    #[derive(Clone)]
    struct SimElapsedTime;

    impl FormatTime for SimElapsedTime {
        fn format_time(
            &self,
            w: &mut tracing_subscriber::fmt::format::Writer<'_>,
        ) -> std::fmt::Result {
            tracing_subscriber::fmt::time().format_time(w)?;
            if let Some(sim_elapsed) = turmoil::sim_elapsed() {
                write!(w, " [{:?}]", sim_elapsed)?;
            }
            Ok(())
        }
    }

    static INIT_TRACING: Once = Once::new();
    INIT_TRACING.call_once(|| {
        let env_filter = tracing_subscriber::EnvFilter::builder()
            .with_default_directive(LevelFilter::INFO.into())
            .from_env_lossy();
        let subscriber = tracing_subscriber::fmt()
            .with_test_writer()
            .with_env_filter(env_filter)
            .with_timer(SimElapsedTime)
            .finish();

        // Another test in this binary may have installed a global subscriber
        // already: `#[mz_ore::test]` does so through `mz_ore::test::init_logging`.
        // `cargo test` runs the module's tests in one process, so unwrapping
        // here would panic and poison `INIT_TRACING` for every turmoil test
        // after it. The installed subscriber also writes to the test writer,
        // so nothing is lost by keeping it.
        if tracing::subscriber::set_global_default(subscriber).is_err() {
            info!("a global tracing subscriber is already installed; keeping it");
        }
    });
}

/// Start simulated persist consensus and blob processes.
fn init_persist(sim: &mut turmoil::Sim) -> PersistLocation {
    use mz_persist::turmoil::*;

    sim.host("consensus", {
        let state = ConsensusState::new();
        move || serve_consensus(7000, state.clone())
    });
    sim.host("blob", {
        let state = BlobState::new();
        move || serve_blob(7000, state.clone())
    });

    PersistLocation {
        blob_uri: "turmoil://blob:7000".parse().unwrap(),
        consensus_uri: "turmoil://consensus:7000".parse().unwrap(),
    }
}

/// A step naming a builtin that no longer exists, or that changed
/// `CatalogItemType`, panics `validate_migration_steps` at catalog open. That
/// needs a catalog already on disk to reproduce, so the default CI suite never
/// sees it and only the upgrade nightly does. Much cheaper to catch here.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn test_migration_steps_resolve_to_builtins() {
    for step in MIGRATIONS.iter() {
        assert!(
            BUILTIN_LOOKUP.contains_key(&step.object),
            "migration step for non-existent builtin: {:?}",
            step.object
        );
    }
}
