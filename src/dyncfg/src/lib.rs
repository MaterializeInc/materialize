// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Dynamically updatable configuration.
//!
//! Basic usage:
//! - A type-safe static `Config` is defined near where it is used.
//! - Once in the lifetime of a process, all interesting `Config`s are
//!   registered to a `ConfigSet`. The values within a `ConfigSet` are shared,
//!   though multiple `ConfigSet`s may be created and each are completely
//!   independent (i.e. one in each unit test).
//! - A `ConfigSet` is plumbed around as necessary and may be used to get or
//!   set the value of `Config`.
//!
//! ```
//! # use mz_dyncfg::{Config, ConfigSet};
//! const FOO: Config<bool> = Config::new("foo", false, "description of foo");
//! fn bar(cfg: &ConfigSet) {
//!     assert_eq!(FOO.get(&cfg), false);
//! }
//! fn main() {
//!     let cfg = ConfigSet::default().add(&FOO);
//!     bar(&cfg);
//! }
//! ```
//!
//! # Design considerations for this library
//!
//! - The primary motivation is minimal boilerplate. Runtime dynamic
//!   configuration is one of the most powerful tools we have to quickly react
//!   to incidents, etc in production. Adding and using them should be easy
//!   enough that engineers feel empowered to use them generously.
//!
//!   The theoretical minimum boilerplate is 1) declare a config and 2) use a
//!   config to get/set the value. These could be combined into one step if (2)
//!   were based on global state, but that doesn't play well with testing. So
//!   instead we accomplish (2) by constructing a shared bag of config values in
//!   each `fn main`, amortizing the cost by plumbing it once to each component
//!   (not once per config).
//! - Config definitions are kept next to the usage. The common case is that a
//!   config is used in only one place and this makes it easy to see the
//!   associated documentation at the usage site. Configs that are used in
//!   multiple places may be defined in some common place as appropriate.
//! - Everything is type-safe.
//! - Secondarily: set up the ability to get and use the latest values of
//!   configs in tooling like `persistcli` and `stash debug`. As we've embraced
//!   dynamic configuration, we've ended up in a situation where it's stressful
//!   to run the read-write `persistcli admin` tooling with the defaults
//!   compiled into code, but `persistcli` doesn't have access to the vars stuff
//!   and doesn't want to instantiate a catalog impl.

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use ref_cast::{ref_cast_custom, RefCastCustom};
use tokio::sync::Notify;
use tracing::error;

use mz_proto::{ProtoType, RustType};

include!(concat!(env!("OUT_DIR"), "/mz_dyncfg.rs"));

/// A handle to a dynamically updatable configuration value.
///
/// This represents a strongly-typed named config of type `T`. It may be
/// registered to a set of such configs with [ConfigSet::add] and then later
/// used to retrieve the latest value at any time with [Self::get].
///
/// The supported types are [bool], [usize], [Duration], and [String], as well as [Option]
/// variants of these as necessary.
#[derive(Clone, Debug)]
pub struct Config<T: ConfigType> {
    name: &'static str,
    desc: &'static str,
    default: T::Default,
}

impl<T: ConfigType> Config<T> {
    /// Constructs a handle for a config of type `T`.
    ///
    /// It is best practice, but not strictly required, for the name to be
    /// globally unique within a process.
    ///
    /// TODO(cfg): Add some sort of categorization of config purpose here: e.g.
    /// limited-lifetime rollout flag, CYA, magic number that we never expect to
    /// tune, magic number that we DO expect to tune, etc. This could be used to
    /// power something like a `--future-default-flags` for CI, to replace part
    /// or all of the manually maintained list.
    ///
    /// TODO(cfg): See if we can make this more Rust-y and take these params as
    /// a struct (the obvious thing hits some issues with const combined with
    /// Drop).
    pub const fn new(name: &'static str, default: T::Default, desc: &'static str) -> Self {
        Config {
            name,
            default,
            desc,
        }
    }

    /// The name of this config.
    pub fn name(&self) -> &str {
        self.name
    }

    /// The description of this config.
    pub fn desc(&self) -> &str {
        self.desc
    }

    /// The default value of this config.
    pub fn default(&self) -> &T::Default {
        &self.default
    }

    /// Returns the latest value of this config within the given set.
    ///
    /// Panics if this config was not previously registered to the set.
    ///
    /// TODO(cfg): Decide if this should be a method on `ConfigSet` instead to
    /// match the precedent of `BTreeMap/HashMap::get` taking a key. It's like
    /// this initially because it was thought that the `Config` definition was
    /// the more important "noun" and also that rustfmt would maybe work better
    /// on this ordering.
    pub fn get(&self, set: &ConfigSet) -> T {
        self.handle(set).get()
    }

    /// Returns the shared value of this config in the given set.
    ///
    /// This allows users to amortize the name lookup with
    /// `Self::get_from_shared`.
    pub fn handle<'a>(&self, set: &'a ConfigSet) -> &'a ConfigValHandle<T> {
        set.configs
            .get(self.name)
            .expect("config should be registered to set")
            .handle()
    }
}

/// A type usable as a [Config].
pub trait ConfigType: Into<ConfigVal> + Clone + Sized {
    /// A const-compatible type suitable for use as the default value of configs
    /// of this type.
    type Default: Into<Self> + Clone;

    /// Converts a type-erased enum value to this type.
    ///
    /// Panics if the enum's variant does not match this type.
    fn from_val(val: ConfigVal) -> Self;
}

/// An set of [Config]s with values independent of other [ConfigSet]s (even if
/// they contain the same configs).
#[derive(Clone, Default)]
pub struct ConfigSet {
    configs: BTreeMap<String, ConfigEntry>,
    /// Synchronization to allow updates to multiple configuration values to be
    /// observed atomically.
    ///
    /// The `Mutex` is held when applying a batch of updates and when running
    /// `with_updates_frozen` and `wait_for` closures. This ensures that these
    /// closures never see a partial batch of updates.
    ///
    /// The `Notify` is signaled when a new batch of updates is applied.
    /// Signaling the `Notify` and registering for notifications must be
    /// performed while holding the `Mutex`.
    ///
    /// `configs` is not stored in the `Mutex` since the common case is
    /// observing configuration values without taking the lock. `Notify` is not
    /// stored in the `Mutex` to avoid lifetime issues.
    update_sync: Arc<(Notify, Mutex<()>)>,
}

impl ConfigSet {
    /// Adds the given config to this set.
    pub fn add<T>(mut self, config: &Config<T>) -> Self
    where
        T: ConfigType,
    {
        let default = Into::<T>::into(config.default.clone());
        let default = Into::<ConfigVal>::into(default);
        let config = ConfigEntry {
            name: config.name,
            desc: config.desc,
            default: default.clone(),
            val: ConfigValAtomic::from(default),
        };
        if let Some(prev) = self.configs.insert(config.name.to_owned(), config) {
            panic!("{} registered twice", prev.name);
        }
        self
    }

    /// Returns the configs currently registered to this set.
    pub fn entries(&self) -> impl Iterator<Item = &ConfigEntry> {
        self.configs.values()
    }

    /// Executes `f` with updates to configuration values frozen.
    ///
    /// This allows `f` to observe multiple configuration values atomically.
    /// Specifically, updates from [`ConfigUpdates`] will never be partially
    /// applied when `f` runs: either all updates from the batch will be applied
    /// or none of them will.
    ///
    /// `f` should take care to execute quickly, as no further configuration
    /// updates can be applied while `f` is executing.
    pub fn with_updates_frozen<F>(&self, mut f: F)
    where
        F: FnMut(),
    {
        let (_notify, lock) = &*self.update_sync;

        // Hold the lock while `f` executes to ensure no configuration updates
        // are applied.
        let _guard = lock.lock().expect("lock poisoned");
        f()
    }

    /// Waits for a condition to become true, checking the condition after every
    /// configuration update.
    ///
    /// The closure `f` is invoked once initially, then again periodically after
    /// configuration updates occur, until the closure returns `true`. The
    /// closure is not guaranteed to be invoked exactly once after every
    /// configuration update.
    ///
    /// Similarly to [`Config::with_updates_frozen`], the closure `f` executes
    /// with configuration updates frozen.
    pub async fn wait_for<F>(&self, mut f: F)
    where
        F: FnMut() -> bool,
    {
        loop {
            let (notify, lock) = &*self.update_sync;
            let notified = {
                // Hold the lock while `f` executes to ensure no configuration
                // updates are applied.
                let _guard = lock.lock().expect("lock poisoned");
                if f() {
                    // `f` indicated the condition was met. Exit.
                    return;
                }

                // Register for notifications while holding the lock to ensure
                // we don't miss a notification.
                notify.notified()
            };

            // `f` indicated the condition was not met. Wait until new
            // configuration values come in, then try again with another turn of
            // the loop.
            notified.await;
        }
    }
}

/// An entry for a config in a [ConfigSet].
#[derive(Clone, Debug)]
pub struct ConfigEntry {
    name: &'static str,
    desc: &'static str,
    default: ConfigVal,
    val: ConfigValAtomic,
}

impl ConfigEntry {
    /// The name of this config.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// The description of this config.
    pub fn desc(&self) -> &'static str {
        self.desc
    }

    /// The default value of this config.
    ///
    /// This value is never updated.
    pub fn default(&self) -> &ConfigVal {
        &self.default
    }

    /// The value of this config in the set.
    pub fn val(&self) -> ConfigVal {
        self.val.load()
    }

    /// Constructs a handle to the value of this config in the set.
    ///
    /// It is the caller's responsibility to construct the handle with the
    /// correct type.
    fn handle<T>(&self) -> &ConfigValHandle<T>
    where
        T: ConfigType,
    {
        ConfigValHandle::<T>::new(&self.val)
    }
}

/// A handle to a configuration value in a [`ConfigSet`].
///
/// Allows users to amortize the lookup of a name within a set.
///
/// Handles can be cheaply cloned.
#[derive(Debug, Clone, RefCastCustom)]
#[repr(transparent)]
pub struct ConfigValHandle<T> {
    val: ConfigValAtomic,
    _type: PhantomData<T>,
}

impl<T> ConfigValHandle<T>
where
    T: ConfigType,
{
    #[ref_cast_custom]
    fn new(val: &ConfigValAtomic) -> &ConfigValHandle<T>;

    /// Returns the latest value of this config within the set associated with
    /// the handle.
    pub fn get(&self) -> T {
        T::from_val(self.val.load())
    }
}

/// A type-erased configuration value for when set of different types are stored
/// in a collection.
#[derive(Clone, Debug)]
pub enum ConfigVal {
    /// A `bool` value.
    Bool(bool),
    /// A `u32` value.
    U32(u32),
    /// A `usize` value.
    Usize(usize),
    /// An `Option<usize>` value.
    OptUsize(Option<usize>),
    /// A `String` value.
    String(String),
    /// A `Duration` value.
    Duration(Duration),
}

/// An atomic version of [`ConfigVal`] to allow configuration values to be
/// shared between configuration writers and readers.
// TODO(cfg): Consider moving these Arcs to be a single one around the map in
// `ConfigSet` instead. That would mean less pointer-chasing in the common
// case, but would remove the possibility of amortizing the name lookup via
// [Config::handle].
#[derive(Clone, Debug)]
enum ConfigValAtomic {
    Bool(Arc<AtomicBool>),
    U32(Arc<AtomicU32>),
    Usize(Arc<AtomicUsize>),
    OptUsize(Arc<RwLock<Option<usize>>>),
    String(Arc<RwLock<String>>),
    Duration(Arc<RwLock<Duration>>),
}

impl From<ConfigVal> for ConfigValAtomic {
    fn from(val: ConfigVal) -> ConfigValAtomic {
        match val {
            ConfigVal::Bool(x) => ConfigValAtomic::Bool(Arc::new(AtomicBool::new(x))),
            ConfigVal::U32(x) => ConfigValAtomic::U32(Arc::new(AtomicU32::new(x))),
            ConfigVal::Usize(x) => ConfigValAtomic::Usize(Arc::new(AtomicUsize::new(x))),
            ConfigVal::OptUsize(x) => ConfigValAtomic::OptUsize(Arc::new(RwLock::new(x))),
            ConfigVal::String(x) => ConfigValAtomic::String(Arc::new(RwLock::new(x))),
            ConfigVal::Duration(x) => ConfigValAtomic::Duration(Arc::new(RwLock::new(x))),
        }
    }
}

impl ConfigValAtomic {
    fn load(&self) -> ConfigVal {
        match self {
            ConfigValAtomic::Bool(x) => ConfigVal::Bool(x.load(SeqCst)),
            ConfigValAtomic::U32(x) => ConfigVal::U32(x.load(SeqCst)),
            ConfigValAtomic::Usize(x) => ConfigVal::Usize(x.load(SeqCst)),
            ConfigValAtomic::OptUsize(x) => ConfigVal::OptUsize(*x.read().expect("lock poisoned")),
            ConfigValAtomic::String(x) => {
                ConfigVal::String(x.read().expect("lock poisoned").clone())
            }
            ConfigValAtomic::Duration(x) => ConfigVal::Duration(*x.read().expect("lock poisoned")),
        }
    }

    fn store(&self, val: ConfigVal) {
        match (self, val) {
            (ConfigValAtomic::Bool(x), ConfigVal::Bool(val)) => x.store(val, SeqCst),
            (ConfigValAtomic::U32(x), ConfigVal::U32(val)) => x.store(val, SeqCst),
            (ConfigValAtomic::Usize(x), ConfigVal::Usize(val)) => x.store(val, SeqCst),
            (ConfigValAtomic::OptUsize(x), ConfigVal::OptUsize(val)) => {
                *x.write().expect("lock poisoned") = val
            }
            (ConfigValAtomic::String(x), ConfigVal::String(val)) => {
                *x.write().expect("lock poisoned") = val
            }
            (ConfigValAtomic::Duration(x), ConfigVal::Duration(val)) => {
                *x.write().expect("lock poisoned") = val
            }
            (_, val) => panic!("attempted to store {val:?} value in {self:?} parameter"),
        }
    }
}

impl ConfigUpdates {
    /// Adds an update for the given config and value.
    ///
    /// If a value of the same config has previously been added to these
    /// updates, replaces it.
    pub fn add<T>(&mut self, config: &Config<T>, val: T)
    where
        T: ConfigType,
    {
        self.updates.push(ProtoConfigVal {
            name: config.name.to_owned(),
            val: val.into().into_proto(),
        });
    }

    /// Adds an update for the given configuration name and value.
    ///
    /// It is the callers responsibility to ensure the value is of the
    /// appropriate type for the configuration.
    ///
    /// If a value of the same config has previously been added to these
    /// updates, replaces it.
    pub fn add_dynamic<T>(&mut self, name: &'static str, val: T)
    where
        T: Into<ConfigVal>,
    {
        self.updates.push(ProtoConfigVal {
            name: name.to_owned(),
            val: val.into().into_proto(),
        });
    }

    /// Adds the entries in `other` to `self`, with `other` taking precedence.
    pub fn extend(&mut self, mut other: Self) {
        self.updates.append(&mut other.updates)
    }

    /// Applies these config updates to the given [ConfigSet].
    ///
    /// This doesn't need to be the same set that the value updates were added
    /// from. In fact, the primary use of this is propagating config updates
    /// across processes.
    ///
    /// The value updates for any configs unknown by the given set are skipped.
    /// Ditto for config type mismatches. However, this is unexpected usage at
    /// present and so is logged to Sentry.
    pub fn apply(&self, set: &ConfigSet) {
        let (notify, lock) = &*set.update_sync;

        // Hold the lock while applying updates to prevent `with_updates_frozen`
        // and `wait_for` from running their closures.
        let _guard = lock.lock().expect("lock poisoned");

        // Signal waiters of the configuration update. We need to do this while
        // holding the lock to avoid lost wakeups. We can signal before we apply
        // the updates since the waiters won't be able to observe the updates
        // until we release the lock.
        notify.notify_waiters();

        for ProtoConfigVal { name, val } in self.updates.iter() {
            let Some(config) = set.configs.get(name) else {
                error!("config update {} {:?} not known set: {:?}", name, val, set);
                continue;
            };
            let val = match (val.clone()).into_rust() {
                Ok(x) => x,
                Err(err) => {
                    error!("config update {} decode error: {}", name, err);
                    continue;
                }
            };
            config.val.store(val);
        }
    }
}

mod impls {
    use std::time::Duration;

    use mz_ore::cast::CastFrom;
    use mz_proto::{ProtoType, RustType, TryFromProtoError};

    use crate::{proto_config_val, ConfigSet, ConfigType, ConfigVal, ProtoOptionU64};

    impl ConfigType for bool {
        type Default = bool;

        fn from_val(val: ConfigVal) -> Self {
            match val {
                ConfigVal::Bool(x) => x,
                x => panic!("expected bool value got {:?}", x),
            }
        }
    }

    impl From<bool> for ConfigVal {
        fn from(val: bool) -> ConfigVal {
            ConfigVal::Bool(val)
        }
    }

    impl ConfigType for u32 {
        type Default = u32;

        fn from_val(val: ConfigVal) -> Self {
            match val {
                ConfigVal::U32(x) => x,
                x => panic!("expected u32 value got {:?}", x),
            }
        }
    }

    impl From<u32> for ConfigVal {
        fn from(val: u32) -> ConfigVal {
            ConfigVal::U32(val)
        }
    }

    impl ConfigType for usize {
        type Default = usize;

        fn from_val(val: ConfigVal) -> Self {
            match val {
                ConfigVal::Usize(x) => x,
                x => panic!("expected usize value got {:?}", x),
            }
        }
    }

    impl From<usize> for ConfigVal {
        fn from(val: usize) -> ConfigVal {
            ConfigVal::Usize(val)
        }
    }

    impl ConfigType for Option<usize> {
        type Default = Option<usize>;

        fn from_val(val: ConfigVal) -> Self {
            match val {
                ConfigVal::OptUsize(x) => x,
                x => panic!("expected usize value got {:?}", x),
            }
        }
    }

    impl From<Option<usize>> for ConfigVal {
        fn from(val: Option<usize>) -> ConfigVal {
            ConfigVal::OptUsize(val)
        }
    }

    impl ConfigType for String {
        type Default = &'static str;

        fn from_val(val: ConfigVal) -> Self {
            match val {
                ConfigVal::String(x) => x,
                x => panic!("expected String value got {:?}", x),
            }
        }
    }

    impl From<String> for ConfigVal {
        fn from(val: String) -> ConfigVal {
            ConfigVal::String(val)
        }
    }

    impl ConfigType for Duration {
        type Default = Duration;

        fn from_val(val: ConfigVal) -> Self {
            match val {
                ConfigVal::Duration(x) => x,
                x => panic!("expected Duration value got {:?}", x),
            }
        }
    }

    impl From<Duration> for ConfigVal {
        fn from(val: Duration) -> ConfigVal {
            ConfigVal::Duration(val)
        }
    }

    impl RustType<Option<proto_config_val::Val>> for ConfigVal {
        fn into_proto(&self) -> Option<proto_config_val::Val> {
            use crate::proto_config_val::Val;
            let val = match self {
                ConfigVal::Bool(x) => Val::Bool(*x),
                ConfigVal::U32(x) => Val::U32(*x),
                ConfigVal::Usize(x) => Val::Usize(u64::cast_from(*x)),
                ConfigVal::OptUsize(x) => Val::OptUsize(ProtoOptionU64 {
                    val: x.map(u64::cast_from),
                }),
                ConfigVal::String(x) => Val::String(x.into_proto()),
                ConfigVal::Duration(x) => Val::Duration(x.into_proto()),
            };
            Some(val)
        }

        fn from_proto(proto: Option<proto_config_val::Val>) -> Result<Self, TryFromProtoError> {
            let val = match proto {
                Some(proto_config_val::Val::Bool(x)) => ConfigVal::Bool(x),
                Some(proto_config_val::Val::U32(x)) => ConfigVal::U32(x),
                Some(proto_config_val::Val::Usize(x)) => ConfigVal::Usize(usize::cast_from(x)),
                Some(proto_config_val::Val::OptUsize(ProtoOptionU64 { val })) => {
                    ConfigVal::OptUsize(val.map(usize::cast_from))
                }
                Some(proto_config_val::Val::String(x)) => ConfigVal::String(x),
                Some(proto_config_val::Val::Duration(x)) => ConfigVal::Duration(x.into_rust()?),
                None => {
                    return Err(TryFromProtoError::unknown_enum_variant(
                        "ProtoConfigVal::Val",
                    ))
                }
            };
            Ok(val)
        }
    }

    impl std::fmt::Debug for ConfigSet {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let ConfigSet {
                configs,
                update_sync: _,
            } = self;
            f.debug_map()
                .entries(configs.iter().map(|(name, val)| (name, val.val())))
                .finish()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use futures::FutureExt;

    use super::*;

    const BOOL: Config<bool> = Config::new("bool", true, "");
    const USIZE: Config<usize> = Config::new("usize", 1, "");
    const OPT_USIZE: Config<Option<usize>> = Config::new("opt_usize", Some(2), "");
    const STRING: Config<String> = Config::new("string", "a", "");
    const DURATION: Config<Duration> = Config::new("duration", Duration::from_nanos(3), "");

    #[mz_ore::test]
    fn all_types() {
        let configs = ConfigSet::default()
            .add(&BOOL)
            .add(&USIZE)
            .add(&OPT_USIZE)
            .add(&STRING)
            .add(&DURATION);
        assert_eq!(BOOL.get(&configs), true);
        assert_eq!(USIZE.get(&configs), 1);
        assert_eq!(OPT_USIZE.get(&configs), Some(2));
        assert_eq!(STRING.get(&configs), "a");
        assert_eq!(DURATION.get(&configs), Duration::from_nanos(3));

        let mut updates = ConfigUpdates::default();
        updates.add(&BOOL, false);
        updates.add(&USIZE, 2);
        updates.add(&OPT_USIZE, None);
        updates.add(&STRING, "b".to_owned());
        updates.add(&DURATION, Duration::from_nanos(4));
        updates.apply(&configs);

        assert_eq!(BOOL.get(&configs), false);
        assert_eq!(USIZE.get(&configs), 2);
        assert_eq!(OPT_USIZE.get(&configs), None);
        assert_eq!(STRING.get(&configs), "b");
        assert_eq!(DURATION.get(&configs), Duration::from_nanos(4));
    }

    #[mz_ore::test]
    fn config_set() {
        let c0 = ConfigSet::default().add(&USIZE);
        assert_eq!(USIZE.get(&c0), 1);
        let mut updates = ConfigUpdates::default();
        updates.add(&USIZE, 2);
        updates.apply(&c0);
        assert_eq!(USIZE.get(&c0), 2);

        // Each ConfigSet is independent, even if they contain the same set of
        // configs.
        let c1 = ConfigSet::default().add(&USIZE);
        assert_eq!(USIZE.get(&c1), 1);
        let mut updates = ConfigUpdates::default();
        updates.add(&USIZE, 3);
        updates.apply(&c1);
        assert_eq!(USIZE.get(&c1), 3);
        assert_eq!(USIZE.get(&c0), 2);

        // We can copy values from one to the other, though (envd -> clusterd).
        let mut updates = ConfigUpdates::default();
        for e in c0.entries() {
            updates.add_dynamic(e.name, e.val());
        }
        assert_eq!(USIZE.get(&c1), 3);
        updates.apply(&c1);
        assert_eq!(USIZE.get(&c1), 2);
    }

    #[mz_ore::test(tokio::test)]
    async fn config_set_wait_for() {
        let configs = ConfigSet::default().add(&BOOL).add(&USIZE);

        // Ensure that the default values are what we expect.
        assert_eq!(BOOL.get(&configs), true);
        assert_eq!(USIZE.get(&configs), 1);

        // Ensure that waiting for a default value for `usize` returns immediately.
        let wait_for_1 = configs.wait_for(|| USIZE.get(&configs) == 1);
        assert!(wait_for_1.now_or_never().is_some());

        // Now wait for the value of `usize` to become 3 in a background task.
        let wait_for_3 = mz_ore::task::spawn(|| "config_set_wait_for", {
            let configs = configs.clone();
            async move {
                configs.wait_for(|| USIZE.get(&configs) == 3).await;

                // `usize` was set to 3 in the same update batch as `bool`
                // was set to `false`, so when `wait_for` returns `bool`
                // should also be `false`.
                assert_eq!(BOOL.get(&configs), false);
            }
        })
        .abort_on_drop();
        let mut wait_for_3 = pin!(wait_for_3);

        // The task should not return immediately.
        assert!(wait_for_3.as_mut().now_or_never().is_none());

        // Nor after 100ms.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(wait_for_3.as_mut().now_or_never().is_none());

        // The task should not return after the value is updated to 2.
        let mut updates = ConfigUpdates::default();
        updates.add(&USIZE, 2);
        updates.apply(&configs);
        assert!(wait_for_3.as_mut().now_or_never().is_none());

        // The task should return shortly after the value is updated to 3.
        let mut updates = ConfigUpdates::default();
        updates.add(&BOOL, false);
        updates.add(&USIZE, 3);
        updates.apply(&configs);
        tokio::time::timeout(Duration::from_secs(5), wait_for_3)
            .await
            .unwrap()
            .unwrap();
    }
}
