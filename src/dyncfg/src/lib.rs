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
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tracing::error;

use mz_proto::{ProtoType, RustType};

include!(concat!(env!("OUT_DIR"), "/mz_dyncfg.rs"));

/// A handle to a dynamically updatable configuration value.
///
/// This represents a strongly-typed named config of type `T`. It may be
/// registered to a set of such configs with [ConfigSet::add] and then later
/// used to retrieve the latest value at any time with [Self::get].
///
/// The supported types are [bool], [usize], and [String].
///
/// TODO(cfg): Add Duration in a followup.
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

    /// Adds this config to the given set.
    ///
    /// Names are required to be unique within a set, but each set is entirely
    /// independent. The same `Config` may be registered to multiple
    /// [ConfigSet]s and thus have independent values (e.g. imagine a units test
    /// executing concurrently in the same process).
    ///
    /// Panics if a config with the same name has previously been registered to
    /// this set.
    pub fn register(&self, set: &mut ConfigSet) -> T {
        T::get(T::shared(self, set).expect("config should be registered to set"))
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
        T::get(T::shared(self, set).expect("config should be registered to set"))
    }

    /// Returns the shared value of this config in the given set.
    ///
    /// This allows users to amortize the name lookup with
    /// `Self::get_from_shared`.
    pub fn shared(&self, set: &ConfigSet) -> Arc<T::Shared> {
        Arc::clone(T::shared(self, set).expect("config should be registered to set"))
    }

    /// [Self::get] except from a previously looked up shared value returned by
    /// [Self::shared].
    pub fn get_from_shared(&self, shared: &T::Shared) -> T {
        T::get(shared)
    }
}

/// A type usable as a [Config].
pub trait ConfigType: Sized {
    /// A const-compatible type suitable for use as the default value of configs
    /// of this type.
    type Default: Into<Self> + Clone;
    /// A value of this type, sharable between config value updaters (like
    /// LaunchDarkly) and config value retrievers.
    type Shared;

    /// Extracts the sharable value for a config of this type from a set.
    ///
    /// External users likely want [Config::shared] instead.
    fn shared<'a>(config: &Config<Self>, vals: &'a ConfigSet) -> Option<&'a Arc<Self::Shared>>;

    /// Converts this type to its type-erased enum equivalent.
    fn to_val(val: &Self) -> ConfigVal;

    /// Retrieves the current config value of this type from a value of its
    /// corresponding sharable type.
    ///
    /// External users likely want [Config::get] or [Config::get_from_shared]
    /// instead.
    fn get(x: &Self::Shared) -> Self;

    /// Updates the sharable value for a config of this type to the given value.
    fn set(x: &Self::Shared, val: Self);
}

/// An set of [Config]s with values independent of other [ConfigSet]s (even if
/// they contain the same configs).
#[derive(Clone, Default)]
pub struct ConfigSet {
    configs: BTreeMap<String, ConfigEntry>,
}

impl ConfigSet {
    /// Adds the given config to this set.
    ///
    /// An alias for [Config::register], but taking and returning `Self` to
    /// allow for easy chaining.
    pub fn add<T: ConfigType>(mut self, config: &Config<T>) -> Self {
        let config = ConfigEntry {
            name: config.name,
            desc: config.desc,
            default: T::to_val(&Into::<T>::into(config.default.clone())),
            val: T::to_val(&Into::<T>::into(config.default.clone())),
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
}

/// An entry for a config in a [ConfigSet].
#[derive(Clone, Debug)]
pub struct ConfigEntry {
    name: &'static str,
    desc: &'static str,
    default: ConfigVal,
    val: ConfigVal,
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

    /// The sharable value of this config in the set.
    pub fn val(&self) -> &ConfigVal {
        &self.val
    }
}

/// A type-erased [ConfigType::Shared] for when set of different types are
/// stored in a collection.
///
/// TODO(cfg): Consider moving these Arcs to be a single one around the map in
/// `ConfigSet` instead. That would mean less pointer-chasing in the common
/// case, but would remove the possibility of amortizing the name lookup via
/// [Config::get_from_shared].
#[derive(Clone, Debug)]
pub enum ConfigVal {
    /// A `bool` shared value.
    Bool(Arc<AtomicBool>),
    /// A `u32` shared value.
    U32(Arc<AtomicU32>),
    /// A `usize` shared value.
    Usize(Arc<AtomicU64>),
    /// A `String` shared value.
    String(Arc<RwLock<String>>),
    /// A 'Duration' shared value.
    Duration(Arc<RwLock<Duration>>),
}

impl ConfigUpdates {
    /// Adds the current value of the given config to this set of updates.
    ///
    /// If a value of the same config has previously been added to these
    /// updates, replaces it.
    pub fn add(&mut self, config: &ConfigEntry) {
        self.updates.push(ProtoConfigVal {
            name: config.name.to_owned(),
            val: config.val.into_proto(),
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
        fn copy<T: ConfigType>(src: &T::Shared, dst: &T::Shared) {
            T::set(dst, T::get(src))
        }

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
            // TODO(cfg): Restructure this match so that it's harder to miss
            // when adding a new `ConfigType`.
            match (&val, &config.val) {
                (ConfigVal::Bool(src), ConfigVal::Bool(dst)) => copy::<bool>(src, dst),
                (ConfigVal::U32(src), ConfigVal::U32(dst)) => copy::<u32>(src, dst),
                (ConfigVal::Usize(src), ConfigVal::Usize(dst)) => copy::<usize>(src, dst),
                (ConfigVal::String(src), ConfigVal::String(dst)) => copy::<String>(src, dst),
                (ConfigVal::Duration(src), ConfigVal::Duration(dst)) => copy::<Duration>(src, dst),
                (src, dst) => error!(
                    "config update {} type mismatch: {:?} vs {:?}",
                    name, src, dst
                ),
            }
        }
    }
}

mod impls {
    use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering::SeqCst};
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    use mz_ore::cast::CastFrom;
    use mz_proto::{ProtoType, RustType, TryFromProtoError};

    use crate::{proto_config_val, Config, ConfigSet, ConfigType, ConfigVal};

    impl ConfigType for bool {
        type Default = bool;
        type Shared = AtomicBool;

        fn shared<'a>(config: &Config<Self>, vals: &'a ConfigSet) -> Option<&'a Arc<Self::Shared>> {
            let entry = vals.configs.get(config.name)?;
            match entry.val() {
                ConfigVal::Bool(x) => Some(x),
                x => panic!("expected bool value got {:?}", x),
            }
        }
        fn to_val(val: &Self) -> ConfigVal {
            ConfigVal::Bool(Arc::new((*val).into()))
        }
        fn set(x: &Self::Shared, val: Self) {
            x.store(val, SeqCst);
        }
        fn get(x: &Self::Shared) -> Self {
            x.load(SeqCst)
        }
    }

    impl ConfigType for u32 {
        type Default = u32;
        type Shared = AtomicU32;

        fn shared<'a>(config: &Config<Self>, vals: &'a ConfigSet) -> Option<&'a Arc<Self::Shared>> {
            let entry = vals.configs.get(config.name)?;
            match entry.val() {
                ConfigVal::U32(x) => Some(x),
                x => panic!("expected u32 value got {:?}", x),
            }
        }
        fn to_val(val: &Self) -> ConfigVal {
            ConfigVal::U32(Arc::new((*val).into()))
        }
        fn set(x: &Self::Shared, val: Self) {
            x.store(val, SeqCst);
        }
        fn get(x: &Self::Shared) -> Self {
            x.load(SeqCst)
        }
    }

    impl ConfigType for usize {
        type Default = usize;
        type Shared = AtomicU64;

        fn shared<'a>(config: &Config<Self>, vals: &'a ConfigSet) -> Option<&'a Arc<Self::Shared>> {
            let entry = vals.configs.get(config.name)?;
            match entry.val() {
                ConfigVal::Usize(x) => Some(x),
                x => panic!("expected usize value got {:?}", x),
            }
        }
        fn to_val(val: &Self) -> ConfigVal {
            ConfigVal::Usize(Arc::new(u64::cast_from(*val).into()))
        }
        fn set(x: &Self::Shared, val: Self) {
            x.store(u64::cast_from(val), SeqCst);
        }
        fn get(x: &Self::Shared) -> Self {
            usize::cast_from(x.load(SeqCst))
        }
    }

    impl ConfigType for String {
        type Default = &'static str;
        type Shared = RwLock<String>;

        fn shared<'a>(config: &Config<Self>, vals: &'a ConfigSet) -> Option<&'a Arc<Self::Shared>> {
            let entry = vals.configs.get(config.name)?;
            match entry.val() {
                ConfigVal::String(x) => Some(x),
                x => panic!("expected String value got {:?}", x),
            }
        }
        fn to_val(val: &Self) -> ConfigVal {
            ConfigVal::String(Arc::new(RwLock::new(val.clone())))
        }
        fn set(x: &Self::Shared, val: Self) {
            *x.write().expect("lock poisoned") = val;
        }
        fn get(x: &Self::Shared) -> Self {
            x.read().expect("lock poisoned").clone()
        }
    }

    impl ConfigType for Duration {
        type Default = Duration;
        type Shared = RwLock<Duration>;

        fn shared<'a>(config: &Config<Self>, vals: &'a ConfigSet) -> Option<&'a Arc<Self::Shared>> {
            let entry = vals.configs.get(config.name)?;
            match entry.val() {
                ConfigVal::Duration(x) => Some(x),
                x => panic!("expected Duration value got {:?}", x),
            }
        }
        fn to_val(val: &Self) -> ConfigVal {
            ConfigVal::Duration(Arc::new(RwLock::new(val.clone())))
        }
        fn set(x: &Self::Shared, val: Self) {
            *x.write().expect("lock poisoned") = val;
        }
        fn get(x: &Self::Shared) -> Self {
            x.read().expect("lock poisoned").clone()
        }
    }

    impl RustType<Option<proto_config_val::Val>> for ConfigVal {
        fn into_proto(&self) -> Option<proto_config_val::Val> {
            use crate::proto_config_val::Val;
            let val = match self {
                ConfigVal::Bool(x) => Val::Bool(bool::get(x)),
                ConfigVal::U32(x) => Val::U32(u32::get(x)),
                ConfigVal::Usize(x) => Val::Usize(u64::cast_from(usize::get(x))),
                ConfigVal::String(x) => Val::String(String::get(x)),
                ConfigVal::Duration(x) => Val::Duration(Duration::get(x).into_proto()),
            };
            Some(val)
        }

        fn from_proto(proto: Option<proto_config_val::Val>) -> Result<Self, TryFromProtoError> {
            let val = match proto {
                Some(proto_config_val::Val::Bool(x)) => ConfigVal::Bool(Arc::new(x.into())),
                Some(proto_config_val::Val::U32(x)) => ConfigVal::U32(Arc::new(x.into())),
                Some(proto_config_val::Val::Usize(x)) => ConfigVal::Usize(Arc::new(x.into())),
                Some(proto_config_val::Val::String(x)) => {
                    ConfigVal::String(Arc::new(x.to_owned().into()))
                }
                Some(proto_config_val::Val::Duration(x)) => {
                    ConfigVal::Duration(Arc::new(RwLock::new(x.to_owned().into_rust()?)))
                }
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
            let ConfigSet { configs } = self;
            f.debug_map()
                .entries(configs.iter().map(|(name, val)| (name, val.val())))
                .finish()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BOOL: Config<bool> = Config::new("bool", true, "");
    const USIZE: Config<usize> = Config::new("usize", 1, "");
    const STRING: Config<String> = Config::new("string", "a", "");

    #[mz_ore::test]
    fn all_types() {
        let configs = ConfigSet::default().add(&BOOL).add(&USIZE).add(&STRING);
        assert_eq!(BOOL.get(&configs), true);
        assert_eq!(USIZE.get(&configs), 1);
        assert_eq!(STRING.get(&configs), "a");

        bool::set(bool::shared(&BOOL, &configs).unwrap(), false);
        usize::set(usize::shared(&USIZE, &configs).unwrap(), 2);
        String::set(String::shared(&STRING, &configs).unwrap(), "b".to_owned());
        assert_eq!(BOOL.get(&configs), false);
        assert_eq!(USIZE.get(&configs), 2);
        assert_eq!(STRING.get(&configs), "b");
    }

    #[mz_ore::test]
    fn config_set() {
        let c0 = ConfigSet::default().add(&USIZE);
        assert_eq!(USIZE.get(&c0), 1);
        usize::set(usize::shared(&USIZE, &c0).unwrap(), 2);
        assert_eq!(USIZE.get(&c0), 2);

        // Each ConfigSet is independent, even if they contain the same set of
        // configs.
        let c1 = ConfigSet::default().add(&USIZE);
        assert_eq!(USIZE.get(&c1), 1);
        usize::set(usize::shared(&USIZE, &c1).unwrap(), 3);
        assert_eq!(USIZE.get(&c1), 3);
        assert_eq!(USIZE.get(&c0), 2);

        // We can copy values from one to the other, though (envd -> clusterd).
        let mut updates = ConfigUpdates::default();
        for e in c0.entries() {
            updates.add(e);
        }
        assert_eq!(USIZE.get(&c1), 3);
        updates.apply(&c1);
        assert_eq!(USIZE.get(&c1), 2);
    }
}
