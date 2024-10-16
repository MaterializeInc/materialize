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
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize};
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
/// The supported types are [bool], [usize], [Duration], and [String], as well as [Option]
/// variants of these as necessary.
#[derive(Clone, Debug)]
pub struct Config<D: ConfigDefault> {
    name: &'static str,
    desc: &'static str,
    default: D,
}

impl<D: ConfigDefault> Config<D> {
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
    pub const fn new(name: &'static str, default: D, desc: &'static str) -> Self {
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
    pub fn default(&self) -> &D {
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
    pub fn get(&self, set: &ConfigSet) -> D::ConfigType {
        D::ConfigType::from_val(self.shared(set).load())
    }

    /// Returns a handle to the value of this config in the given set.
    ///
    /// This allows users to amortize the cost of the name lookup.
    pub fn handle(&self, set: &ConfigSet) -> ConfigValHandle<D::ConfigType> {
        ConfigValHandle {
            val: self.shared(set).clone(),
            _type: PhantomData,
        }
    }

    /// Returns the shared value of this config in the given set.
    fn shared<'a>(&self, set: &'a ConfigSet) -> &'a ConfigValAtomic {
        &set.configs
            .get(self.name)
            .unwrap_or_else(|| panic!("config {} should be registered to set", self.name))
            .val
    }

    /// Parse a string value for this config.
    pub fn parse_val(&self, val: &str) -> Result<ConfigVal, String> {
        let val = D::ConfigType::parse(val)?;
        let val = Into::<ConfigVal>::into(val);
        Ok(val)
    }
}

/// A type usable as a [Config].
pub trait ConfigType: Into<ConfigVal> + Clone + Sized {
    /// Converts a type-erased enum value to this type.
    ///
    /// Panics if the enum's variant does not match this type.
    fn from_val(val: ConfigVal) -> Self;

    /// Parses this string slice into a [`ConfigType`].
    fn parse(s: &str) -> Result<Self, String>;
}

/// A trait for a type that can be used as a default for a [`Config`].
pub trait ConfigDefault: Clone {
    type ConfigType: ConfigType;

    /// Converts into the config type.
    fn into_config_type(self) -> Self::ConfigType;
}

impl<T: ConfigType> ConfigDefault for T {
    type ConfigType = T;

    fn into_config_type(self) -> T {
        self
    }
}

impl<T: ConfigType> ConfigDefault for fn() -> T {
    type ConfigType = T;

    fn into_config_type(self) -> T {
        (self)()
    }
}

/// An set of [Config]s with values that may or may not be independent of other
/// [ConfigSet]s.
///
/// When constructing a ConfigSet from scratch with [ConfigSet::default]
/// followed by [ConfigSet::add], the values added to the ConfigSet will be
/// independent of the values in all other ConfigSets.
///
/// When constructing a ConfigSet by cloning an existing ConfigSet, any values
/// cloned from the original ConfigSet will be shared with the original
/// ConfigSet. Updates to these values in one ConfigSet will be seen in the
/// other ConfigSet, and vice versa. Any value added to the new ConfigSet via
/// ConfigSet::add will be independent of values in the original ConfigSet,
/// unless the new ConfigSet is later cloned.
#[derive(Clone, Default)]
pub struct ConfigSet {
    configs: BTreeMap<String, ConfigEntry>,
}

impl ConfigSet {
    /// Adds the given config to this set.
    ///
    /// Names are required to be unique within a set, but each set is entirely
    /// independent. The same `Config` may be registered to multiple
    /// [`ConfigSet`]s and thus have independent values (e.g. imagine a unit
    /// test executing concurrently in the same process).
    ///
    /// Panics if a config with the same name has been previously registered
    /// to this set.
    pub fn add<D: ConfigDefault>(mut self, config: &Config<D>) -> Self {
        let default = config.default.clone().into_config_type();
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

    /// Returns the config with `name` registered to this set, if one exists.
    pub fn entry(&self, name: &str) -> Option<&ConfigEntry> {
        self.configs.get(name)
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
}

/// A handle to a configuration value in a [`ConfigSet`].
///
/// Allows users to amortize the lookup of a name within a set.
///
/// Handles can be cheaply cloned.
#[derive(Debug, Clone)]
pub struct ConfigValHandle<T> {
    val: ConfigValAtomic,
    _type: PhantomData<T>,
}

impl<T: ConfigType> ConfigValHandle<T> {
    /// Returns the latest value of this config within the set associated with
    /// the handle.
    pub fn get(&self) -> T {
        T::from_val(self.val.load())
    }
}

/// A type-erased configuration value for when set of different types are stored
/// in a collection.
#[derive(Clone, Debug, PartialEq)]
pub enum ConfigVal {
    /// A `bool` value.
    Bool(bool),
    /// A `u32` value.
    U32(u32),
    /// A `usize` value.
    Usize(usize),
    /// An `Option<usize>` value.
    OptUsize(Option<usize>),
    /// An `f64` value.
    F64(f64),
    /// A `String` value.
    String(String),
    /// A `Duration` value.
    Duration(Duration),
    /// A JSON value.
    Json(serde_json::Value),
}

/// An atomic version of [`ConfigVal`] to allow configuration values to be
/// shared between configuration writers and readers.
///
/// TODO(cfg): Consider moving these Arcs to be a single one around the map in
/// `ConfigSet` instead. That would mean less pointer-chasing in the common
/// case, but would remove the possibility of amortizing the name lookup via
/// [Config::handle].
#[derive(Clone, Debug)]
enum ConfigValAtomic {
    Bool(Arc<AtomicBool>),
    U32(Arc<AtomicU32>),
    Usize(Arc<AtomicUsize>),
    OptUsize(Arc<RwLock<Option<usize>>>),
    // Shared via to_bits/from_bits so we can use the atomic instead of Mutex.
    F64(Arc<AtomicU64>),
    String(Arc<RwLock<String>>),
    Duration(Arc<RwLock<Duration>>),
    Json(Arc<RwLock<serde_json::Value>>),
}

impl From<ConfigVal> for ConfigValAtomic {
    fn from(val: ConfigVal) -> ConfigValAtomic {
        match val {
            ConfigVal::Bool(x) => ConfigValAtomic::Bool(Arc::new(AtomicBool::new(x))),
            ConfigVal::U32(x) => ConfigValAtomic::U32(Arc::new(AtomicU32::new(x))),
            ConfigVal::Usize(x) => ConfigValAtomic::Usize(Arc::new(AtomicUsize::new(x))),
            ConfigVal::OptUsize(x) => ConfigValAtomic::OptUsize(Arc::new(RwLock::new(x))),
            ConfigVal::F64(x) => ConfigValAtomic::F64(Arc::new(AtomicU64::new(x.to_bits()))),
            ConfigVal::String(x) => ConfigValAtomic::String(Arc::new(RwLock::new(x))),
            ConfigVal::Duration(x) => ConfigValAtomic::Duration(Arc::new(RwLock::new(x))),
            ConfigVal::Json(x) => ConfigValAtomic::Json(Arc::new(RwLock::new(x))),
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
            ConfigValAtomic::F64(x) => ConfigVal::F64(f64::from_bits(x.load(SeqCst))),
            ConfigValAtomic::String(x) => {
                ConfigVal::String(x.read().expect("lock poisoned").clone())
            }
            ConfigValAtomic::Duration(x) => ConfigVal::Duration(*x.read().expect("lock poisoned")),
            ConfigValAtomic::Json(x) => ConfigVal::Json(x.read().expect("lock poisoned").clone()),
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
            (ConfigValAtomic::F64(x), ConfigVal::F64(val)) => x.store(val.to_bits(), SeqCst),
            (ConfigValAtomic::String(x), ConfigVal::String(val)) => {
                *x.write().expect("lock poisoned") = val
            }
            (ConfigValAtomic::Duration(x), ConfigVal::Duration(val)) => {
                *x.write().expect("lock poisoned") = val
            }
            (ConfigValAtomic::Json(x), ConfigVal::Json(val)) => {
                *x.write().expect("lock poisoned") = val
            }
            (ConfigValAtomic::Bool(_), val)
            | (ConfigValAtomic::U32(_), val)
            | (ConfigValAtomic::Usize(_), val)
            | (ConfigValAtomic::OptUsize(_), val)
            | (ConfigValAtomic::F64(_), val)
            | (ConfigValAtomic::String(_), val)
            | (ConfigValAtomic::Duration(_), val)
            | (ConfigValAtomic::Json(_), val) => {
                panic!("attempted to store {val:?} value in {self:?} parameter")
            }
        }
    }
}

impl ConfigUpdates {
    /// Adds an update for the given config and value.
    ///
    /// If a value of the same config has previously been added to these
    /// updates, replaces it.
    pub fn add<T, U>(&mut self, config: &Config<T>, val: U)
    where
        T: ConfigDefault,
        U: ConfigDefault<ConfigType = T::ConfigType>,
    {
        self.add_dynamic(config.name, val.into_config_type().into());
    }

    /// Adds an update for the given configuration name and value.
    ///
    /// It is the callers responsibility to ensure the value is of the
    /// appropriate type for the configuration.
    ///
    /// If a value of the same config has previously been added to these
    /// updates, replaces it.
    pub fn add_dynamic(&mut self, name: &str, val: ConfigVal) {
        self.updates.insert(
            name.to_owned(),
            ProtoConfigVal {
                val: val.into_proto(),
            },
        );
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
        for (name, ProtoConfigVal { val }) in self.updates.iter() {
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
    use std::num::{ParseFloatError, ParseIntError};
    use std::str::ParseBoolError;
    use std::time::Duration;

    use mz_ore::cast::CastFrom;
    use mz_proto::{ProtoType, RustType, TryFromProtoError};

    use crate::{
        proto_config_val, ConfigDefault, ConfigSet, ConfigType, ConfigVal, ProtoOptionU64,
    };

    impl ConfigType for bool {
        fn from_val(val: ConfigVal) -> Self {
            match val {
                ConfigVal::Bool(x) => x,
                x => panic!("expected bool value got {:?}", x),
            }
        }

        fn parse(s: &str) -> Result<Self, String> {
            match s {
                "on" => return Ok(true),
                "off" => return Ok(false),
                _ => {}
            }
            s.parse().map_err(|e: ParseBoolError| e.to_string())
        }
    }

    impl From<bool> for ConfigVal {
        fn from(val: bool) -> ConfigVal {
            ConfigVal::Bool(val)
        }
    }

    impl ConfigType for u32 {
        fn from_val(val: ConfigVal) -> Self {
            match val {
                ConfigVal::U32(x) => x,
                x => panic!("expected u32 value got {:?}", x),
            }
        }

        fn parse(s: &str) -> Result<Self, String> {
            s.parse().map_err(|e: ParseIntError| e.to_string())
        }
    }

    impl From<u32> for ConfigVal {
        fn from(val: u32) -> ConfigVal {
            ConfigVal::U32(val)
        }
    }

    impl ConfigType for usize {
        fn from_val(val: ConfigVal) -> Self {
            match val {
                ConfigVal::Usize(x) => x,
                x => panic!("expected usize value got {:?}", x),
            }
        }

        fn parse(s: &str) -> Result<Self, String> {
            s.parse().map_err(|e: ParseIntError| e.to_string())
        }
    }

    impl From<usize> for ConfigVal {
        fn from(val: usize) -> ConfigVal {
            ConfigVal::Usize(val)
        }
    }

    impl ConfigType for Option<usize> {
        fn from_val(val: ConfigVal) -> Self {
            match val {
                ConfigVal::OptUsize(x) => x,
                x => panic!("expected usize value got {:?}", x),
            }
        }

        fn parse(s: &str) -> Result<Self, String> {
            if s.is_empty() {
                Ok(None)
            } else {
                let val = s.parse().map_err(|e: ParseIntError| e.to_string())?;
                Ok(Some(val))
            }
        }
    }

    impl From<Option<usize>> for ConfigVal {
        fn from(val: Option<usize>) -> ConfigVal {
            ConfigVal::OptUsize(val)
        }
    }

    impl ConfigType for f64 {
        fn from_val(val: ConfigVal) -> Self {
            match val {
                ConfigVal::F64(x) => x,
                x => panic!("expected f64 value got {:?}", x),
            }
        }

        fn parse(s: &str) -> Result<Self, String> {
            s.parse().map_err(|e: ParseFloatError| e.to_string())
        }
    }

    impl From<f64> for ConfigVal {
        fn from(val: f64) -> ConfigVal {
            ConfigVal::F64(val)
        }
    }

    impl ConfigType for String {
        fn from_val(val: ConfigVal) -> Self {
            match val {
                ConfigVal::String(x) => x,
                x => panic!("expected String value got {:?}", x),
            }
        }

        fn parse(s: &str) -> Result<Self, String> {
            Ok(s.to_string())
        }
    }

    impl From<String> for ConfigVal {
        fn from(val: String) -> ConfigVal {
            ConfigVal::String(val)
        }
    }

    impl ConfigDefault for &str {
        type ConfigType = String;

        fn into_config_type(self) -> String {
            self.into()
        }
    }

    impl ConfigType for Duration {
        fn from_val(val: ConfigVal) -> Self {
            match val {
                ConfigVal::Duration(x) => x,
                x => panic!("expected Duration value got {:?}", x),
            }
        }

        fn parse(s: &str) -> Result<Self, String> {
            humantime::parse_duration(s).map_err(|e| e.to_string())
        }
    }

    impl From<Duration> for ConfigVal {
        fn from(val: Duration) -> ConfigVal {
            ConfigVal::Duration(val)
        }
    }

    impl ConfigType for serde_json::Value {
        fn from_val(val: ConfigVal) -> Self {
            match val {
                ConfigVal::Json(x) => x,
                x => panic!("expected JSON value got {:?}", x),
            }
        }

        fn parse(s: &str) -> Result<Self, String> {
            serde_json::from_str(s).map_err(|e| e.to_string())
        }
    }

    impl From<serde_json::Value> for ConfigVal {
        fn from(val: serde_json::Value) -> ConfigVal {
            ConfigVal::Json(val)
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
                ConfigVal::F64(x) => Val::F64(*x),
                ConfigVal::String(x) => Val::String(x.into_proto()),
                ConfigVal::Duration(x) => Val::Duration(x.into_proto()),
                ConfigVal::Json(x) => Val::Json(x.to_string()),
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
                Some(proto_config_val::Val::F64(x)) => ConfigVal::F64(x),
                Some(proto_config_val::Val::String(x)) => ConfigVal::String(x),
                Some(proto_config_val::Val::Duration(x)) => ConfigVal::Duration(x.into_rust()?),
                Some(proto_config_val::Val::Json(x)) => ConfigVal::Json(serde_json::from_str(&x)?),
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

    use mz_ore::assert_err;

    const BOOL: Config<bool> = Config::new("bool", true, "");
    const U32: Config<u32> = Config::new("u32", 4, "");
    const USIZE: Config<usize> = Config::new("usize", 1, "");
    const OPT_USIZE: Config<Option<usize>> = Config::new("opt_usize", Some(2), "");
    const F64: Config<f64> = Config::new("f64", 5.0, "");
    const STRING: Config<&str> = Config::new("string", "a", "");
    const DURATION: Config<Duration> = Config::new("duration", Duration::from_nanos(3), "");
    const JSON: Config<fn() -> serde_json::Value> =
        Config::new("json", || serde_json::json!({}), "");

    #[mz_ore::test]
    fn all_types() {
        let configs = ConfigSet::default()
            .add(&BOOL)
            .add(&USIZE)
            .add(&U32)
            .add(&OPT_USIZE)
            .add(&F64)
            .add(&STRING)
            .add(&DURATION)
            .add(&JSON);
        assert_eq!(BOOL.get(&configs), true);
        assert_eq!(U32.get(&configs), 4);
        assert_eq!(USIZE.get(&configs), 1);
        assert_eq!(OPT_USIZE.get(&configs), Some(2));
        assert_eq!(F64.get(&configs), 5.0);
        assert_eq!(STRING.get(&configs), "a");
        assert_eq!(DURATION.get(&configs), Duration::from_nanos(3));
        assert_eq!(JSON.get(&configs), serde_json::json!({}));

        let mut updates = ConfigUpdates::default();
        updates.add(&BOOL, false);
        updates.add(&U32, 7);
        updates.add(&USIZE, 2);
        updates.add(&OPT_USIZE, None);
        updates.add(&F64, 8.0);
        updates.add(&STRING, "b");
        updates.add(&DURATION, Duration::from_nanos(4));
        updates.add(&JSON, serde_json::json!({"a": 1}));
        updates.apply(&configs);

        assert_eq!(BOOL.get(&configs), false);
        assert_eq!(U32.get(&configs), 7);
        assert_eq!(USIZE.get(&configs), 2);
        assert_eq!(OPT_USIZE.get(&configs), None);
        assert_eq!(F64.get(&configs), 8.0);
        assert_eq!(STRING.get(&configs), "b");
        assert_eq!(DURATION.get(&configs), Duration::from_nanos(4));
        assert_eq!(JSON.get(&configs), serde_json::json!({"a": 1}));
    }

    #[mz_ore::test]
    fn fn_default() {
        const BOOL_FN_DEFAULT: Config<fn() -> bool> = Config::new("bool", || !true, "");
        const STRING_FN_DEFAULT: Config<fn() -> String> =
            Config::new("string", || "x".repeat(3), "");

        let configs = ConfigSet::default()
            .add(&BOOL_FN_DEFAULT)
            .add(&STRING_FN_DEFAULT);
        assert_eq!(BOOL_FN_DEFAULT.get(&configs), false);
        assert_eq!(STRING_FN_DEFAULT.get(&configs), "xxx");
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

    #[mz_ore::test]
    fn config_updates_extend() {
        // Regression test for database-issues#7793.
        //
        // Construct two ConfigUpdates with overlapping, but not identical, sets
        // of configs. Combine them and assert that the expected number of
        // updates is present.
        let mut u1 = {
            let c = ConfigSet::default().add(&USIZE).add(&STRING);
            let mut x = ConfigUpdates::default();
            for e in c.entries() {
                x.add_dynamic(e.name(), e.val());
            }
            x
        };
        let u2 = {
            let c = ConfigSet::default().add(&USIZE).add(&DURATION);
            let mut updates = ConfigUpdates::default();
            updates.add(&USIZE, 2);
            updates.apply(&c);
            let mut x = ConfigUpdates::default();
            for e in c.entries() {
                x.add_dynamic(e.name(), e.val());
            }
            x
        };
        assert_eq!(u1.updates.len(), 2);
        assert_eq!(u2.updates.len(), 2);
        u1.extend(u2);
        assert_eq!(u1.updates.len(), 3);

        // Assert that extend kept the correct (later) value for the overlapping
        // config.
        let c = ConfigSet::default().add(&USIZE);
        u1.apply(&c);
        assert_eq!(USIZE.get(&c), 2);
    }

    #[mz_ore::test]
    fn config_parse() {
        assert_eq!(BOOL.parse_val("true"), Ok(ConfigVal::Bool(true)));
        assert_eq!(BOOL.parse_val("on"), Ok(ConfigVal::Bool(true)));
        assert_eq!(BOOL.parse_val("false"), Ok(ConfigVal::Bool(false)));
        assert_eq!(BOOL.parse_val("off"), Ok(ConfigVal::Bool(false)));
        assert_err!(BOOL.parse_val("42"));
        assert_err!(BOOL.parse_val("66.6"));
        assert_err!(BOOL.parse_val("farragut"));
        assert_err!(BOOL.parse_val(""));
        assert_err!(BOOL.parse_val("5 s"));

        assert_err!(U32.parse_val("true"));
        assert_err!(U32.parse_val("false"));
        assert_eq!(U32.parse_val("42"), Ok(ConfigVal::U32(42)));
        assert_err!(U32.parse_val("66.6"));
        assert_err!(U32.parse_val("farragut"));
        assert_err!(U32.parse_val(""));
        assert_err!(U32.parse_val("5 s"));

        assert_err!(USIZE.parse_val("true"));
        assert_err!(USIZE.parse_val("false"));
        assert_eq!(USIZE.parse_val("42"), Ok(ConfigVal::Usize(42)));
        assert_err!(USIZE.parse_val("66.6"));
        assert_err!(USIZE.parse_val("farragut"));
        assert_err!(USIZE.parse_val(""));
        assert_err!(USIZE.parse_val("5 s"));

        assert_err!(OPT_USIZE.parse_val("true"));
        assert_err!(OPT_USIZE.parse_val("false"));
        assert_eq!(OPT_USIZE.parse_val("42"), Ok(ConfigVal::OptUsize(Some(42))));
        assert_err!(OPT_USIZE.parse_val("66.6"));
        assert_err!(OPT_USIZE.parse_val("farragut"));
        assert_eq!(OPT_USIZE.parse_val(""), Ok(ConfigVal::OptUsize(None)));
        assert_err!(OPT_USIZE.parse_val("5 s"));

        assert_err!(F64.parse_val("true"));
        assert_err!(F64.parse_val("false"));
        assert_eq!(F64.parse_val("42"), Ok(ConfigVal::F64(42.0)));
        assert_eq!(F64.parse_val("66.6"), Ok(ConfigVal::F64(66.6)));
        assert_err!(F64.parse_val("farragut"));
        assert_err!(F64.parse_val(""));
        assert_err!(F64.parse_val("5 s"));

        assert_eq!(
            STRING.parse_val("true"),
            Ok(ConfigVal::String("true".to_string()))
        );
        assert_eq!(
            STRING.parse_val("false"),
            Ok(ConfigVal::String("false".to_string()))
        );
        assert_eq!(
            STRING.parse_val("66.6"),
            Ok(ConfigVal::String("66.6".to_string()))
        );
        assert_eq!(
            STRING.parse_val("42"),
            Ok(ConfigVal::String("42".to_string()))
        );
        assert_eq!(
            STRING.parse_val("farragut"),
            Ok(ConfigVal::String("farragut".to_string()))
        );
        assert_eq!(STRING.parse_val(""), Ok(ConfigVal::String("".to_string())));
        assert_eq!(
            STRING.parse_val("5 s"),
            Ok(ConfigVal::String("5 s".to_string()))
        );

        assert_err!(DURATION.parse_val("true"));
        assert_err!(DURATION.parse_val("false"));
        assert_err!(DURATION.parse_val("42"));
        assert_err!(DURATION.parse_val("66.6"));
        assert_err!(DURATION.parse_val("farragut"));
        assert_err!(DURATION.parse_val(""));
        assert_eq!(
            DURATION.parse_val("5 s"),
            Ok(ConfigVal::Duration(Duration::from_secs(5)))
        );

        assert_eq!(
            JSON.parse_val("true"),
            Ok(ConfigVal::Json(serde_json::json!(true)))
        );
        assert_eq!(
            JSON.parse_val("false"),
            Ok(ConfigVal::Json(serde_json::json!(false)))
        );
        assert_eq!(
            JSON.parse_val("42"),
            Ok(ConfigVal::Json(serde_json::json!(42)))
        );
        assert_eq!(
            JSON.parse_val("66.6"),
            Ok(ConfigVal::Json(serde_json::json!(66.6)))
        );
        assert_err!(JSON.parse_val("farragut"));
        assert_err!(JSON.parse_val(""));
        assert_err!(JSON.parse_val("5 s"));
        assert_eq!(
            JSON.parse_val("{\"joe\": \"developer\"}"),
            Ok(ConfigVal::Json(serde_json::json!({"joe": "developer"})))
        );
    }
}
