use std::{borrow::Cow, io};

use crate::{
    constants::SessionStateType,
    io::ParseBuf,
    misc::raw::{bytes::EofBytes, int::LenEnc, RawBytes},
    proto::{MyDeserialize, MySerialize},
};

// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

/// Represents a parsed change in a session state (part of MySql's Ok packet).
///
/// See [MySql docs][1].
///
/// [1]: https://dev.mysql.com/doc/c-api/5.7/en/mysql-session-track-get-first.html
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum SessionStateChange<'a> {
    IsTracked(bool),
    Schema(Schema<'a>),
    SystemVariables(Vec<SystemVariable<'a>>),
    Gtids(Gtids<'a>),
    TransactionCharacteristics(TransactionCharacteristics<'a>),
    TransactionState(TransactionState<'a>),
    Unsupported(Unsupported<'a>),
}

impl<'a> SessionStateChange<'a> {
    pub fn into_owned(self) -> SessionStateChange<'static> {
        match self {
            SessionStateChange::SystemVariables(x) => SessionStateChange::SystemVariables(
                x.into_iter().map(SystemVariable::into_owned).collect(),
            ),
            SessionStateChange::Schema(schema) => SessionStateChange::Schema(schema.into_owned()),
            SessionStateChange::IsTracked(x) => SessionStateChange::IsTracked(x),
            SessionStateChange::Gtids(x) => SessionStateChange::Gtids(x.into_owned()),
            SessionStateChange::TransactionCharacteristics(x) => {
                SessionStateChange::TransactionCharacteristics(x.into_owned())
            }
            SessionStateChange::TransactionState(x) => {
                SessionStateChange::TransactionState(x.into_owned())
            }
            SessionStateChange::Unsupported(x) => SessionStateChange::Unsupported(x.into_owned()),
        }
    }
}

impl<'de> MyDeserialize<'de> for SessionStateChange<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = SessionStateType;

    fn deserialize(ty: SessionStateType, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        match ty {
            SessionStateType::SESSION_TRACK_SYSTEM_VARIABLES => {
                let mut vars = Vec::new();
                while !buf.is_empty() {
                    vars.push(buf.parse_unchecked(())?);
                }
                Ok(SessionStateChange::SystemVariables(vars))
            }
            SessionStateType::SESSION_TRACK_SCHEMA => {
                buf.parse_unchecked(()).map(SessionStateChange::Schema)
            }
            SessionStateType::SESSION_TRACK_STATE_CHANGE => {
                let is_tracked: RawBytes<'_, LenEnc> = buf.parse_unchecked(())?;
                Ok(SessionStateChange::IsTracked(is_tracked.as_bytes() == b"1"))
            }
            // Layout isn't specified in the documentation
            SessionStateType::SESSION_TRACK_GTIDS => {
                Ok(SessionStateChange::Gtids(buf.parse_unchecked(())?))
            }
            SessionStateType::SESSION_TRACK_TRANSACTION_CHARACTERISTICS => Ok(
                SessionStateChange::TransactionCharacteristics(buf.parse_unchecked(())?),
            ),
            SessionStateType::SESSION_TRACK_TRANSACTION_STATE => buf
                .parse_unchecked(())
                .map(SessionStateChange::TransactionState),
        }
    }
}

impl MySerialize for SessionStateChange<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            SessionStateChange::SystemVariables(vars) => {
                for var in vars {
                    var.serialize(&mut *buf);
                }
            }
            SessionStateChange::Schema(schema) => schema.serialize(buf),
            SessionStateChange::IsTracked(is_tracked) => {
                if *is_tracked {
                    b"\x011".serialize(buf);
                } else {
                    b"\x010".serialize(buf);
                }
            }
            SessionStateChange::Gtids(x) => x.serialize(buf),
            SessionStateChange::TransactionCharacteristics(x) => x.serialize(buf),
            SessionStateChange::TransactionState(x) => x.serialize(buf),
            SessionStateChange::Unsupported(x) => x.serialize(buf),
        }
    }
}

/// This tracker type indicates that GTIDs are available and contains the GTID string.
///
/// The GTID string is in the standard format for specifying a set of GTID values.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Gtids<'a>(RawBytes<'a, EofBytes>);

impl<'a> Gtids<'a> {
    pub fn new(gtid_set: impl Into<Cow<'a, [u8]>>) -> Self {
        Self(RawBytes::new(gtid_set))
    }

    /// Returns a raw GTID string.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Returns a GTID string (lossy converted).
    pub fn as_str(&self) -> Cow<'_, str> {
        self.0.as_str()
    }

    /// Returns a `'static` version of self.
    pub fn into_owned(self) -> Gtids<'static> {
        Gtids(self.0.into_owned())
    }
}

impl<'de> MyDeserialize<'de> for Gtids<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        buf.parse_unchecked(()).map(Self)
    }
}

impl MySerialize for Gtids<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.0.serialize(buf);
    }
}

/// This tracker type indicates that the default schema has been set.
///
/// The value contains the new default schema name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Schema<'a>(RawBytes<'a, LenEnc>);

impl<'a> Schema<'a> {
    pub fn new(schema_name: impl Into<Cow<'a, [u8]>>) -> Self {
        Self(RawBytes::new(schema_name))
    }

    /// Returns a raw schema name.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Returns schema name (lossy converted).
    pub fn as_str(&self) -> Cow<'_, str> {
        self.0.as_str()
    }

    /// Returns a `'static` version of `self`.
    pub fn into_owned(self) -> Schema<'static> {
        Schema(self.0.into_owned())
    }
}

impl<'de> MyDeserialize<'de> for Schema<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        buf.parse_unchecked(()).map(Self)
    }
}

impl MySerialize for Schema<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.0.serialize(buf);
    }
}

/// This tracker type indicates that one or more tracked session
/// system variables have been assigned a value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SystemVariable<'a> {
    name: RawBytes<'a, LenEnc>,
    value: RawBytes<'a, LenEnc>,
}

impl<'a> SystemVariable<'a> {
    pub fn new(name: impl Into<Cow<'a, [u8]>>, value: impl Into<Cow<'a, [u8]>>) -> Self {
        Self {
            name: RawBytes::new(name),
            value: RawBytes::new(value),
        }
    }

    /// Returns a raw name.
    pub fn name_bytes(&self) -> &[u8] {
        self.name.as_bytes()
    }

    /// Returns a name (lossy converted).
    pub fn name_str(&self) -> Cow<'_, str> {
        self.name.as_str()
    }

    /// Returns a raw value.
    pub fn value_bytes(&self) -> &[u8] {
        self.value.as_bytes()
    }

    /// Returns a value (lossy converted).
    pub fn value_str(&self) -> Cow<'_, str> {
        self.value.as_str()
    }

    /// Returns a `'static` version of `self`.
    pub fn into_owned(self) -> SystemVariable<'static> {
        SystemVariable {
            name: self.name.into_owned(),
            value: self.value.into_owned(),
        }
    }
}

impl<'de> MyDeserialize<'de> for SystemVariable<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            name: buf.parse_unchecked(())?,
            value: buf.parse_unchecked(())?,
        })
    }
}

impl MySerialize for SystemVariable<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.name.serialize(&mut *buf);
        self.value.serialize(buf);
    }
}

/// This tracker type indicates that transaction characteristics are available.
///
/// The characteristics tracker data string may be empty,
/// or it may contain one or more SQL statements, each terminated by a semicolon.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TransactionCharacteristics<'a>(RawBytes<'a, LenEnc>);

impl<'a> TransactionCharacteristics<'a> {
    pub fn new(value: impl Into<Cow<'a, [u8]>>) -> Self {
        Self(RawBytes::new(value))
    }

    /// Returns a raw value.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Returns a value (lossy converted).
    pub fn as_str(&self) -> Cow<'_, str> {
        self.0.as_str()
    }

    /// Returns a `'static` version of `self`.
    pub fn into_owned(self) -> TransactionCharacteristics<'static> {
        TransactionCharacteristics(self.0.into_owned())
    }
}

impl<'de> MyDeserialize<'de> for TransactionCharacteristics<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        buf.parse_unchecked(()).map(Self)
    }
}

impl MySerialize for TransactionCharacteristics<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.0.serialize(buf);
    }
}

/// This tracker type indicates that transaction state information is available.
///
/// Value is a string containing ASCII characters, each of which indicates some aspect
/// of the transaction state.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TransactionState<'a>(RawBytes<'a, LenEnc>);

impl<'a> TransactionState<'a> {
    pub fn new(value: impl Into<Cow<'a, [u8]>>) -> Self {
        Self(RawBytes::new(value))
    }

    /// Returns a raw value.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Returns a value (lossy converted).
    pub fn as_str(&self) -> Cow<'_, str> {
        self.0.as_str()
    }

    /// Returns a `'static` version of `self`.
    pub fn into_owned(self) -> TransactionState<'static> {
        TransactionState(self.0.into_owned())
    }
}

impl<'de> MyDeserialize<'de> for TransactionState<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        buf.parse_unchecked(()).map(Self)
    }
}

impl MySerialize for TransactionState<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.0.serialize(buf);
    }
}

/// This tracker type is unknown/unsupported.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Unsupported<'a>(RawBytes<'a, LenEnc>);

impl<'a> Unsupported<'a> {
    pub fn new(value: impl Into<Cow<'a, [u8]>>) -> Self {
        Self(RawBytes::new(value))
    }

    /// Returns a value as a slice of bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Returns a value as a string (lossy converted).
    pub fn as_str(&self) -> Cow<'_, str> {
        self.0.as_str()
    }

    /// Returns a `'static` version of `self`.
    pub fn into_owned(self) -> Unsupported<'static> {
        Unsupported(self.0.into_owned())
    }
}

impl<'de> MyDeserialize<'de> for Unsupported<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        buf.parse_unchecked(()).map(Self)
    }
}

impl MySerialize for Unsupported<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.0.serialize(buf);
    }
}
