// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! SQL configuration parameters and connection state.
//!
//! Materialize roughly follows the PostgreSQL configuration model, which works
//! as follows. There is a global set of named configuration parameters, like
//! `DateStyle` and `client_encoding`. These parameters can be set in several
//! places: in an on-disk configuration file (in Postgres, named
//! postgresql.conf), in command line arguments when the server is started, or
//! at runtime via the `ALTER SYSTEM` or `SET` statements. Parameters that are
//! set in a session take precedence over database defaults, which in turn take
//! precedence over command line arguments, which in turn take precedence over
//! settings in the on-disk configuration.
//!
//! The Materialize configuration hierarchy at the moment is much simpler.
//! Global defaults are hardcoded into the binary, and a select few parameters
//! can be overridden per session. The infrastructure has been designed with
//! an eye towards supporting additional layers to the hierarchy, however, as
//! should the need arise.
//!
//! The configuration parameters that exist are driven by compatibility with
//! PostgreSQL drivers that expect them, not because they are particularly
//! important.

#![forbid(missing_docs)]

#[allow(clippy::module_inception)]
mod session;
mod statement;
mod transaction;
mod var;

pub use session::Session;
pub use statement::{FieldFormat, Portal, PreparedStatement};
pub use transaction::TransactionStatus;
