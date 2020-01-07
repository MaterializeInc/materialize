// Copyright 2020 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Track and clean up the state of the Data Definition Layer between test files

use crate::error::Error;

/// The state of the data definition layer
#[derive(Debug)]
pub struct Ddl {
    sources: Vec<String>,
    views: Vec<String>,
}

impl Ddl {
    /// Initialize from mz
    pub fn new(pgclient: &mut postgres::Client) -> Result<Ddl, Error> {
        let sources = Self::load_names(pgclient, "SHOW SOURCES")?;
        let views = Self::load_names(pgclient, "SHOW VIEWS")?;
        Ok(Ddl { sources, views })
    }

    /// Clear things that did not exist when [`new`] was called
    pub fn clear_since_new(&mut self, pgclient: &mut postgres::Client) -> Result<(), Error> {
        let views = Self::load_names(pgclient, "SHOW VIEWS")?;
        let new_views: Vec<&str> = views
            .iter()
            .filter(|v| !self.views.contains(v))
            .map(AsRef::as_ref)
            .collect();
        Self::clear_inner(pgclient, "VIEW", new_views);

        let sources = Self::load_names(pgclient, "SHOW SOURCES")?;
        let new_sources: Vec<&str> = sources
            .iter()
            .filter(|s| !self.sources.contains(s))
            .map(AsRef::as_ref)
            .collect();

        Self::clear_inner(pgclient, "SOURCE", new_sources);

        Ok(())
    }

    // helpers

    fn load_names(pgclient: &mut postgres::Client, query: &str) -> Result<Vec<String>, Error> {
        Ok(pgclient
            .query(query, &[])?
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect())
    }

    fn clear_inner(pgclient: &mut postgres::Client, kind: &str, mut names: Vec<&str>) {
        let mut next_round: Vec<_> = vec![];
        let max_tries = names.len();
        let mut tries = 0;
        let mut err = None;
        if !names.is_empty() {
            println!("> DROP {} {}", kind, names.join(", "));
        }
        while !names.is_empty() && tries < max_tries {
            for source in names {
                if let Err(e) = pgclient.execute(&*format!("DROP {} {}", kind, source), &[]) {
                    next_round.push(&*source);
                    err = Some(e);
                }
            }
            names = next_round.clone();
            next_round.clear();
            tries += 1;
        }
        if !names.is_empty() {
            println!(
                "WARNING: unable to clear all names from {}s, final error: {}",
                kind,
                err.map(|e| e.to_string())
                    .unwrap_or_else(|| "<unknown>".to_string())
            )
        }
    }
}
