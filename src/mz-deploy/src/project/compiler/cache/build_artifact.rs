// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persistence layer for the incremental compiler.
//!
//! Stores advisory build state scoped to one profile namespace. The database
//! persists four categories of state:
//!
//! - **File metadata** — Content hashes and source text, keyed by source path.
//!   Freshness is determined by file size and modification time; stale entries
//!   are transparently refreshed from disk.
//! - **Object artifacts** — Compiled object payloads, keyed by logical object
//!   identifier and content fingerprint.
//! - **Typecheck artifacts** — Per-object validation results (fingerprints,
//!   column schemas), used for incremental dirty detection.
//! - **Project snapshot** — Full compiled project graph for read-only consumers
//!   (LSP, explain).
//!
//! All cached state is advisory. Missing, corrupt, or schema-incompatible
//! entries are treated as cache misses and rebuilt from source. The compiler
//! owns the schema version; a version mismatch triggers a full rebuild of the
//! namespace-local database.

use super::CacheError;
use super::schema;
use crate::project::ast::Statement;
use crate::project::compiler::cache_io::hex_digest;
use crate::project::ir::graph;
use crate::project::ir::infrastructure::{self, Infrastructure};
use crate::project::ir::object_id::ObjectId;
use crate::project::resolve::cte_scope::CteScope;
use crate::types::ColumnType;
use mz_sql_parser::ast::visit::{self, Visit};
use mz_sql_parser::ast::{CommentObjectType, Raw, RawClusterName, TableFactor};
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

const OBJECT_STATE_TABLE: &str = "object_state";
const TYPECHECK_OBJECTS_TABLE: &str = "typecheck_objects";
const TYPECHECK_COLUMNS_TABLE: &str = "typecheck_columns";

fn db_err(path: &Path) -> impl Fn(rusqlite::Error) -> CacheError + '_ {
    move |source| CacheError::DatabaseOperationFailed {
        path: path.to_path_buf(),
        source,
    }
}

fn file_read_err(path: &Path) -> impl Fn(std::io::Error) -> CacheError + '_ {
    move |source| CacheError::FileReadFailed {
        path: path.to_path_buf(),
        source,
    }
}

const FILE_STATE_UPSERT: &str = "
    INSERT INTO file_state(path, size, mtime_ns, content_hash, contents)
    VALUES(?1, ?2, ?3, ?4, ?5)
    ON CONFLICT(path) DO UPDATE SET
        size = excluded.size,
        mtime_ns = excluded.mtime_ns,
        content_hash = excluded.content_hash,
        contents = excluded.contents
";

/// Read the file at `path`, compute its hash, and upsert the row into
/// `file_state`. Returns the hash and contents.
fn read_and_upsert_file(
    upsert: &mut rusqlite::Statement<'_>,
    path: &Path,
    path_str: &str,
    size: i64,
    mtime_ns: i64,
    db_path: &Path,
) -> Result<(String, String), CacheError> {
    let contents = fs::read_to_string(path).map_err(file_read_err(path))?;
    let content_hash = hex_digest(Sha256::digest(contents.as_bytes()));
    upsert
        .execute(params![path_str, size, mtime_ns, &content_hash, &contents])
        .map_err(db_err(db_path))?;
    Ok((content_hash, contents))
}

/// A persisted compile artifact for one logical object.
///
/// `Skipped` records that the object was intentionally excluded for the active
/// profile (e.g., a profile variant that doesn't match). `Object` carries the
/// SQL strings that constitute a compiled object — they are stored verbatim
/// and re-parsed on cache hit.
#[derive(Debug, Clone)]
pub(crate) enum CompiledObjectArtifact {
    Skipped,
    Object(CompiledObjectArtifactData),
}

/// SQL fragments that together describe a compiled database object.
///
/// Stores SQL text rather than AST nodes because the AST types are not
/// `Serialize`-able. On cache hit, the strings are re-parsed back into AST.
#[derive(Debug, Clone)]
pub(crate) struct CompiledObjectArtifactData {
    pub db_name: String,
    pub schema_name: String,
    pub file_path: PathBuf,
    pub stmt_sql: String,
    pub indexes_sql: Vec<String>,
    pub grants_sql: Vec<String>,
    pub comments_sql: Vec<String>,
    pub tests_sql: Vec<String>,
}

/// Column values to write into the `object_state` row, derived from a
/// [`CompiledObjectArtifact`].
struct ObjectStateHeader<'a> {
    kind: &'static str,
    db_name: Option<&'a str>,
    schema_name: Option<&'a str>,
    file_path: Option<String>,
    stmt_sql: Option<&'a str>,
}

impl<'a> ObjectStateHeader<'a> {
    fn from_artifact(artifact: &'a CompiledObjectArtifact) -> Self {
        match artifact {
            CompiledObjectArtifact::Skipped => Self {
                kind: "skipped",
                db_name: None,
                schema_name: None,
                file_path: None,
                stmt_sql: None,
            },
            CompiledObjectArtifact::Object(data) => Self {
                kind: "object",
                db_name: Some(&data.db_name),
                schema_name: Some(&data.schema_name),
                file_path: Some(data.file_path.to_string_lossy().into_owned()),
                stmt_sql: Some(&data.stmt_sql),
            },
        }
    }
}

fn prepare_delete<'tx>(
    tx: &'tx rusqlite::Transaction<'_>,
    table: &str,
    path: &Path,
) -> Result<rusqlite::Statement<'tx>, CacheError> {
    tx.prepare(&format!("DELETE FROM {table} WHERE object_key = ?1"))
        .map_err(db_err(path))
}

fn prepare_fragment_insert<'tx>(
    tx: &'tx rusqlite::Transaction<'_>,
    table: &str,
    path: &Path,
) -> Result<rusqlite::Statement<'tx>, CacheError> {
    tx.prepare(&format!(
        "INSERT INTO {table}(object_key, position, sql_text) VALUES(?1, ?2, ?3)"
    ))
    .map_err(db_err(path))
}

fn run_execute(
    stmt: &mut rusqlite::Statement<'_>,
    params: impl rusqlite::Params,
    path: &Path,
) -> Result<(), CacheError> {
    stmt.execute(params).map(|_| ()).map_err(db_err(path))
}

fn write_fragments(
    stmt: &mut rusqlite::Statement<'_>,
    object_key: &str,
    fragments: &[String],
    path: &Path,
) -> Result<(), CacheError> {
    for (position, sql) in fragments.iter().enumerate() {
        let position = i64::try_from(position).unwrap_or(i64::MAX);
        stmt.execute(params![object_key, position, sql])
            .map_err(db_err(path))?;
    }
    Ok(())
}

fn collect_fragments(
    stmt: &mut rusqlite::Statement<'_>,
    object_key: &str,
    path: &Path,
) -> Result<Vec<String>, CacheError> {
    let rows = stmt
        .query_map(params![object_key], |row| row.get::<_, String>(0))
        .map_err(db_err(path))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(db_err(path))?);
    }
    Ok(out)
}

pub(crate) struct BuildArtifact {
    path: PathBuf,
    conn: Connection,
}

impl BuildArtifact {
    /// Open (or create) the SQLite build artifact database for a profile namespace.
    ///
    /// The namespace directory is derived from the active profile name, optional
    /// suffix, and compile-time variable bindings, so different profiles use
    /// isolated caches. On schema version mismatch the database is dropped and
    /// recreated — safe because all cached state is advisory.
    pub(crate) fn open(
        root: &Path,
        profile: &str,
        profile_suffix: Option<&str>,
        variables: &BTreeMap<String, String>,
    ) -> Result<Self, CacheError> {
        let path = super::db_path(root, profile, profile_suffix, variables);
        let parent = path
            .parent()
            .expect("cache db path always has a parent directory");
        fs::create_dir_all(parent).map_err(|source| CacheError::DirectoryCreationFailed {
            path: parent.to_path_buf(),
            source,
        })?;
        let conn = Connection::open(&path).map_err(|source| CacheError::DatabaseOpenFailed {
            path: path.clone(),
            source,
        })?;
        let db = Self { path, conn };
        db.initialize()?;
        Ok(db)
    }

    fn initialize(&self) -> Result<(), CacheError> {
        // Negative cache_size is in KiB; positive would be page count.
        const PAGE_CACHE_KIB: i64 = -64 * 1024;
        const MMAP_BYTES: i64 = 256 * 1024 * 1024;
        const WAL_AUTOCHECKPOINT_PAGES: i64 = 10_000;
        let pragmas = format!(
            "
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            PRAGMA cache_size={PAGE_CACHE_KIB};
            PRAGMA temp_store=MEMORY;
            PRAGMA mmap_size={MMAP_BYTES};
            PRAGMA wal_autocheckpoint={WAL_AUTOCHECKPOINT_PAGES};
            ",
        );
        self.conn
            .execute_batch(&pragmas)
            .map_err(db_err(&self.path))?;
        self.conn
            .execute_batch(
                "
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                ",
            )
            .map_err(db_err(&self.path))?;

        let version: Option<i64> = self
            .conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'schema_version'",
                [],
                |row| {
                    row.get::<_, String>(0)
                        .map(|s| s.parse::<i64>().unwrap_or_default())
                },
            )
            .optional()
            .map_err(db_err(&self.path))?;

        if version != Some(schema::SCHEMA_VERSION) {
            self.conn
                .execute_batch(schema::DROP_SQL)
                .map_err(db_err(&self.path))?;
        }

        self.create_schema()?;
        Ok(())
    }

    fn create_schema(&self) -> Result<(), CacheError> {
        self.conn
            .execute_batch(schema::CREATE_SQL)
            .map_err(db_err(&self.path))?;

        self.conn
            .execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES ('schema_version', ?1)",
                params![schema::SCHEMA_VERSION.to_string()],
            )
            .map_err(db_err(&self.path))?;
        Ok(())
    }

    /// Load content hashes for the requested source paths. Stale or missing
    /// cache entries are transparently refreshed from disk.
    pub(crate) fn load_file_hashes(
        &mut self,
        fs: &crate::fs::FileSystem,
        paths: &BTreeSet<PathBuf>,
    ) -> Result<BTreeMap<PathBuf, String>, CacheError> {
        let tx = self.conn.transaction().map_err(db_err(&self.path))?;
        let mut select = tx
            .prepare("SELECT size, mtime_ns, content_hash FROM file_state WHERE path = ?1")
            .map_err(db_err(&self.path))?;
        let mut upsert = tx.prepare(FILE_STATE_UPSERT).map_err(db_err(&self.path))?;

        let mut results = BTreeMap::new();
        for path in paths {
            // Overlay-covered paths bypass the disk-keyed content cache:
            // disk size+mtime are unchanged while the in-memory buffer can
            // differ, so a cache hit would serve stale disk bytes.
            if fs.is_overlay(path) {
                let contents = fs.read_to_string(path).map_err(file_read_err(path))?;
                results.insert(
                    path.clone(),
                    hex_digest(Sha256::digest(contents.as_bytes())),
                );
                continue;
            }

            let (size, mtime_ns) = file_metadata_signature(path)?;
            let path_str = path.to_string_lossy().to_string();
            let cached: Option<(i64, i64, String)> = select
                .query_row([&path_str], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .optional()
                .map_err(db_err(&self.path))?;

            let hash = match cached {
                Some((s, m, h)) if s == size && m == mtime_ns => h,
                _ => {
                    let (h, _) = read_and_upsert_file(
                        &mut upsert,
                        path,
                        &path_str,
                        size,
                        mtime_ns,
                        &self.path,
                    )?;
                    h
                }
            };
            results.insert(path.clone(), hash);
        }

        drop(select);
        drop(upsert);
        tx.commit().map_err(db_err(&self.path))?;
        Ok(results)
    }

    /// Load file contents for the requested source paths. Stale, missing,
    /// or contents-NULL cache entries are transparently refreshed from disk.
    pub(crate) fn load_file_contents(
        &mut self,
        fs: &crate::fs::FileSystem,
        paths: &BTreeSet<PathBuf>,
    ) -> Result<BTreeMap<PathBuf, String>, CacheError> {
        let tx = self.conn.transaction().map_err(db_err(&self.path))?;
        let mut select = tx
            .prepare(
                "SELECT size, mtime_ns, contents \
                 FROM file_state WHERE path = ?1",
            )
            .map_err(db_err(&self.path))?;
        let mut upsert = tx.prepare(FILE_STATE_UPSERT).map_err(db_err(&self.path))?;

        let mut results = BTreeMap::new();
        for path in paths {
            if fs.is_overlay(path) {
                let contents = fs.read_to_string(path).map_err(file_read_err(path))?;
                results.insert(path.clone(), contents);
                continue;
            }

            let (size, mtime_ns) = file_metadata_signature(path)?;
            let path_str = path.to_string_lossy().to_string();
            let cached: Option<(i64, i64, Option<String>)> = select
                .query_row([&path_str], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .optional()
                .map_err(db_err(&self.path))?;

            let contents = match cached {
                Some((s, m, Some(c))) if s == size && m == mtime_ns => c,
                _ => {
                    let (_, contents) = read_and_upsert_file(
                        &mut upsert,
                        path,
                        &path_str,
                        size,
                        mtime_ns,
                        &self.path,
                    )?;
                    contents
                }
            };
            results.insert(path.clone(), contents);
        }

        drop(select);
        drop(upsert);
        tx.commit().map_err(db_err(&self.path))?;
        Ok(results)
    }

    /// Load just the (object_key, fingerprint) pairs from `object_state`.
    ///
    /// Used during the planning phase to detect cache hits without paying the
    /// cost of materializing each object's SQL fragments.
    pub(crate) fn load_object_fingerprints(&self) -> Result<BTreeMap<String, String>, CacheError> {
        let mut stmt = self
            .conn
            .prepare("SELECT object_key, fingerprint FROM object_state")
            .map_err(db_err(&self.path))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(db_err(&self.path))?;

        let mut result = BTreeMap::new();
        for row in rows {
            let (key, fingerprint) = row.map_err(db_err(&self.path))?;
            result.insert(key, fingerprint);
        }
        Ok(result)
    }

    /// Load full compile artifacts for the requested object keys. Only keys
    /// present in `object_state` produce an entry.
    pub(crate) fn load_object_artifacts(
        &self,
        keys: &BTreeSet<String>,
    ) -> Result<BTreeMap<String, CompiledObjectArtifact>, CacheError> {
        let mut result = BTreeMap::new();
        if keys.is_empty() {
            return Ok(result);
        }

        let mut header = self
            .conn
            .prepare(
                "SELECT kind, db_name, schema_name, file_path, stmt_sql \
                 FROM object_state WHERE object_key = ?1",
            )
            .map_err(db_err(&self.path))?;
        let mut indexes = self.prepare_fragment_select("object_state_indexes")?;
        let mut grants = self.prepare_fragment_select("object_state_grants")?;
        let mut comments = self.prepare_fragment_select("object_state_comments")?;
        let mut tests = self.prepare_fragment_select("object_state_tests")?;

        for key in keys {
            let row = header
                .query_row(params![key], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                })
                .optional()
                .map_err(db_err(&self.path))?;
            let Some((kind, db_name, schema_name, file_path, stmt_sql)) = row else {
                continue;
            };
            let artifact = match kind.as_str() {
                "skipped" => CompiledObjectArtifact::Skipped,
                "object" => CompiledObjectArtifact::Object(CompiledObjectArtifactData {
                    db_name: db_name.unwrap_or_default(),
                    schema_name: schema_name.unwrap_or_default(),
                    file_path: file_path.map(PathBuf::from).unwrap_or_default(),
                    stmt_sql: stmt_sql.unwrap_or_default(),
                    indexes_sql: collect_fragments(&mut indexes, key, &self.path)?,
                    grants_sql: collect_fragments(&mut grants, key, &self.path)?,
                    comments_sql: collect_fragments(&mut comments, key, &self.path)?,
                    tests_sql: collect_fragments(&mut tests, key, &self.path)?,
                }),
                _ => continue,
            };
            result.insert(key.clone(), artifact);
        }
        Ok(result)
    }

    fn prepare_fragment_select(&self, table: &str) -> Result<rusqlite::Statement<'_>, CacheError> {
        self.conn
            .prepare(&format!(
                "SELECT sql_text FROM {table} WHERE object_key = ?1 ORDER BY position"
            ))
            .map_err(db_err(&self.path))
    }

    /// Replace each object's fragment rows so row-count changes (e.g., a
    /// removed index) are reflected exactly.
    pub(crate) fn upsert_object_rows(&mut self, rows: &[ObjectStateRow]) -> Result<(), CacheError> {
        let tx = self.conn.transaction().map_err(db_err(&self.path))?;
        {
            let mut upsert_header = tx
                .prepare(
                    "
                    INSERT INTO object_state(
                        object_key, fingerprint, kind, db_name, schema_name, file_path, stmt_sql
                    )
                    VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
                    ON CONFLICT(object_key) DO UPDATE SET
                        fingerprint = excluded.fingerprint,
                        kind = excluded.kind,
                        db_name = excluded.db_name,
                        schema_name = excluded.schema_name,
                        file_path = excluded.file_path,
                        stmt_sql = excluded.stmt_sql
                    ",
                )
                .map_err(db_err(&self.path))?;
            let mut delete_indexes = prepare_delete(&tx, "object_state_indexes", &self.path)?;
            let mut delete_grants = prepare_delete(&tx, "object_state_grants", &self.path)?;
            let mut delete_comments = prepare_delete(&tx, "object_state_comments", &self.path)?;
            let mut delete_tests = prepare_delete(&tx, "object_state_tests", &self.path)?;
            let mut insert_indexes =
                prepare_fragment_insert(&tx, "object_state_indexes", &self.path)?;
            let mut insert_grants =
                prepare_fragment_insert(&tx, "object_state_grants", &self.path)?;
            let mut insert_comments =
                prepare_fragment_insert(&tx, "object_state_comments", &self.path)?;
            let mut insert_tests = prepare_fragment_insert(&tx, "object_state_tests", &self.path)?;

            for row in rows {
                let header = ObjectStateHeader::from_artifact(&row.artifact);
                upsert_header
                    .execute(params![
                        row.object_key,
                        row.fingerprint,
                        header.kind,
                        header.db_name,
                        header.schema_name,
                        header.file_path,
                        header.stmt_sql,
                    ])
                    .map_err(db_err(&self.path))?;

                run_execute(&mut delete_indexes, params![row.object_key], &self.path)?;
                run_execute(&mut delete_grants, params![row.object_key], &self.path)?;
                run_execute(&mut delete_comments, params![row.object_key], &self.path)?;
                run_execute(&mut delete_tests, params![row.object_key], &self.path)?;

                if let CompiledObjectArtifact::Object(data) = &row.artifact {
                    write_fragments(
                        &mut insert_indexes,
                        &row.object_key,
                        &data.indexes_sql,
                        &self.path,
                    )?;
                    write_fragments(
                        &mut insert_grants,
                        &row.object_key,
                        &data.grants_sql,
                        &self.path,
                    )?;
                    write_fragments(
                        &mut insert_comments,
                        &row.object_key,
                        &data.comments_sql,
                        &self.path,
                    )?;
                    write_fragments(
                        &mut insert_tests,
                        &row.object_key,
                        &data.tests_sql,
                        &self.path,
                    )?;
                }
            }
        }
        tx.commit().map_err(db_err(&self.path))
    }

    /// Remove `object_state` rows for objects no longer in the current project.
    pub(crate) fn prune_object_rows(&mut self, keep: &BTreeSet<String>) -> Result<(), CacheError> {
        self.prune_rows(OBJECT_STATE_TABLE, keep)
    }

    /// Persist or update typecheck artifacts for a batch of objects.
    ///
    /// Column records for an object are fully replaced (no partial updates).
    pub(crate) fn upsert_typecheck_results(
        &mut self,
        rows: &[(String, String, BTreeMap<String, ColumnType>)],
    ) -> Result<(), CacheError> {
        let tx = self.conn.transaction().map_err(db_err(&self.path))?;
        {
            let mut upsert_obj = tx
                .prepare(
                    "
                    INSERT INTO typecheck_objects(object_key, object_kind)
                    VALUES(?1, ?2)
                    ON CONFLICT(object_key) DO UPDATE SET
                        object_kind = excluded.object_kind
                    ",
                )
                .map_err(db_err(&self.path))?;
            let mut delete_cols = tx
                .prepare("DELETE FROM typecheck_columns WHERE object_key = ?1")
                .map_err(db_err(&self.path))?;
            let mut insert_col = tx
                .prepare(
                    "INSERT INTO typecheck_columns(object_key, column_name, column_type, nullable, position)
                     VALUES(?1, ?2, ?3, ?4, ?5)",
                )
                .map_err(db_err(&self.path))?;

            for (key, kind, columns) in rows {
                upsert_obj
                    .execute(params![key, kind])
                    .map_err(db_err(&self.path))?;
                delete_cols.execute([key]).map_err(db_err(&self.path))?;
                for (col_name, col_type) in columns {
                    insert_col
                        .execute(params![
                            key,
                            col_name,
                            col_type.r#type,
                            i32::from(col_type.nullable),
                            i64::try_from(col_type.position).unwrap_or(0),
                        ])
                        .map_err(db_err(&self.path))?;
                }
            }
        }
        tx.commit().map_err(db_err(&self.path))
    }

    /// Remove stale typecheck artifacts for objects no longer in the current
    /// project.
    pub(crate) fn prune_typecheck_results(
        &mut self,
        keep: &BTreeSet<String>,
    ) -> Result<(), CacheError> {
        self.prune_rows(TYPECHECK_COLUMNS_TABLE, keep)?;
        self.prune_rows(TYPECHECK_OBJECTS_TABLE, keep)
    }

    /// Load every cached typecheck column row, grouped by object key.
    ///
    /// Single round-trip so the parallel typecheck DAG can run without
    /// holding a live SQLite connection.
    pub(crate) fn load_typecheck_columns(
        &self,
    ) -> Result<BTreeMap<String, BTreeMap<String, ColumnType>>, CacheError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT object_key, column_name, column_type, nullable, position \
                 FROM typecheck_columns",
            )
            .map_err(db_err(&self.path))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    ColumnType {
                        r#type: row.get(2)?,
                        nullable: row.get::<_, i32>(3)? != 0,
                        position: usize::try_from(row.get::<_, i64>(4)?).unwrap_or(0),
                        comment: None,
                    },
                ))
            })
            .map_err(db_err(&self.path))?;
        let mut out: BTreeMap<String, BTreeMap<String, ColumnType>> = BTreeMap::new();
        for row in rows {
            let (key, name, ty) = row.map_err(db_err(&self.path))?;
            out.entry(key).or_default().insert(name, ty);
        }
        Ok(out)
    }

    /// Load the digest map for external types (object_key -> digest).
    pub(crate) fn load_external_type_digests(
        &self,
    ) -> Result<BTreeMap<String, String>, CacheError> {
        let mut stmt = self
            .conn
            .prepare("SELECT object_key, digest FROM external_type_digest")
            .map_err(db_err(&self.path))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(db_err(&self.path))?;
        let mut out = BTreeMap::new();
        for row in rows {
            let (key, digest) = row.map_err(db_err(&self.path))?;
            out.insert(key, digest);
        }
        Ok(out)
    }

    /// Replace the cached external-type digest set with the provided rows and
    /// drop any rows for keys not present.
    pub(crate) fn replace_external_type_digests(
        &mut self,
        digests: &BTreeMap<String, String>,
    ) -> Result<(), CacheError> {
        let db_err = db_err(&self.path);
        let tx = self.conn.transaction().map_err(&db_err)?;
        tx.execute("DELETE FROM external_type_digest", [])
            .map_err(&db_err)?;
        {
            let mut stmt = tx
                .prepare("INSERT INTO external_type_digest(object_key, digest) VALUES(?1, ?2)")
                .map_err(&db_err)?;
            for (key, digest) in digests {
                stmt.execute(params![key, digest]).map_err(&db_err)?;
            }
        }
        tx.commit().map_err(&db_err)
    }

    /// Rewrites per-object rows for `changed_keys ∪ deleted_keys`; small
    /// project-wide tables are rewritten in full.
    pub(crate) fn write_project(
        &mut self,
        project: &graph::Project,
        changed_keys: &BTreeSet<String>,
        deleted_keys: &BTreeSet<String>,
        root: &Path,
    ) -> Result<(), CacheError> {
        const PER_OBJECT_TABLES: &[&str] = &[
            "project_objects",
            "project_dependencies",
            "project_comments",
            "project_indexes",
            "project_grants",
            "project_tests",
            "project_aliases",
            "project_infrastructure",
            "project_infrastructure_properties",
        ];

        let db_err = db_err(&self.path);

        let tx = self.conn.transaction().map_err(&db_err)?;

        if !changed_keys.is_empty() || !deleted_keys.is_empty() {
            for table in PER_OBJECT_TABLES {
                let mut stmt = tx
                    .prepare(&format!("DELETE FROM {table} WHERE object_key = ?1"))
                    .map_err(&db_err)?;
                for key in changed_keys.iter().chain(deleted_keys.iter()) {
                    stmt.execute(params![key]).map_err(&db_err)?;
                }
            }
        }

        tx.execute_batch(
            "
            DELETE FROM project_databases;
            DELETE FROM project_schemas;
            DELETE FROM project_external_dependencies;
            DELETE FROM project_cluster_dependencies;
            DELETE FROM project_replacement_schemas;
            DELETE FROM project_mod_statements;
            ",
        )
        .map_err(&db_err)?;

        {
            let mut stmts = ProjectStatements::new(&tx, &db_err)?;

            for db in &project.databases {
                stmts.ins_db.execute(params![&db.name]).map_err(&db_err)?;

                for schema in &db.schemas {
                    let schema_type = schema.schema_type.to_string();
                    stmts
                        .ins_schema
                        .execute(params![&db.name, &schema.name, schema_type.as_str()])
                        .map_err(&db_err)?;

                    for obj in &schema.objects {
                        if changed_keys.contains(&obj.id.to_string()) {
                            stmts.insert_object(obj, &db.name, &schema.name, root, &db_err)?;
                        }
                    }
                }

                stmts.insert_mod_statements(db, &db_err)?;
            }

            for ext_dep in &project.external_dependencies {
                stmts
                    .ins_ext_dep
                    .execute(params![ext_dep.to_string()])
                    .map_err(&db_err)?;
            }
            for cluster in &project.cluster_dependencies {
                stmts
                    .ins_cluster_dep
                    .execute(params![&cluster.name])
                    .map_err(&db_err)?;
            }
            for rs in &project.replacement_schemas {
                stmts
                    .ins_repl_schema
                    .execute(params![&rs.database, &rs.schema])
                    .map_err(&db_err)?;
            }
        }

        tx.commit().map_err(&db_err)
    }

    fn load_row_keys(&self, table: &str) -> Result<BTreeSet<String>, CacheError> {
        let mut stmt = self
            .conn
            .prepare(&format!("SELECT object_key FROM {table}"))
            .map_err(db_err(&self.path))?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(db_err(&self.path))?;
        let mut keys = BTreeSet::new();
        for row in rows {
            keys.insert(row.map_err(db_err(&self.path))?);
        }
        Ok(keys)
    }

    fn prune_rows(&mut self, table: &str, keep: &BTreeSet<String>) -> Result<(), CacheError> {
        let existing = self.load_row_keys(table)?;
        let tx = self.conn.transaction().map_err(db_err(&self.path))?;
        {
            let mut stmt = tx
                .prepare(&format!("DELETE FROM {table} WHERE object_key = ?1"))
                .map_err(db_err(&self.path))?;
            for key in &existing {
                if !keep.contains(key) {
                    stmt.execute([key]).map_err(db_err(&self.path))?;
                }
            }
        }
        tx.commit().map_err(db_err(&self.path))
    }
}

/// Prepared INSERT statements for [`BuildArtifact::write_project`].
struct ProjectStatements<'tx> {
    ins_db: rusqlite::Statement<'tx>,
    ins_schema: rusqlite::Statement<'tx>,
    ins_obj: rusqlite::Statement<'tx>,
    ins_dep: rusqlite::Statement<'tx>,
    ins_comment: rusqlite::Statement<'tx>,
    ins_index: rusqlite::Statement<'tx>,
    ins_grant: rusqlite::Statement<'tx>,
    ins_test: rusqlite::Statement<'tx>,
    ins_infra: rusqlite::Statement<'tx>,
    ins_infra_prop: rusqlite::Statement<'tx>,
    ins_ext_dep: rusqlite::Statement<'tx>,
    ins_cluster_dep: rusqlite::Statement<'tx>,
    ins_repl_schema: rusqlite::Statement<'tx>,
    ins_alias: rusqlite::Statement<'tx>,
    ins_mod_stmt: rusqlite::Statement<'tx>,
}

impl<'tx> ProjectStatements<'tx> {
    fn new(
        tx: &'tx rusqlite::Transaction<'_>,
        db_err: &impl Fn(rusqlite::Error) -> CacheError,
    ) -> Result<Self, CacheError> {
        Ok(Self {
            ins_db: tx
                .prepare("INSERT INTO project_databases (name) VALUES (?1)")
                .map_err(db_err)?,
            ins_schema: tx
                .prepare("INSERT INTO project_schemas (database, name, schema_type) VALUES (?1, ?2, ?3)")
                .map_err(db_err)?,
            ins_obj: tx
                .prepare("INSERT INTO project_objects (object_key, database, schema, name, object_kind, cluster, file_path, sql_text) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)")
                .map_err(db_err)?,
            ins_dep: tx
                .prepare("INSERT INTO project_dependencies (object_key, dependency_key) VALUES (?1, ?2)")
                .map_err(db_err)?,
            ins_comment: tx
                .prepare("INSERT INTO project_comments (object_key, comment_type, target_column, comment_text, sql_text) VALUES (?1, ?2, ?3, ?4, ?5)")
                .map_err(db_err)?,
            ins_index: tx
                .prepare("INSERT INTO project_indexes (object_key, index_name, cluster, columns, sql_text) VALUES (?1, ?2, ?3, ?4, ?5)")
                .map_err(db_err)?,
            ins_grant: tx
                .prepare("INSERT INTO project_grants (object_key, privilege, grantee, sql_text) VALUES (?1, ?2, ?3, ?4)")
                .map_err(db_err)?,
            ins_test: tx
                .prepare("INSERT INTO project_tests (object_key, test_name, sql_text) VALUES (?1, ?2, ?3)")
                .map_err(db_err)?,
            ins_infra: tx
                .prepare("INSERT INTO project_infrastructure (object_key, infra_type, connector_type, connection_ref, source_ref, external_reference) VALUES (?1, ?2, ?3, ?4, ?5, ?6)")
                .map_err(db_err)?,
            ins_infra_prop: tx
                .prepare("INSERT INTO project_infrastructure_properties (object_key, property_key, property_value, secret_ref, object_ref) VALUES (?1, ?2, ?3, ?4, ?5)")
                .map_err(db_err)?,
            ins_ext_dep: tx
                .prepare("INSERT INTO project_external_dependencies (object_key) VALUES (?1)")
                .map_err(db_err)?,
            ins_cluster_dep: tx
                .prepare("INSERT INTO project_cluster_dependencies (cluster_name) VALUES (?1)")
                .map_err(db_err)?,
            ins_repl_schema: tx
                .prepare("INSERT INTO project_replacement_schemas (database, schema) VALUES (?1, ?2)")
                .map_err(db_err)?,
            ins_alias: tx
                .prepare("INSERT INTO project_aliases (object_key, alias, target_fqn) VALUES (?1, ?2, ?3)")
                .map_err(db_err)?,
            ins_mod_stmt: tx
                .prepare("INSERT INTO project_mod_statements (database, schema, position, sql_text) VALUES (?1, ?2, ?3, ?4)")
                .map_err(db_err)?,
        })
    }

    fn insert_object(
        &mut self,
        obj: &graph::DatabaseObject,
        db_name: &str,
        schema_name: &str,
        root: &Path,
        db_err: &impl Fn(rusqlite::Error) -> CacheError,
    ) -> Result<(), CacheError> {
        let object_key = obj.id.to_string();
        let typed = &obj.typed_object;
        let kind = typed.stmt.kind().as_str();
        let cluster = statement_cluster(&typed.stmt);
        let file_path = typed
            .path
            .strip_prefix(root)
            .unwrap_or(&typed.path)
            .to_string_lossy()
            .to_string();
        let sql_text = format!("{};", typed.stmt);

        self.ins_obj
            .execute(params![
                &object_key,
                db_name,
                schema_name,
                obj.id.object(),
                kind,
                &cluster,
                &file_path,
                &sql_text,
            ])
            .map_err(db_err)?;

        for dep in &obj.dependencies {
            self.ins_dep
                .execute(params![&object_key, dep.to_string()])
                .map_err(db_err)?;
        }

        for comment in &typed.comments {
            let (comment_type, target_column) = match &comment.object {
                CommentObjectType::Column { name } => ("column", Some(name.column.to_string())),
                _ => ("object", None),
            };
            let comment_text = comment.comment.as_deref().unwrap_or("");
            let comment_sql = format!("{};", comment);
            self.ins_comment
                .execute(params![
                    &object_key,
                    comment_type,
                    &target_column,
                    comment_text,
                    &comment_sql,
                ])
                .map_err(db_err)?;
        }

        for idx in &typed.indexes {
            let index_name = idx.name.as_ref().map(|n| n.to_string()).unwrap_or_default();
            let idx_cluster = idx.in_cluster.as_ref().map(|c| c.to_string());
            let columns_str = idx
                .key_parts
                .as_ref()
                .map(|parts| {
                    parts
                        .iter()
                        .map(|p| p.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            let idx_sql = format!("{};", idx);
            self.ins_index
                .execute(params![
                    &object_key,
                    &index_name,
                    &idx_cluster,
                    &columns_str,
                    &idx_sql,
                ])
                .map_err(db_err)?;
        }

        for grant in &typed.grants {
            let privilege = grant.privileges.to_string();
            let grantee = grant
                .roles
                .iter()
                .map(|r| r.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let grant_sql = format!("{};", grant);
            self.ins_grant
                .execute(params![&object_key, &privilege, &grantee, &grant_sql])
                .map_err(db_err)?;
        }

        for test in &typed.tests {
            let test_name = test.name.to_string();
            let test_sql = format!("{};", test);
            self.ins_test
                .execute(params![&object_key, &test_name, &test_sql])
                .map_err(db_err)?;
        }

        if let Some(infra) = infrastructure::extract(&typed.stmt) {
            self.insert_infrastructure(&object_key, &infra, db_err)?;
        }

        let aliases = extract_alias_map(&typed.stmt, db_name, schema_name);
        for (alias, target_fqn) in &aliases {
            self.ins_alias
                .execute(params![&object_key, alias, target_fqn])
                .map_err(db_err)?;
        }

        Ok(())
    }

    fn insert_infrastructure(
        &mut self,
        object_key: &str,
        infra: &Infrastructure,
        db_err: &impl Fn(rusqlite::Error) -> CacheError,
    ) -> Result<(), CacheError> {
        let (infra_type, connector_type, connection_ref, source_ref, external_reference) =
            match infra {
                Infrastructure::Connection { connector_type, .. } => (
                    "connection",
                    Some(connector_type.as_str()),
                    None,
                    None,
                    None,
                ),
                Infrastructure::Source {
                    connector_type,
                    connection_ref,
                    ..
                } => (
                    "source",
                    Some(connector_type.as_str()),
                    connection_ref.as_deref(),
                    None,
                    None,
                ),
                Infrastructure::TableFromSource {
                    source_ref,
                    external_reference,
                } => (
                    "table-from-source",
                    None,
                    None,
                    Some(source_ref.as_str()),
                    external_reference.as_deref(),
                ),
            };

        self.ins_infra
            .execute(params![
                object_key,
                infra_type,
                connector_type,
                connection_ref,
                source_ref,
                external_reference,
            ])
            .map_err(db_err)?;

        let properties = match infra {
            Infrastructure::Connection { properties, .. }
            | Infrastructure::Source { properties, .. } => properties.as_slice(),
            Infrastructure::TableFromSource { .. } => &[],
        };
        for prop in properties {
            self.ins_infra_prop
                .execute(params![
                    object_key,
                    &prop.key,
                    &prop.value,
                    &prop.secret_ref,
                    &prop.object_ref,
                ])
                .map_err(db_err)?;
        }

        Ok(())
    }

    fn insert_mod_statements(
        &mut self,
        db: &graph::Database,
        db_err: &impl Fn(rusqlite::Error) -> CacheError,
    ) -> Result<(), CacheError> {
        if let Some(stmts) = &db.mod_statements {
            for (pos, stmt) in stmts.iter().enumerate() {
                self.ins_mod_stmt
                    .execute(params![
                        &db.name,
                        Option::<String>::None,
                        i64::try_from(pos).unwrap_or(0),
                        format!("{};", stmt),
                    ])
                    .map_err(db_err)?;
            }
        }

        for schema in &db.schemas {
            if let Some(stmts) = &schema.mod_statements {
                for (pos, stmt) in stmts.iter().enumerate() {
                    self.ins_mod_stmt
                        .execute(params![
                            &db.name,
                            Some(&schema.name),
                            i64::try_from(pos).unwrap_or(0),
                            format!("{};", stmt),
                        ])
                        .map_err(db_err)?;
                }
            }
        }

        Ok(())
    }
}

fn file_metadata_signature(path: &Path) -> Result<(i64, i64), CacheError> {
    let metadata = fs::metadata(path).map_err(|source| CacheError::FileReadFailed {
        path: path.to_path_buf(),
        source,
    })?;
    let size = i64::try_from(metadata.len()).unwrap_or(i64::MAX);
    let modified = metadata
        .modified()
        .map_err(|source| CacheError::FileReadFailed {
            path: path.to_path_buf(),
            source,
        })?;
    let duration =
        modified
            .duration_since(UNIX_EPOCH)
            .map_err(|source| CacheError::FileReadFailed {
                path: path.to_path_buf(),
                source: std::io::Error::other(source),
            })?;
    // File mtimes are an advisory cache key; saturate if the platform
    // reports a nanosecond value larger than the on-disk schema stores.
    let mtime_ns = i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX);
    Ok((size, mtime_ns))
}

/// Extract the cluster name from a statement's `IN CLUSTER` clause, if present.
fn statement_cluster(stmt: &Statement) -> Option<String> {
    use crate::project::ast::Statement;

    let in_cluster = match stmt {
        Statement::CreateMaterializedView(mv) => mv.in_cluster.as_ref(),
        Statement::CreateSource(source) => source.in_cluster.as_ref(),
        Statement::CreateSink(sink) => sink.in_cluster.as_ref(),
        Statement::CreateView(_)
        | Statement::CreateTable(_)
        | Statement::CreateTableFromSource(_)
        | Statement::CreateSecret(_)
        | Statement::CreateConnection(_) => None,
    };

    match in_cluster {
        Some(RawClusterName::Unresolved(ident)) => Some(ident.to_string()),
        _ => None,
    }
}

/// An object compilation artifact to be written to `object_state`.
#[derive(Debug, Clone)]
pub(crate) struct ObjectStateRow {
    /// Logical object identifier (`database.schema.object`).
    pub object_key: String,
    /// Composite hash of the object key, variant paths, content hashes, and
    /// compile-time variables. Used to detect cache staleness.
    pub fingerprint: String,
    pub artifact: CompiledObjectArtifact,
}

/// AST visitor that collects FROM-clause table aliases. Only direct table
/// references; derived subqueries and table functions are skipped.
struct AliasVisitor<'a> {
    default_db: &'a str,
    default_schema: &'a str,
    aliases: BTreeMap<String, String>,
    cte_scope: CteScope,
}

impl<'a> AliasVisitor<'a> {
    fn new(default_db: &'a str, default_schema: &'a str) -> Self {
        Self {
            default_db,
            default_schema,
            aliases: BTreeMap::new(),
            cte_scope: CteScope::new(),
        }
    }
}

impl<'ast> Visit<'ast, Raw> for AliasVisitor<'_> {
    fn visit_query(&mut self, node: &'ast mz_sql_parser::ast::Query<Raw>) {
        let names = CteScope::collect_cte_names(&node.ctes);
        self.cte_scope.push(names);
        visit::visit_query(self, node);
        self.cte_scope.pop();
    }

    fn visit_table_factor(&mut self, node: &'ast TableFactor<Raw>) {
        match node {
            TableFactor::Table { name, alias } => {
                let unresolved = name.name();
                if unresolved.0.len() == 1 && self.cte_scope.is_cte(&unresolved.0[0].to_string()) {
                    return;
                }
                let obj_id =
                    ObjectId::from_raw_item_name(name, self.default_db, self.default_schema);
                let fqn = obj_id.to_string();
                if let Some(bare) = unresolved.0.last().map(|i| i.to_string().to_lowercase()) {
                    self.aliases.insert(bare, fqn.clone());
                }
                if let Some(alias) = alias {
                    self.aliases
                        .insert(alias.name.to_string().to_lowercase(), fqn);
                }
            }
            TableFactor::NestedJoin { .. } => {
                visit::visit_table_factor(self, node);
            }
            // Don't recurse into subqueries or table functions for alias collection
            TableFactor::Derived { .. }
            | TableFactor::Function { .. }
            | TableFactor::RowsFrom { .. } => {}
        }
    }
}

/// Extract alias → fully-qualified name map from a statement's query body.
///
/// Only `CreateView` and `CreateMaterializedView` produce aliases.
/// All keys are lowercased for case-insensitive lookup. CTE references
/// are excluded from the alias map.
pub(crate) fn extract_alias_map(
    stmt: &Statement,
    default_db: &str,
    default_schema: &str,
) -> BTreeMap<String, String> {
    let mut visitor = AliasVisitor::new(default_db, default_schema);
    match stmt {
        Statement::CreateView(s) => {
            visitor.visit_query(&s.definition.query);
        }
        Statement::CreateMaterializedView(s) => {
            visitor.visit_query(&s.query);
        }
        _ => {}
    }
    visitor.aliases
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn open_db(root: &Path) -> BuildArtifact {
        BuildArtifact::open(root, "default", None, &BTreeMap::new()).unwrap()
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn load_file_contents_treats_null_contents_as_miss() {
        let temp = tempdir().unwrap();
        let file = temp.path().join("model.sql");
        fs::write(&file, "CREATE VIEW v AS SELECT 1;").unwrap();

        let mut db = open_db(temp.path());
        let paths = BTreeSet::from([file.clone()]);
        let fs = crate::fs::FileSystem::new();
        db.load_file_hashes(&fs, &paths).unwrap();
        db.conn
            .execute(
                "UPDATE file_state SET contents = NULL WHERE path = ?1",
                [file.to_string_lossy().to_string()],
            )
            .unwrap();

        let entries = db.load_file_contents(&fs, &paths).unwrap();
        assert_eq!(
            entries.get(&file).map(String::as_str),
            Some("CREATE VIEW v AS SELECT 1;")
        );

        let repaired: Option<String> = db
            .conn
            .query_row(
                "SELECT contents FROM file_state WHERE path = ?1",
                [file.to_string_lossy().to_string()],
                |row| row.get(0),
            )
            .optional()
            .unwrap();
        assert_eq!(repaired.as_deref(), Some("CREATE VIEW v AS SELECT 1;"));
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn prune_rows_removes_stale_object_and_typecheck_entries() {
        let temp = tempdir().unwrap();
        let mut db = open_db(temp.path());
        db.upsert_object_rows(&[
            ObjectStateRow {
                object_key: "db.public.keep".into(),
                fingerprint: "keep".into(),
                artifact: CompiledObjectArtifact::Skipped,
            },
            ObjectStateRow {
                object_key: "db.public.drop".into(),
                fingerprint: "drop".into(),
                artifact: CompiledObjectArtifact::Skipped,
            },
        ])
        .unwrap();
        db.upsert_typecheck_results(&[
            ("db.public.keep".into(), "view".into(), BTreeMap::new()),
            ("db.public.drop".into(), "view".into(), BTreeMap::new()),
        ])
        .unwrap();

        let keep = BTreeSet::from([String::from("db.public.keep")]);
        db.prune_object_rows(&keep).unwrap();
        db.prune_typecheck_results(&keep).unwrap();

        let object_keys = db.load_row_keys(OBJECT_STATE_TABLE).unwrap();
        let typecheck_keys = db.load_row_keys(TYPECHECK_OBJECTS_TABLE).unwrap();
        assert_eq!(object_keys, keep);
        assert_eq!(typecheck_keys, keep);
    }

    /// Helper: parse SQL into a [`Statement`] for test construction.
    fn parse_stmt(sql: &str) -> Statement {
        let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();
        match parsed.into_iter().next().unwrap().ast {
            mz_sql_parser::ast::Statement::CreateView(s) => Statement::CreateView(s),
            mz_sql_parser::ast::Statement::CreateMaterializedView(s) => {
                Statement::CreateMaterializedView(s)
            }
            mz_sql_parser::ast::Statement::CreateTable(s) => Statement::CreateTable(s),
            other => panic!("Unexpected statement type: {:?}", other),
        }
    }

    fn all_object_keys(project: &graph::Project) -> BTreeSet<String> {
        project
            .databases
            .iter()
            .flat_map(|db| {
                db.schemas
                    .iter()
                    .flat_map(|s| s.objects.iter().map(|o| o.id.to_string()))
            })
            .collect()
    }

    fn make_project(db_name: &str, schema_name: &str, stmt: Statement) -> graph::Project {
        use crate::project::ir::compiled;

        let typed_obj = compiled::DatabaseObject {
            path: PathBuf::from("test.sql"),
            stmt,
            indexes: vec![],
            grants: vec![],
            comments: vec![],
            tests: vec![],
        };
        let obj_id = ObjectId::new(
            db_name.to_string(),
            schema_name.to_string(),
            typed_obj.stmt.ident().object.clone(),
        );
        let db_obj = graph::DatabaseObject {
            id: obj_id,
            typed_object: typed_obj,
            dependencies: BTreeSet::new(),
        };
        graph::Project {
            databases: vec![graph::Database {
                name: db_name.to_string(),
                schemas: vec![graph::Schema {
                    name: schema_name.to_string(),
                    objects: vec![db_obj],
                    mod_statements: None,
                    schema_type: graph::SchemaType::Compute,
                }],
                mod_statements: None,
            }],
            dependency_graph: BTreeMap::new(),
            external_dependencies: BTreeSet::new(),
            cluster_dependencies: BTreeSet::new(),
            tests: vec![],
            replacement_schemas: BTreeSet::new(),
            compile_dirty: BTreeSet::new(),
        }
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn write_project_persists_aliases() {
        let temp = tempdir().unwrap();
        let mut db = open_db(temp.path());

        let stmt = parse_stmt("CREATE VIEW v AS SELECT o.id FROM orders AS o");
        let project = make_project("mydb", "public", stmt);
        let changed = all_object_keys(&project);
        db.write_project(&project, &changed, &BTreeSet::new(), temp.path())
            .unwrap();

        let rows: Vec<(String, String, String)> = db
            .conn
            .prepare("SELECT object_key, alias, target_fqn FROM project_aliases ORDER BY alias")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(rows.len(), 2);

        let o_row = rows.iter().find(|(_, alias, _)| alias == "o").unwrap();
        assert_eq!(o_row.0, "mydb.public.v");
        assert_eq!(o_row.2, "mydb.public.orders");

        let orders_row = rows.iter().find(|(_, alias, _)| alias == "orders").unwrap();
        assert_eq!(orders_row.0, "mydb.public.v");
        assert_eq!(orders_row.2, "mydb.public.orders");
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn write_project_no_aliases_for_table_stmt() {
        let temp = tempdir().unwrap();
        let mut db = open_db(temp.path());

        let stmt = parse_stmt("CREATE TABLE t (id INT)");
        let project = make_project("mydb", "public", stmt);
        let changed = all_object_keys(&project);
        db.write_project(&project, &changed, &BTreeSet::new(), temp.path())
            .unwrap();

        let count: i64 = db
            .conn
            .query_row("SELECT COUNT(*) FROM project_aliases", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }
}
