use std::cmp;
use std::io::{self, Read};

use byteorder::{BigEndian, ReadBytesExt};
use bytes::Bytes;
use memchr::memchr;
use postgres_protocol::{Lsn, Oid};

// replication message tags
pub const XLOG_DATA_TAG: u8 = b'w';
pub const PRIMARY_KEEPALIVE_TAG: u8 = b'k';
pub const STANDBY_STATUS_UPDATE_TAG: u8 = b'r';
pub const HOT_STANDBY_FEEDBACK_TAG: u8 = b'h';

// logical replication message tags
pub const BEGIN_TAG: u8 = b'B';
pub const COMMIT_TAG: u8 = b'C';
pub const ORIGIN_TAG: u8 = b'O';
pub const RELATION_TAG: u8 = b'R';
pub const TYPE_TAG: u8 = b'Y';
pub const INSERT_TAG: u8 = b'I';
pub const UPDATE_TAG: u8 = b'U';
pub const DELETE_TAG: u8 = b'D';
pub const TRUNCATE_TAG: u8 = b'T';
pub const TUPLE_NEW_TAG: u8 = b'N';
pub const TUPLE_KEY_TAG: u8 = b'K';
pub const TUPLE_OLD_TAG: u8 = b'O';
pub const TUPLE_DATA_NULL_TAG: u8 = b'n';
pub const TUPLE_DATA_TOAST_TAG: u8 = b'u';
pub const TUPLE_DATA_TEXT_TAG: u8 = b't';

// replica identity tags
pub const REPLICA_IDENTITY_DEFAULT_TAG: u8 = b'd';
pub const REPLICA_IDENTITY_NOTHING_TAG: u8 = b'n';
pub const REPLICA_IDENTITY_FULL_TAG: u8 = b'f';
pub const REPLICA_IDENTITY_INDEX_TAG: u8 = b'i';

/// An enum representing Postgres backend replication messages.
#[non_exhaustive]
#[derive(Debug)]
pub enum ReplicationMessage<D> {
    XLogData(XLogDataBody<D>),
    PrimaryKeepAlive(PrimaryKeepAliveBody),
}

impl ReplicationMessage<Bytes> {
    #[inline]
    pub fn parse(buf: &Bytes) -> io::Result<Self> {
        let mut buf = Buffer {
            bytes: buf.clone(),
            idx: 0,
        };

        let tag = buf.read_u8()?;

        let replication_message = match tag {
            XLOG_DATA_TAG => {
                let wal_start = buf.read_u64::<BigEndian>()?;
                let wal_end = buf.read_u64::<BigEndian>()?;
                let timestamp = buf.read_i64::<BigEndian>()?;
                let data = buf.read_all();
                ReplicationMessage::XLogData(XLogDataBody {
                    wal_start,
                    wal_end,
                    timestamp,
                    data,
                })
            }
            PRIMARY_KEEPALIVE_TAG => {
                let wal_end = buf.read_u64::<BigEndian>()?;
                let timestamp = buf.read_i64::<BigEndian>()?;
                let reply = buf.read_u8()?;
                ReplicationMessage::PrimaryKeepAlive(PrimaryKeepAliveBody {
                    wal_end,
                    timestamp,
                    reply,
                })
            }
            tag => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown replication message tag `{}`", tag),
                ));
            }
        };

        Ok(replication_message)
    }
}

#[derive(Debug)]
pub struct XLogDataBody<D> {
    wal_start: u64,
    wal_end: u64,
    timestamp: i64,
    data: D,
}

impl<D> XLogDataBody<D> {
    #[inline]
    pub fn wal_start(&self) -> u64 {
        self.wal_start
    }

    #[inline]
    pub fn wal_end(&self) -> u64 {
        self.wal_end
    }

    #[inline]
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    #[inline]
    pub fn data(&self) -> &D {
        &self.data
    }

    #[inline]
    pub fn into_data(self) -> D {
        self.data
    }

    pub fn map_data<F, D2, E>(self, f: F) -> Result<XLogDataBody<D2>, E>
    where
        F: Fn(D) -> Result<D2, E>,
    {
        let data = f(self.data)?;
        Ok(XLogDataBody {
            wal_start: self.wal_start,
            wal_end: self.wal_end,
            timestamp: self.timestamp,
            data,
        })
    }
}

#[derive(Debug)]
pub struct PrimaryKeepAliveBody {
    wal_end: u64,
    timestamp: i64,
    reply: u8,
}

impl PrimaryKeepAliveBody {
    #[inline]
    pub fn wal_end(&self) -> u64 {
        self.wal_end
    }

    #[inline]
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    #[inline]
    pub fn reply(&self) -> u8 {
        self.reply
    }
}

#[non_exhaustive]
/// A message of the logical replication stream
#[derive(Debug)]
pub enum LogicalReplicationMessage {
    /// A BEGIN statement
    Begin(BeginBody),
    /// A BEGIN statement
    Commit(CommitBody),
    /// An Origin replication message
    /// Note that there can be multiple Origin messages inside a single transaction.
    Origin(OriginBody),
    /// A Relation replication message
    Relation(RelationBody),
    /// A Type replication message
    Type(TypeBody),
    /// An INSERT statement
    Insert(InsertBody),
    /// An UPDATE statement
    Update(UpdateBody),
    /// A DELETE statement
    Delete(DeleteBody),
    /// A TRUNCATE statement
    Truncate(TruncateBody),
}

impl LogicalReplicationMessage {
    pub fn parse(buf: &Bytes) -> io::Result<Self> {
        let mut buf = Buffer {
            bytes: buf.clone(),
            idx: 0,
        };

        let tag = buf.read_u8()?;

        let logical_replication_message = match tag {
            BEGIN_TAG => Self::Begin(BeginBody {
                final_lsn: buf.read_u64::<BigEndian>()?,
                timestamp: buf.read_i64::<BigEndian>()?,
                xid: buf.read_u32::<BigEndian>()?,
            }),
            COMMIT_TAG => Self::Commit(CommitBody {
                flags: buf.read_i8()?,
                commit_lsn: buf.read_u64::<BigEndian>()?,
                end_lsn: buf.read_u64::<BigEndian>()?,
                timestamp: buf.read_i64::<BigEndian>()?,
            }),
            ORIGIN_TAG => Self::Origin(OriginBody {
                commit_lsn: buf.read_u64::<BigEndian>()?,
                name: buf.read_cstr()?,
            }),
            RELATION_TAG => {
                let rel_id = buf.read_u32::<BigEndian>()?;
                let namespace = buf.read_cstr()?;
                let name = buf.read_cstr()?;
                let replica_identity = match buf.read_u8()? {
                    REPLICA_IDENTITY_DEFAULT_TAG => ReplicaIdentity::Default,
                    REPLICA_IDENTITY_NOTHING_TAG => ReplicaIdentity::Nothing,
                    REPLICA_IDENTITY_FULL_TAG => ReplicaIdentity::Full,
                    REPLICA_IDENTITY_INDEX_TAG => ReplicaIdentity::Index,
                    tag => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("unknown replica identity tag `{}`", tag),
                        ));
                    }
                };
                let column_len = buf.read_i16::<BigEndian>()?;

                let mut columns =
                    Vec::with_capacity(usize::try_from(column_len).expect("negative length"));
                for _ in 0..column_len {
                    columns.push(Column::parse(&mut buf)?);
                }

                Self::Relation(RelationBody {
                    rel_id,
                    namespace,
                    name,
                    replica_identity,
                    columns,
                })
            }
            TYPE_TAG => Self::Type(TypeBody {
                id: buf.read_u32::<BigEndian>()?,
                namespace: buf.read_cstr()?,
                name: buf.read_cstr()?,
            }),
            INSERT_TAG => {
                let rel_id = buf.read_u32::<BigEndian>()?;
                let tag = buf.read_u8()?;

                let tuple = match tag {
                    TUPLE_NEW_TAG => Tuple::parse(&mut buf)?,
                    tag => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("unexpected tuple tag `{}`", tag),
                        ));
                    }
                };

                Self::Insert(InsertBody { rel_id, tuple })
            }
            UPDATE_TAG => {
                let rel_id = buf.read_u32::<BigEndian>()?;
                let tag = buf.read_u8()?;

                let mut key_tuple = None;
                let mut old_tuple = None;

                let new_tuple = match tag {
                    TUPLE_NEW_TAG => Tuple::parse(&mut buf)?,
                    TUPLE_OLD_TAG | TUPLE_KEY_TAG => {
                        if tag == TUPLE_OLD_TAG {
                            old_tuple = Some(Tuple::parse(&mut buf)?);
                        } else {
                            key_tuple = Some(Tuple::parse(&mut buf)?);
                        }

                        match buf.read_u8()? {
                            TUPLE_NEW_TAG => Tuple::parse(&mut buf)?,
                            tag => {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    format!("unexpected tuple tag `{}`", tag),
                                ));
                            }
                        }
                    }
                    tag => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("unknown tuple tag `{}`", tag),
                        ));
                    }
                };

                Self::Update(UpdateBody {
                    rel_id,
                    key_tuple,
                    old_tuple,
                    new_tuple,
                })
            }
            DELETE_TAG => {
                let rel_id = buf.read_u32::<BigEndian>()?;
                let tag = buf.read_u8()?;

                let mut key_tuple = None;
                let mut old_tuple = None;

                match tag {
                    TUPLE_OLD_TAG => old_tuple = Some(Tuple::parse(&mut buf)?),
                    TUPLE_KEY_TAG => key_tuple = Some(Tuple::parse(&mut buf)?),
                    tag => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("unknown tuple tag `{}`", tag),
                        ));
                    }
                }

                Self::Delete(DeleteBody {
                    rel_id,
                    key_tuple,
                    old_tuple,
                })
            }
            TRUNCATE_TAG => {
                let relation_len = buf.read_i32::<BigEndian>()?;
                let options = buf.read_i8()?;

                let mut rel_ids =
                    Vec::with_capacity(usize::try_from(relation_len).expect("negative length"));
                for _ in 0..relation_len {
                    rel_ids.push(buf.read_u32::<BigEndian>()?);
                }

                Self::Truncate(TruncateBody { options, rel_ids })
            }
            tag => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown replication message tag `{}`", tag),
                ));
            }
        };

        Ok(logical_replication_message)
    }
}

/// A row as it appears in the replication stream
#[derive(Debug)]
pub struct Tuple(Vec<TupleData>);

impl Tuple {
    #[inline]
    /// The tuple data of this tuple
    pub fn tuple_data(&self) -> &[TupleData] {
        &self.0
    }
}

impl Tuple {
    fn parse(buf: &mut Buffer) -> io::Result<Self> {
        let col_len = buf.read_i16::<BigEndian>()?;
        let mut tuple = Vec::with_capacity(usize::try_from(col_len).expect("negative length"));
        for _ in 0..col_len {
            tuple.push(TupleData::parse(buf)?);
        }

        Ok(Tuple(tuple))
    }
}

/// A column as it appears in the replication stream
#[derive(Debug)]
pub struct Column {
    flags: i8,
    name: Bytes,
    type_id: i32,
    type_modifier: i32,
}

impl Column {
    #[inline]
    /// Flags for the column. Currently can be either 0 for no flags or 1 which marks the column as
    /// part of the key.
    pub fn flags(&self) -> i8 {
        self.flags
    }

    #[inline]
    /// Name of the column.
    pub fn name(&self) -> io::Result<&str> {
        get_str(&self.name)
    }

    #[inline]
    /// ID of the column's data type.
    pub fn type_id(&self) -> i32 {
        self.type_id
    }

    #[inline]
    /// Type modifier of the column (`atttypmod`).
    pub fn type_modifier(&self) -> i32 {
        self.type_modifier
    }
}

impl Column {
    fn parse(buf: &mut Buffer) -> io::Result<Self> {
        Ok(Self {
            flags: buf.read_i8()?,
            name: buf.read_cstr()?,
            type_id: buf.read_i32::<BigEndian>()?,
            type_modifier: buf.read_i32::<BigEndian>()?,
        })
    }
}

/// The data of an individual column as it appears in the replication stream
#[derive(Debug)]
pub enum TupleData {
    /// Represents a NULL value
    Null,
    /// Represents an unchanged TOASTed value (the actual value is not sent).
    UnchangedToast,
    /// Column data as text formatted value.
    Text(Bytes),
}

impl TupleData {
    fn parse(buf: &mut Buffer) -> io::Result<Self> {
        let type_tag = buf.read_u8()?;

        let tuple = match type_tag {
            TUPLE_DATA_NULL_TAG => TupleData::Null,
            TUPLE_DATA_TOAST_TAG => TupleData::UnchangedToast,
            TUPLE_DATA_TEXT_TAG => {
                let len = buf.read_i32::<BigEndian>()?;
                let mut data = vec![0; usize::try_from(len).expect("negative length")];
                buf.read_exact(&mut data)?;
                TupleData::Text(data.into())
            }
            tag => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown replication message tag `{}`", tag),
                ));
            }
        };

        Ok(tuple)
    }
}

/// A BEGIN statement
#[derive(Debug)]
pub struct BeginBody {
    final_lsn: u64,
    timestamp: i64,
    xid: u32,
}

impl BeginBody {
    #[inline]
    /// Gets the final lsn of the transaction
    pub fn final_lsn(&self) -> Lsn {
        self.final_lsn
    }

    #[inline]
    /// Commit timestamp of the transaction. The value is in number of microseconds since PostgreSQL epoch (2000-01-01).
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    #[inline]
    /// Xid of the transaction.
    pub fn xid(&self) -> u32 {
        self.xid
    }
}

/// A COMMIT statement
#[derive(Debug)]
pub struct CommitBody {
    flags: i8,
    commit_lsn: u64,
    end_lsn: u64,
    timestamp: i64,
}

impl CommitBody {
    #[inline]
    /// The LSN of the commit.
    pub fn commit_lsn(&self) -> Lsn {
        self.commit_lsn
    }

    #[inline]
    /// The end LSN of the transaction.
    pub fn end_lsn(&self) -> Lsn {
        self.end_lsn
    }

    #[inline]
    /// Commit timestamp of the transaction. The value is in number of microseconds since PostgreSQL epoch (2000-01-01).
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    #[inline]
    /// Flags; currently unused (will be 0).
    pub fn flags(&self) -> i8 {
        self.flags
    }
}

/// An Origin replication message
///
/// Note that there can be multiple Origin messages inside a single transaction.
#[derive(Debug)]
pub struct OriginBody {
    commit_lsn: u64,
    name: Bytes,
}

impl OriginBody {
    #[inline]
    /// The LSN of the commit on the origin server.
    pub fn commit_lsn(&self) -> Lsn {
        self.commit_lsn
    }

    #[inline]
    /// Name of the origin.
    pub fn name(&self) -> io::Result<&str> {
        get_str(&self.name)
    }
}

/// Describes the REPLICA IDENTITY setting of a table
#[derive(Debug)]
pub enum ReplicaIdentity {
    /// default selection for replica identity (primary key or nothing)
    Default,
    /// no replica identity is logged for this relation
    Nothing,
    /// all columns are logged as replica identity
    Full,
    /// An explicitly chosen candidate key's columns are used as replica identity.
    /// Note this will still be set if the index has been dropped; in that case it
    /// has the same meaning as 'd'.
    Index,
}

/// A Relation replication message
#[derive(Debug)]
pub struct RelationBody {
    rel_id: u32,
    namespace: Bytes,
    name: Bytes,
    replica_identity: ReplicaIdentity,
    columns: Vec<Column>,
}

impl RelationBody {
    #[inline]
    /// ID of the relation.
    pub fn rel_id(&self) -> u32 {
        self.rel_id
    }

    #[inline]
    /// Namespace (empty string for pg_catalog).
    pub fn namespace(&self) -> io::Result<&str> {
        get_str(&self.namespace)
    }

    #[inline]
    /// Relation name.
    pub fn name(&self) -> io::Result<&str> {
        get_str(&self.name)
    }

    #[inline]
    /// Replica identity setting for the relation
    pub fn replica_identity(&self) -> &ReplicaIdentity {
        &self.replica_identity
    }

    #[inline]
    /// The column definitions of this relation
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }
}

/// A Type replication message
#[derive(Debug)]
pub struct TypeBody {
    id: u32,
    namespace: Bytes,
    name: Bytes,
}

impl TypeBody {
    #[inline]
    /// ID of the data type.
    pub fn id(&self) -> Oid {
        self.id
    }

    #[inline]
    /// Namespace (empty string for pg_catalog).
    pub fn namespace(&self) -> io::Result<&str> {
        get_str(&self.namespace)
    }

    #[inline]
    /// Name of the data type.
    pub fn name(&self) -> io::Result<&str> {
        get_str(&self.name)
    }
}

/// An INSERT statement
#[derive(Debug)]
pub struct InsertBody {
    rel_id: u32,
    tuple: Tuple,
}

impl InsertBody {
    #[inline]
    /// ID of the relation corresponding to the ID in the relation message.
    pub fn rel_id(&self) -> u32 {
        self.rel_id
    }

    #[inline]
    /// The inserted tuple
    pub fn tuple(&self) -> &Tuple {
        &self.tuple
    }
}

/// An UPDATE statement
#[derive(Debug)]
pub struct UpdateBody {
    rel_id: u32,
    old_tuple: Option<Tuple>,
    key_tuple: Option<Tuple>,
    new_tuple: Tuple,
}

impl UpdateBody {
    #[inline]
    /// ID of the relation corresponding to the ID in the relation message.
    pub fn rel_id(&self) -> u32 {
        self.rel_id
    }

    #[inline]
    /// This field is optional and is only present if the update changed data in any of the
    /// column(s) that are part of the REPLICA IDENTITY index.
    pub fn key_tuple(&self) -> Option<&Tuple> {
        self.key_tuple.as_ref()
    }

    #[inline]
    /// This field is optional and is only present if table in which the update happened has
    /// REPLICA IDENTITY set to FULL.
    pub fn old_tuple(&self) -> Option<&Tuple> {
        self.old_tuple.as_ref()
    }

    #[inline]
    /// The new tuple
    pub fn new_tuple(&self) -> &Tuple {
        &self.new_tuple
    }
}

/// A DELETE statement
#[derive(Debug)]
pub struct DeleteBody {
    rel_id: u32,
    old_tuple: Option<Tuple>,
    key_tuple: Option<Tuple>,
}

impl DeleteBody {
    #[inline]
    /// ID of the relation corresponding to the ID in the relation message.
    pub fn rel_id(&self) -> u32 {
        self.rel_id
    }

    #[inline]
    /// This field is present if the table in which the delete has happened uses an index as
    /// REPLICA IDENTITY.
    pub fn key_tuple(&self) -> Option<&Tuple> {
        self.key_tuple.as_ref()
    }

    #[inline]
    /// This field is present if the table in which the delete has happened has REPLICA IDENTITY
    /// set to FULL.
    pub fn old_tuple(&self) -> Option<&Tuple> {
        self.old_tuple.as_ref()
    }
}

/// A TRUNCATE statement
#[derive(Debug)]
pub struct TruncateBody {
    options: i8,
    rel_ids: Vec<u32>,
}

impl TruncateBody {
    #[inline]
    /// The IDs of the relations corresponding to the ID in the relation messages
    pub fn rel_ids(&self) -> &[u32] {
        &self.rel_ids
    }

    #[inline]
    /// Option bits for TRUNCATE: 1 for CASCADE, 2 for RESTART IDENTITY
    pub fn options(&self) -> i8 {
        self.options
    }
}

struct Buffer {
    bytes: Bytes,
    idx: usize,
}

impl Buffer {
    #[inline]
    fn slice(&self) -> &[u8] {
        &self.bytes[self.idx..]
    }

    #[inline]
    fn read_cstr(&mut self) -> io::Result<Bytes> {
        match memchr(0, self.slice()) {
            Some(pos) => {
                let start = self.idx;
                let end = start + pos;
                let cstr = self.bytes.slice(start..end);
                self.idx = end + 1;
                Ok(cstr)
            }
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF",
            )),
        }
    }

    #[inline]
    fn read_all(&mut self) -> Bytes {
        let buf = self.bytes.slice(self.idx..);
        self.idx = self.bytes.len();
        buf
    }
}

impl Read for Buffer {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = {
            let slice = self.slice();
            let len = cmp::min(slice.len(), buf.len());
            buf[..len].copy_from_slice(&slice[..len]);
            len
        };
        self.idx += len;
        Ok(len)
    }
}

#[inline]
fn get_str(buf: &[u8]) -> io::Result<&str> {
    std::str::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))
}
