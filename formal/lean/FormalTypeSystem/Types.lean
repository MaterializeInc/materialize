inductive ScalarBaseType where
  | Bool | Int16 | Int32 | Int64 | UInt16 | UInt32 | UInt64
  | Float32 | Float64 | Numeric | Date | Time | Timestamp | TimestampTz
  | Interval | PgLegacyChar | PgLegacyName | Bytes | String | Char | VarChar
  | Jsonb | Uuid | Array | List | Record | Oid | Map
  | RegProc | RegType | RegClass | Int2Vector | MzTimestamp | Range
  | MzAclItem | AclItem
  deriving DecidableEq, Repr, Inhabited, BEq, Hashable

inductive CastContext where
  | Implicit | Assignment | Explicit | Coerced
  deriving DecidableEq, Repr, Inhabited, BEq

structure CastEntry where
  from : ScalarBaseType
  to : ScalarBaseType
  ctx : CastContext
  deriving Repr, BEq

def allTypes : List ScalarBaseType :=
  [.Bool, .Int16, .Int32, .Int64, .UInt16, .UInt32, .UInt64,
   .Float32, .Float64, .Numeric, .Date, .Time, .Timestamp, .TimestampTz,
   .Interval, .PgLegacyChar, .PgLegacyName, .Bytes, .String, .Char, .VarChar,
   .Jsonb, .Uuid, .Array, .List, .Record, .Oid, .Map, .RegProc, .RegType,
   .RegClass, .Int2Vector, .MzTimestamp, .Range, .MzAclItem, .AclItem]
