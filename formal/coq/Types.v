(** * Materialize Scalar Base Types *)

Require Import Coq.Lists.List.
Require Import Coq.Bool.Bool.
Import ListNotations.

Inductive ScalarBaseType : Set :=
  | Bool | Int16 | Int32 | Int64 | UInt16 | UInt32 | UInt64
  | Float32 | Float64 | Numeric | Date | Time | Timestamp | TimestampTz
  | Interval | PgLegacyChar | PgLegacyName | Bytes | String | Char | VarChar
  | Jsonb | Uuid | Array | List | Record | Oid | Map
  | RegProc | RegType | RegClass | Int2Vector | MzTimestamp | Range
  | MzAclItem | AclItem.

Scheme Equality for ScalarBaseType.

Inductive CastContext : Set :=
  | Implicit | Assignment | Explicit | Coerced.

Scheme Equality for CastContext.

Definition all_types : list ScalarBaseType :=
  [ Bool; Int16; Int32; Int64; UInt16; UInt32; UInt64;
    Float32; Float64; Numeric; Date; Time; Timestamp; TimestampTz;
    Interval; PgLegacyChar; PgLegacyName; Bytes; String; Char; VarChar;
    Jsonb; Uuid; Array; List; Record; Oid; Map; RegProc; RegType;
    RegClass; Int2Vector; MzTimestamp; Range; MzAclItem; AclItem ].

Record CastEntry := mkCast {
  cast_from : ScalarBaseType;
  cast_to : ScalarBaseType;
  cast_ctx : CastContext;
}.
