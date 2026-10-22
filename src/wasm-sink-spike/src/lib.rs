//! THROWAWAY spike library: WASM sink guest runner and Arrow encoding, shared
//! by the standalone host binary and the clusterd sink operator.

pub mod guest;

pub mod encode {
    use arrow::ipc::writer::StreamWriter;
    use mz_arrow_util::builder::ArrowBuilder;
    use mz_repr::{Datum, RelationDesc, Row, SqlScalarType, Timestamp};

pub struct Encoder {
    desc: RelationDesc,
}

impl Encoder {
    pub fn new(desc: &RelationDesc) -> Self {
        let mut b = RelationDesc::builder();
        for (name, ty) in desc.iter() {
            b = b.with_column(name.clone(), ty.clone());
        }
        let desc = b
            .with_column("mz_time", SqlScalarType::UInt64.nullable(false))
            .with_column("mz_diff", SqlScalarType::Int64.nullable(false))
            .finish();
        Encoder { desc }
    }

    pub fn encode(&self, updates: &[(Row, Timestamp, i64)]) -> anyhow::Result<Vec<u8>> {
        let mut builder = ArrowBuilder::new(&self.desc, updates.len(), updates.len() * 32)?;
        let mut row = Row::default();
        for (r, t, d) in updates {
            let mut p = row.packer();
            p.extend(r.iter());
            p.push(Datum::UInt64(u64::from(*t)));
            p.push(Datum::Int64(*d));
            builder.add_row(&row)?;
        }
        let batch = builder.to_record_batch()?;
        let mut out = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut out, &batch.schema())?;
            w.write(&batch)?;
            w.finish()?;
        }
        Ok(out)
    }
}

}
