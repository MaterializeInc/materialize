mod avro;
mod csv;
use self::csv::csv;
use avro::avro;

use dataflow_types::{DataEncoding, Diff, Timestamp};
use repr::Row;
use timely::dataflow::{Scope, Stream};

pub fn decode<G>(
    stream: &Stream<G, Vec<u8>>,
    encoding: DataEncoding,
) -> Stream<G, (Row, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    match encoding {
        DataEncoding::Csv(enc) => csv(stream, enc.n_cols),
        DataEncoding::Avro(enc) => avro(stream, &enc.raw_schema, enc.schema_registry_url),
    }
}
