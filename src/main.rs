use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::{BufMut, Bytes, BytesMut};
use parquet::{
    basic::Compression,
    data_type::{ByteArray, ByteArrayType},
    file::{
        properties::WriterProperties, reader::FileReader, serialized_reader::SerializedFileReader,
        writer::SerializedFileWriter,
    },
    schema::parser::parse_message_type,
};

pub fn parse_schema(schema: &str) -> parquet::schema::types::Type {
    parse_message_type(schema).expect("Bad schema")
}

#[derive(Debug, Default)]
pub struct RepeatedWriter {
    values: Vec<ByteArray>,
    def_levels: Vec<i16>,
    rep_levels: Vec<i16>,
}

impl RepeatedWriter {
    fn new() -> Self {
        RepeatedWriter {
            values: Default::default(),
            def_levels: Default::default(),
            rep_levels: Default::default(),
        }
    }

    fn push<Iter: ExactSizeIterator<Item = T>, T>(&mut self, values: Iter)
    where
        T: Into<ByteArray>,
    {
        let num = values.len();
        if num == 0 {
            self.def_levels.push(0);
            self.rep_levels.push(0);
        } else {
            self.def_levels.resize(self.def_levels.len() + num, 1);
            self.rep_levels.push(0);
            self.rep_levels.resize(self.rep_levels.len() + num - 1, 1);
            self.values.extend(values.map(|val| val.into()));
        }
    }

    fn values(&self) -> &[ByteArray] {
        &self.values
    }

    fn def_levels(&self) -> Option<&[i16]> {
        Some(&self.def_levels)
    }

    fn rep_levels(&self) -> Option<&[i16]> {
        Some(&self.rep_levels)
    }
}

fn main() -> Result<()> {
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build(),
    );

    let schema = Arc::new(
        parse_message_type(
            "
message schema {
    REQUIRED GROUP names (LIST) {
        REPEATED GROUP list {
            REQUIRED BYTE_ARRAY list_element (UTF8);
        }
    }
}
        ",
        )
        .unwrap(),
    );

    let bytes = create_small_parquet_file(Arc::clone(&schema), Arc::clone(&props))?;

    eprintln!("parquet file created: {} bytes", bytes.len());

    let mut reader = SerializedFileReader::new(bytes)?;
    let reader: &mut dyn FileReader = &mut reader;

    let mut writer = SerializedFileWriter::new(BytesMut::new().writer(), schema, props)?;

    let mut values = vec![Default::default(); 5].into_boxed_slice();
    let mut def_levels = [0i16; 5];
    let mut rep_levels = [0i16; 5];

    {
        for i in 0..reader.num_row_groups() {
            let row_group_reader = reader.get_row_group(i)?;
            let mut column_group_writer = writer.next_row_group()?;

            for j in 0..row_group_reader.num_columns() {
                let mut column_reader = row_group_reader.get_column_reader(j)?;

                let mut column_writer = column_group_writer
                    .next_column()?
                    .expect("Expected the writer to have the same number of columns as the reader");

                let typed_column_writer = column_writer.typed::<ByteArrayType>();

                loop {
                    let (total_records_read, values_read, levels_read) = match &mut column_reader {
                        parquet::column::reader::ColumnReader::ByteArrayColumnReader(cr) => cr
                            .read_records(
                                5,
                                Some(&mut def_levels),
                                Some(&mut rep_levels),
                                &mut values[..],
                            )?,
                        _ => panic!("Only implemented for byte arrays"),
                    };

                    eprintln!("reader: {total_records_read} records read");
                    eprintln!("reader: {values_read} values read");
                    eprintln!("reader: {levels_read} levels read");

                    if values_read == 0 && levels_read == 0 {
                        eprintln!("reader: no values or levels read, exiting loop");
                        break;
                    }

                    let values_written = typed_column_writer.write_batch(
                        &values[0..values_read],
                        Some(&def_levels[0..levels_read]),
                        Some(&rep_levels[0..levels_read]),
                    )?;

                    eprintln!("writer: {values_written} values written");
                }

                column_writer.close()?;
            }

            column_group_writer.close()?;
        }
    }

    let bytes = Bytes::from(writer.into_inner()?.into_inner());

    eprintln!("parquet file rewritten: {} bytes", bytes.len());

    Ok(())
}

fn create_small_parquet_file(
    schema: Arc<parquet::schema::types::Type>,
    props: Arc<WriterProperties>,
) -> Result<Bytes> {
    let mut writer = SerializedFileWriter::new(BytesMut::new().writer(), schema, props)?;

    {
        let mut row_group_writer = writer.next_row_group()?;

        let mut column_writer = row_group_writer
            .next_column()?
            .ok_or(anyhow!("No column"))?;

        let typed = column_writer.typed::<ByteArrayType>();

        let mut repeated_writer = RepeatedWriter::new();

        repeated_writer.push(names(4).into_iter());
        repeated_writer.push(names(4).into_iter());

        let _ = typed.write_batch(
            repeated_writer.values(),
            repeated_writer.def_levels(),
            repeated_writer.rep_levels(),
        )?;

        column_writer.close()?;

        row_group_writer.close()?;
    }

    Ok(Bytes::from(writer.into_inner()?.into_inner()))
}

fn names(count: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| format!("Name {i}").into_bytes())
        .collect()
}
