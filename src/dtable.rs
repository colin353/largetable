use std::io;
use std::io::Seek;
use std::io::Read;
use std;
use std::fs;
use std::fmt;

use protobuf;
use protobuf::Message;

use mtable;
use generated::dtable::*;

pub struct DTable {
    filename: String,
    pub lookup: DTableHeader
}

#[derive(Debug)]
pub enum TError {
    IoError,
    NotFound,
    AlreadyExists
}

impl std::convert::From<std::io::Error> for TError {
    fn from(_: std::io::Error) -> Self {
        return TError::IoError;
    }
}

impl DColumn {
    pub fn get_latest_value(&self) -> Result<DEntry, TError> {
        self.get_value(std::u64::MAX)
    }

    pub fn get_value(&self, timestamp: u64) -> Result<DEntry, TError> {
        let entries = self.get_entries();
        match entries.len() {
            0 => Err(TError::NotFound),
            n => {
                let mut index = n-1;
                for i in (0..n).rev() {
                    if entries[i].get_timestamp() <= timestamp {
                        index = i;
                        break;
                    }
                }
                Ok(entries[index].clone())
            }
        }
    }

    // This function merges together a series of DColumns into a single one.
    pub fn from_vec(cols: &[&DColumn]) -> DColumn {
        let mut iterators = cols.iter()
            .map(|c| c.get_entries().iter().peekable())
            .collect::<Vec<_>>();

        let mut output = vec![];

        loop {
            let index = match iterators.iter_mut()
                .enumerate()
                .fold(None, |acc, (j, mut x)| match (acc, x.peek()) {
                    (Some((i, timestamp)), Some(e)) => {
                        match (timestamp, e.get_timestamp()) {
                            (t, t_new) if t >= t_new => Some((i, timestamp)),
                            (_, t_new) => Some((j, t_new))
                        }
                    },
                    (Some((i, timestamp)), None) => Some((i, timestamp)),
                    (None, Some(e)) => Some((j, e.get_timestamp())),
                    (None, None) => None
                }) {
                Some((i, _)) => i,
                None => break
            };

            output.push(iterators[index].next().unwrap().clone());
        }

        let mut d = DColumn::new();
        d.set_entries(protobuf::RepeatedField::from_vec(output));
        return d;
    }
}

impl DRow {
    pub fn get_column(&self, key: &str) -> Result<&DColumn, TError> {
        let keys = self.get_keys();
        let mut l: i32 = 0;
        let mut r: i32 = keys.len() as i32 - 1;

        while l <= r {
            let index = (l + r) >> 1;
            match &keys[index as usize] {
                k if key == k => return Ok(&self.get_columns()[index as usize]),
                k if key > k  => l = index + 1,
                _             => r = index - 1
            }
        }
        return Err(TError::NotFound);
    }

    pub fn get_latest_value(&self, key: &str) -> Result<DEntry, TError> {
        self.get_column(key)?.get_latest_value()
    }

    pub fn get_value(&self, key: &str, timestamp: u64) -> Result<DEntry, TError> {
        self.get_column(key)?.get_value(timestamp)
    }

    // Merge a list of DRows with the same key together into a new DRow
    // with the same key
    pub fn from_vec(rows: &[DRow]) -> DRow {
        let mut iterators = rows.iter()
            .map(|r| r.get_keys().iter().peekable())
            .collect::<Vec<_>>();

        let mut indices = vec![0; iterators.len()];

        let mut output_keys = vec![];
        let mut output_cols = vec![];

        loop {
            // First step is to figure out the column key to insert into
            // the new row. It's possible that several DRows will share
            // the same columns, in which case we'll have to merge those
            // columns.
            let (indices_to_merge, key) = match iterators
                .iter_mut()
                .enumerate()
                .fold(None, |acc, (i, mut x)| match (acc, x.peek()) {
                (Some((mut ix, acc_key)), Some(new_key)) => {
                    match (new_key, acc_key) {
                        (new_key, acc_key) if new_key < acc_key  => Some((vec![i], new_key)),
                        (new_key, acc_key) if new_key == acc_key => {
                            ix.push(i);
                            Some((ix, acc_key))
                        }
                        _ => Some((ix, acc_key))
                    }
                },
                (Some((ix, key)), None) => Some((ix, key)),
                (None, Some(k)) => Some((vec![i], k)),
                (None, None) => None
            }) {
                Some((indices_to_merge, key)) => {
                    (indices_to_merge, key.to_string())
                },
                None => break
            };

            // If there's only one index to merge, then we can directly copy it.
            if indices_to_merge.len() == 1 {
                let index = indices_to_merge[0];
                output_keys.push(key);
                output_cols.push(rows[index].get_columns()[indices[index]].clone());
                indices[index] += 1;
                iterators[index].next();
            }
            // In this case, we need to merge a list of columns together and then copy that
            // column into our output.
            else {
                output_keys.push(key.to_string());
                let col = DColumn::from_vec(
                    indices_to_merge.iter()
                        .map(|index| &rows[index.clone()].get_columns()[indices[index.clone()]])
                        .collect::<Vec<_>>()
                        .as_slice()
                );

                for index in indices_to_merge {
                    indices[index] += 1;
                }
                output_cols.push(col);
            }
        }

        let mut d = DRow::new();
        d.set_columns(protobuf::RepeatedField::from_vec(output_cols));
        d.set_keys(protobuf::RepeatedField::from_vec(output_keys));
        return d;
    }
}

impl std::fmt::Display for DRow {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DRow: {{ {} }}",
                self.get_keys()
                .iter()
                .map(|s| format!("{}: {:?}", s, self.get_latest_value(s).unwrap().get_value()))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

struct DataRegion {
    start: u64,
    length: Option<u64>
}

impl DTable {
    pub fn new(filename: String, mut header: fs::File) -> Result<DTable, io::Error> {
        let lookup = protobuf::parse_from_reader::<DTableHeader>(&mut header)?;

        Ok(DTable{
            filename: filename,
            lookup: lookup
        })
    }

    pub fn from_dtableheader(filename: String, header: DTableHeader) -> DTable {
        DTable{
            filename: filename,
            lookup: header
        }
    }

    pub fn len(&self) -> usize {
        self.lookup.get_entries().len()
    }

    fn get_offset_from_index(&self, index: usize) -> DataRegion {
        let entries = self.lookup.get_entries();

        let length = match index + 1 {
            i if i as usize == entries.len() =>
                None,
            i => Some(entries[i as usize].get_offset())
        };

        return DataRegion{
            start:  entries[index].get_offset(),
            length: length
        };
    }

    fn get_row_offset(&self, key: &str) -> Option<DataRegion> {
        let entries = self.lookup.get_entries();
        let mut l: i32 = 0;
        let mut r: i32 = entries.len() as i32 - 1;

        while l <= r {
            let index = (l + r) >> 1;
            match &entries[index as usize] {
                e if key == e.get_key() => {
                    let length = match index + 1 {
                        i if i as usize == entries.len() =>
                            None,
                        i => Some(entries[i as usize].get_offset())
                    };
                    return Some(DataRegion{
                        start:  e.get_offset(),
                        length: length
                    })
                }
                e if key > e.get_key()  => l = index + 1,
                _                       => r = index - 1
            }
        }
        return None;
    }

    fn get_reader(&self) -> Result<std::fs::File, io::Error> {
        std::fs::File::open(&self.filename)
    }

    #[cfg(test)]
    pub fn select_one(&self, row: &str, col: &str) -> Option<Vec<u8>> {
        match self.select(row, &[col], std::u64::MAX) {
            Some(ref result) => match result[0] {
                Some(ref value) => Some(value.get_value().to_owned()),
                None        => None
            },
            None => None
        }
    }

    pub fn select(&self, row: &str, cols: &[&str], timestamp: u64) -> mtable::TOption {
        let row = match self.get_row(row) {
            Ok(r)   => r,
            Err(_)  => return None
        };

        return Some(cols.iter().map(|col| {
            match row.get_value(col, timestamp) {
                Ok(v)   => Some(v),
                Err(_)  => None
            }
        }).collect::<Vec<_>>());
    }

    pub fn get_row(&self, key: &str) -> Result<DRow, TError> {
        let offset = match self.get_row_offset(key) {
            Some(n) => n,
            None    => return Err(TError::NotFound)
        };

        let mut file = self.get_reader()?;

        file.seek(io::SeekFrom::Start(offset.start))?;

        return match offset.length {
            Some(n) => protobuf::parse_from_reader::<DRow>(&mut file.take(n)),
            None    => protobuf::parse_from_reader::<DRow>(&mut file)
        }.map_err(|_| {
            TError::IoError
        });
    }

    // from_vec takes a list of dtables and merges them into a single
    // dtable. This is a bit of a complicated function. Essentially, it
    // runs sequentially through the rows of each dtable and merges them
    // together in order.
    pub fn from_vec(filename: &str, tables: &[DTable]) -> Result<DTable, TError> {
        let mut f_out = std::fs::File::create(filename)?;
        let mut files = tables.iter()
            .map(|t| t.get_reader())
            .filter(|r| r.is_ok())
            .map(|f| f.unwrap())
            .collect::<Vec<_>>();

        // The indices vector tells us how many elements have been removed
        // from each iterator.
        let mut indices = vec![0; tables.len()];

        // The offset tracks how many bytes we've written to the dtable.
        let mut offset = 0;

        // Need to detect if any errors occurred in creating file readers
        // during the iteration process.
        if files.len() != tables.len() {
            return Err(TError::IoError);
        }

        let mut iterators = tables.iter()
            .map(|t| t.lookup.get_entries().iter().peekable())
            .collect::<Vec<_>>();

        // The output is the DTable that we'll return, which corresponds
        // to the merged data.
        let mut output = DTable{
            filename: filename.to_owned(),
            lookup: DTableHeader::new()
        };

        loop {
            // Here we're going to search the list of provided dtables to find
            // the next index to write.
            let (indices_to_write, next_key) = match iterators.iter_mut()
                .enumerate()
                .fold(None, |acc, (i, mut x)| match (acc, x.peek()) {
                    (Some((mut ix, key)), Some(k)) => {
                        match (k.get_key(), key) {
                            (new_key, acc_key) if new_key < acc_key  => Some((vec![i], new_key)),
                            (new_key, acc_key) if new_key == acc_key => {
                                ix.push(i);
                                Some((ix, key))
                            }
                            _ => Some((ix, key))
                        }
                    },
                    (Some((ix, key)), None) => Some((ix, key)),
                    (None, Some(k)) => Some((vec![i], k.get_key())),
                    (None, None) => None
                }) {
                Some((ix, next_key)) => (ix, next_key),

                // If we reach this statement, it means that all of the DTables
                // we are reading from are empty, so we're done.
                None => break
            };

            // There are two possibilities here. One: we have a single key that needs
            // to be directly copied from the source file to the destination, or two,
            // we have a number of identical keys which need to be merged, then written.
            match indices_to_write.len() {
                0 => panic!("It should not be possible to reach this statement."),

                // Okay, there's only one key which is to be written. In that case,
                // we'll directly copy the data from the source file to the destination.
                1 => {
                    let index = indices_to_write[0];
                    // Let's figure out which part of the files to copy into the new record.
                    let region = tables[index].get_offset_from_index(indices[index]);

                    // Next: move forward the index that we chose.
                    indices[index] += 1;

                    // Now seek the file to the start of the location we wish to copy, and
                    // copy the data from the source dtable to the new dtable.
                    let ref mut origin = files[index];
                    origin.seek(io::SeekFrom::Start(region.start))?;
                    let length = match region.length {
                            Some(n) => io::copy(&mut origin.take(n), &mut f_out),
                            None    => io::copy(origin, &mut f_out)
                    }?;

                    let mut hentry = DTableHeaderEntry::new();
                    hentry.set_key(next_key.to_owned());
                    hentry.set_offset(offset);
                    offset += length;

                    output.lookup.mut_entries().push(hentry);

                    iterators[index].next();
                },

                // Okay, we have multiple rows which need to be merged before being written.
                _ => {
                    panic!("Merging rows not yet implemented.")
                }
            };
        }

        // Finally, write the headers.
        let mut header_file = std::fs::File::create(format!("{}.header", filename))?;
        output.lookup.write_to_writer(&mut header_file).map_err(|_| TError::IoError)?;

        // Flush the writes to disk.
        header_file.sync_all()?;
        f_out.sync_all()?;

        return Ok(output);
    }
}

#[cfg(test)]
mod tests {
    use rand;
    use protobuf;
    use std;

    #[test]
    fn can_merge_columns() {
        let data = vec![230, 210, 40, 30, 22];

        // Generate columns with valid (ordered, but random) timestamps.
        let cols = (0..10).map(|_| {
                let mut c = super::DColumn::new();
                let mut entries = (0..100).map(|_| {
                        let mut e = super::DEntry::new();
                        e.set_timestamp(rand::random::<u32>() as u64);
                        e.set_value(data.clone());
                        return e;
                    }).collect::<Vec<_>>();
                entries.sort_by_key(|e| -(e.get_timestamp() as i64));
                c.set_entries(protobuf::RepeatedField::from_vec(entries));
                return c;
            }).collect::<Vec<_>>();

        // Merge the columns together. It should still be ordered after the
        // merge, and have exactly 1000 entries.
        let merged = super::DColumn::from_vec(cols.iter().collect::<Vec<_>>().as_slice());

        let entries = merged.get_entries();
        assert_eq!(entries.len(), 1000);

        let mut minimum = std::u64::MAX;
        for e in entries {
            let t = e.get_timestamp();
            assert!(t <= minimum, "t({}) > minimum({})", t, minimum);
            minimum = t;
        }
    }

    #[test]
    fn can_merge_rows() {
        let rows = (0..20).map(|index| {
            let mut e = super::DEntry::new();
            e.set_timestamp(100);
            e.set_value(vec![]);

            let mut c = super::DColumn::new();
            c.set_entries(protobuf::RepeatedField::from_vec(vec![e]));

            let mut r = super::DRow::new();
            r.set_keys(protobuf::RepeatedField::from_vec(vec![format!("hello{}", index%3)]));
            r.set_columns(protobuf::RepeatedField::from_vec(vec![c]));

            return r;
        }).collect::<Vec<_>>();

        let new_row = super::DRow::from_vec(rows.as_slice());
        new_row.get_column("hello0").unwrap();
        new_row.get_column("hello1").unwrap();
        new_row.get_column("hello2").unwrap();
    }
}
