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
            let (index, next_key) = match iterators.iter_mut()
                .enumerate()
                .fold(None, |acc, (i, mut x)| match (acc, x.peek()) {
                    (Some((j, key)), Some(k)) => {
                        match k.get_key() < key {
                            true    => Some((i, k.get_key())),
                            false   => Some((j, key))
                        }
                    },
                    (Some((j, key)), None) => Some((j, key)),
                    (None, Some(k)) => Some((i, k.get_key())),
                    (None, None) => None
                }) {
                Some((index, next_key)) => (index, next_key),
                None => break
            };
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
