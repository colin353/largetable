use std::io;
use std::io::Seek;
use std::io::Read;
use std;
use std::fs;
use std::fmt;

use protobuf;

use mtable;
use generated::dtable::*;

pub struct DTable {
    filename: String,
    lookup: DTableHeader
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
    pub fn get_value(&self) -> Result<DEntry, TError> {
        let entries = self.get_entries();
        match entries.len() {
            0 => Err(TError::NotFound),
            n => Ok(entries[n-1].clone())
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

    pub fn get_value(&self, key: &str) -> Result<DEntry, TError> {
        self.get_column(key)?.get_value()
    }
}

impl std::fmt::Display for DRow {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DRow: {{ {} }}",
                self.get_keys()
                .iter()
                .map(|s| format!("{}: {:?}", s, self.get_value(s).unwrap().get_value()))
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

    pub fn select_one(&self, row: &str, col: &str) -> Option<Vec<u8>> {
        match self.select(row, &[col]) {
            Some(ref result) => match result[0] {
                Some(ref value) => Some(value.get_value().to_owned()),
                None        => None
            },
            None => None
        }
    }

    pub fn select(&self, row: &str, cols: &[&str]) -> mtable::TOption {
        let row = match self.get_row(row) {
            Ok(r)   => r,
            Err(_)  => return None
        };

        return Some(cols.iter().map(|col| {
            match row.get_value(col) {
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
}
