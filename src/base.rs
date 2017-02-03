/*
    base.rs

    This file contains the core functions of the database, such as
    handling queries on the tables, generating and merging tables,
    etc.
*/

use std;
use std::fs;

use mtable;
use dtable;
use query;
use glob::glob;

#[derive(Debug)]
pub enum BaseError {
    CorruptedFiles
}

pub struct Base {
    memtable: mtable::MTable,
    disktables: Vec<dtable::DTable>
}

impl Base {
    pub fn new() -> Base {
        Base{
            memtable: mtable::MTable::new(),
            disktables: vec![]
        }
    }

    // Load up all of the DTables located in the directory.
    pub fn load(&mut self, directory: &str) -> Result<(), BaseError> {
        let entries = glob(&format!("{}/*.dtable", directory)).map_err(|_| BaseError::CorruptedFiles)?;
        for entry in entries {
            // We need two files to read a dtable. One is the dtable filename, and
            // the second is the header, which must be read into memory.
            let data_path = entry.map_err(|_| BaseError::CorruptedFiles)?;
            let data = data_path.to_str().ok_or(BaseError::CorruptedFiles)?;
            let mut header: String = data.to_owned();
            header.push_str(".header");
            let mut header_file = std::fs::File::open(&header).map_err(|_| BaseError::CorruptedFiles)?;

            self.disktables.push(
                dtable::DTable::new(data.to_owned(), header_file).map_err(|_| BaseError::CorruptedFiles)?
            );
        }

        Ok(())
    }
}
