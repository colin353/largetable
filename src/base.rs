/*
    base.rs

    This file contains the core functions of the database, such as
    handling queries on the tables, generating and merging tables,
    etc.
*/

use std;
use std::mem;

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
            println!("Loaded dtable: {}", data);
        }

        Ok(())
    }

    pub fn query(&mut self, q: query::Query) -> query::QueryResult {
        match q {
            query::Query::Select{row: r, get: g} => {
                self.select(
                    &r,
                    g.iter()
                      .map(|s| s.as_str())
                      .collect::<Vec<&str>>()
                      .as_slice()
                 )
            },
            query::Query::Insert{row: r, set: s} => {
                self.insert(
                    &r,
                    s.into_iter().map(|(key, value)|
                        mtable::MUpdate::new(key.as_str(), value.into_bytes())
                    ).collect::<Vec<_>>()
                )
            }
            _ => query::QueryResult::NotImplemented
        }
    }

    pub fn insert(&mut self, row: &str, updates: Vec<mtable::MUpdate>) -> query::QueryResult {
        self.memtable.insert(row, updates);
        query::QueryResult::Done
    }

    pub fn select(&self, row: &str, cols: &[&str]) -> query::QueryResult {
        // First, try to query the mtable.
        let mut result = self.memtable.select(row, cols);

        // Now, merge the results with those in the dtables.
        for d in &self.disktables {
            let mut merge = d.select(row, cols);
            match result {
                Some(ref r) => {
                    match merge {
                        // Both dtable and mtable have values, so we
                        // must merge them together.
                        Some(m) => {

                        },
                        // The dtable has nothing to contribute, skip.
                        None => continue
                    }
                },
                // The memtable didn't have anything, so we'll use
                // this dtable value as the result.
                None    => {
                    mem::replace(&mut result, merge);
                }
            }
        }

        match result {
            Some(result) => query::QueryResult::Data{columns: result},
            None         => query::QueryResult::RowNotFound
        }
    }
}
