/*
    base.rs

    This file contains the core functions of the database, such as
    handling queries on the tables, generating and merging tables,
    etc.
*/

use std;
use std::iter;
use std::iter::FromIterator;
use std::mem;
use std::io::Read;
use std::io::Write;
use std::io::Seek;

use regex;
use mtable;
use dtable;
use query;
use glob::glob;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use protobuf;
use protobuf::Message;

use generated::dtable::*;

#[derive(Debug)]
pub enum BaseError {
    CorruptedFiles
}

pub struct Base {
    directory: String,
    disktable_index: u32,
    memtable: mtable::MTable,
    disktables: Vec<dtable::DTable>,
    commit_log: std::fs::File
}

impl Base {
    pub fn new(directory: &str) -> Base {
        let log = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(format!("{}/commit.log", directory))
            .unwrap();

        Base{
            directory: directory.to_owned(),
            disktable_index: 0,
            memtable: mtable::MTable::new(),
            disktables: vec![],
            commit_log: log
        }
    }

    // Try to load the complete state of the database from the filesystem.
    pub fn load(&mut self) -> Result<(), BaseError> {
        self.load_mtable()?;
        println!("Loaded mtable");
        self.load_dtables()?;

        Ok(())
    }

    // Read from the commit log, and write all entries to the memtable.
    fn load_mtable(&mut self) -> Result<(), BaseError> {
        let mut commit_log = std::fs::File::open(format!("{}/commit.log", self.directory))
            .map_err(|_| BaseError::CorruptedFiles)?;

        loop {
            // Try to read an entry from the commit log. First, get the size
            // which is encoded as 4 bytes.
            println!("new position {}", commit_log.seek(std::io::SeekFrom::Current(0)).unwrap());
            let size = match commit_log.read_u32::<LittleEndian>() {
                Ok(n)   => n,
                // If we reach end of file, we'll quit.
                Err(e) => {
                    println!("memtable: EOF ({})", e);
                    return Ok(())
                }
            };

            println!("Read commit of length {}", size);

            // Next, load the next few bytes into a CommitLogUpdate.
            let mut buf = vec![0; size as usize]; //Vec::<u8>::with_capacity(size as usize);
            commit_log.read_exact(&mut buf)
                .map_err(|_| BaseError::CorruptedFiles)?;
            let clu = protobuf::parse_from_bytes::<CommitLogEntry>(&buf)
                .map_err(|_| BaseError::CorruptedFiles)?;

            println!("Applied query on {:?}", clu.get_key());

            // Write the commit log update to the memtable.
            match self.direct_update(
                clu.get_key(),
                clu.get_updates()
                    .iter()
                    .map(|u| mtable::MUpdate::new(
                        u.get_column(),
                        u.get_value().to_owned()
                    )).collect::<Vec<_>>()
            ) {
                query::QueryResult::Done => (),
                _ => return Err(BaseError::CorruptedFiles)
            };
        }
    }

    // Load up all of the DTables located in the directory.
    fn load_dtables(&mut self) -> Result<(), BaseError> {
        let entries = glob(&format!("{}/*.dtable", self.directory)).map_err(|_| BaseError::CorruptedFiles)?;

        let file_scanner = regex::Regex::new(r"/([0-9]+)\.dtable$").unwrap();
        for entry in entries {
            let data_path = entry.map_err(|_| BaseError::CorruptedFiles)?;
            let data = data_path.to_str().ok_or(BaseError::CorruptedFiles)?;

            // First, let's check for a number in the filename. That'll let us know
            // what index future dtables should be at.
            let mat = file_scanner.captures(&data).ok_or(BaseError::CorruptedFiles)?;
            let index = mat.get(1).unwrap().as_str().parse::<u32>().map_err(|_| BaseError::CorruptedFiles)?;
            if index > self.disktable_index {
                self.disktable_index = index;
            }

            // We need two files to read a dtable. One is the dtable filename, and
            // the second is the header, which must be read into memory.
            let mut header: String = data.to_owned();
            header.push_str(".header");
            let header_file = std::fs::File::open(&header).map_err(|_| BaseError::CorruptedFiles)?;

            self.disktables.push(
                dtable::DTable::new(data.to_owned(), header_file).map_err(|_| BaseError::CorruptedFiles)?
            );
            println!("Loaded dtable: {}", data);
        }

        Ok(())
    }

    // This function takes the current state of the memtable and empties it
    // into a DTable, finally replacing the memtable with a new, blank one.
    pub fn empty_memtable(&mut self) -> Result<(), BaseError> {
        self.disktable_index += 1;

        // This block clarifies scope for the file references.
        {
            println!("Creating dtable file.");
            let mut f = std::fs::File::create(
                format!("{}/{}.dtable", self.directory, self.disktable_index)
            ).map_err(|_| BaseError::CorruptedFiles)?;

            println!("Creating dtable header.");
            let mut h = std::fs::File::create(
                format!("{}/{}.dtable.header", self.directory, self.disktable_index)
            ).map_err(|_| BaseError::CorruptedFiles)?;

            println!("Writing memtable to disk.");
            self.memtable.write_to_writer(&mut f, &mut h).map_err(|_| BaseError::CorruptedFiles)?;
        }

        println!("Emptying memtable.");
        mem::replace(&mut self.memtable, mtable::MTable::new());

        println!("Reading fresh dtable.");
        let h = std::fs::File::open(
            format!("{}/{}.dtable.header", self.directory, self.disktable_index)
        ).map_err(|_| BaseError::CorruptedFiles)?;

        self.disktables.push(
            dtable::DTable::new(format!("{}/{}.dtable.header", self.directory, self.disktable_index), h).map_err(|_| BaseError::CorruptedFiles)?
        );

        // Delete the commit log, since we are writing it to disk.
        println!("Truncating commit log.");
        mem::replace(
            &mut self.commit_log,
            std::fs::File::create(format!("{}/commit.log", self.directory))
                .map_err(|_| BaseError::CorruptedFiles)?
        );

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
            },
            query::Query::Update{row: r, set: s} => {
                self.update(
                    &r,
                    s.into_iter().map(|(key, value)|
                        mtable::MUpdate::new(key.as_str(), value.into_bytes())
                    ).collect::<Vec<_>>()
                )
            }
        }
    }

    // Publish an insert/update to the commit log.
    pub fn commit(&mut self, row: &str, updates: &[mtable::MUpdate]) -> Result<(), BaseError> {
        let mut c = CommitLogEntry::new();
        c.set_key(row.to_owned());
        c.set_updates(::protobuf::RepeatedField::from_iter(
            updates.iter()
                .map(|u| {
                    let mut cu = CommitLogUpdate::new();
                    cu.set_column(u.key.to_owned());
                    cu.set_value(u.value.to_owned());
                    cu.set_timestamp(200);
                    return cu;
                })
        ));

        let size = c.compute_size();
        println!("Writing commit, length = {}", size);
        self.commit_log.write_u32::<LittleEndian>(size).map_err(|_| BaseError::CorruptedFiles)?;

        c.write_to_writer(&mut self.commit_log).map_err(|_| BaseError::CorruptedFiles)?;
        self.commit_log.flush().map_err(|_| BaseError::CorruptedFiles)?;
        Ok(())
    }

    pub fn insert(&mut self, row: &str, updates: Vec<mtable::MUpdate>) -> query::QueryResult {
        self.commit(row, &updates);
        match self.memtable.insert(row, updates) {
            Ok(_)   => query::QueryResult::Done,
            Err(dtable::TError::AlreadyExists)  => query::QueryResult::RowAlreadyExists,
            Err(_) => query::QueryResult::InternalError
        }
    }

    #[cfg(test)]
    pub fn str_query(&mut self, input: &str) -> String {
        format!("{}", self.query(query::Query::parse(input).unwrap()))
    }

    // This private method does an update without creating a commit log entry.
    fn direct_update(&mut self, row: &str, updates: Vec<mtable::MUpdate>) -> query::QueryResult {
        match self.memtable.update(row, updates) {
            Ok(_) => query::QueryResult::Done,
            Err(dtable::TError::NotFound) => query::QueryResult::RowNotFound,
            Err(_) => query::QueryResult::InternalError
        }
    }

    // This function does a commit-then-update, using the private direct_update method.
    pub fn update(&mut self, row: &str, updates: Vec<mtable::MUpdate>) -> query::QueryResult {
        match self.commit(row, &updates) {
            Err(_) => return query::QueryResult::InternalError,
            Ok(_)  => ()
        };

        self.direct_update(row, updates)
    }

    pub fn select(&self, row: &str, cols: &[&str]) -> query::QueryResult {
        // First, try to query the mtable.
        let mresult = iter::once(&self.memtable)
            .map(|m| m.select(row, cols));

        // Now, merge the results with those in the dtables.
        let dresults = self.disktables
            .iter()
            .map((|d| d.select(row, cols)));

        // Eliminate any misses, and collect up rows to merge.
        let results = mresult
            .chain(dresults)
            .filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .collect::<Vec<_>>();

        match results.len() {
            0 => query::QueryResult::RowNotFound,
            _ => query::QueryResult::Data{columns: cols.iter()
                .enumerate()
                .map(|(i, _)| {
                    for row in &results {
                        if row[i].is_some() {
                            return row[i].clone();
                        }
                    }
                    return None
                }).collect::<Vec<_>>()
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use query;

    #[test]
    fn test_insert() {
        let mut database = super::Base::new();
        database.load("./data").unwrap();

        let done = format!("{}", query::QueryResult::Done);
        let row_not_found = format!("{}", query::QueryResult::RowNotFound);

        assert_eq!(
            database.str_query(r#"{"select": {"row": "non-row", "get": []}}"#),
            row_not_found
        );

        assert_eq!(
            database.str_query(r#"{"insert": {"row": "non-row", "set": {"date": "01-01-1970", "weight": "12 kg"}}}"#),
            done
        );

        assert_eq!(
            database.str_query(r#"{"update": {"row": "non-row", "set": {"weight": "15 kg"}}}"#),
            done
        );

        assert_eq!(
            database.str_query(r#"{"select": {"row": "non-row", "get": ["date", "fate", "weight"]}}"#),
            r#"Data: ["01-01-1970", None, "15 kg"]"#
        );
    }
}
