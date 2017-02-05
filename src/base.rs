/*
    base.rs

    This file contains the core functions of the database, such as
    handling queries on the tables, generating and merging tables,
    etc.
*/

use std;
use std::iter;
use std::mem;
use regex;

use mtable;
use dtable;
use query;
use glob::glob;

#[derive(Debug)]
pub enum BaseError {
    CorruptedFiles
}

pub struct Base {
    directory: String,
    disktable_index: u32,
    memtable: mtable::MTable,
    disktables: Vec<dtable::DTable>
}

impl Base {
    pub fn new() -> Base {
        Base{
            directory: String::new(),
            disktable_index: 0,
            memtable: mtable::MTable::new(),
            disktables: vec![]
        }
    }

    // Load up all of the DTables located in the directory.
    pub fn load(&mut self, directory: &str) -> Result<(), BaseError> {
        let entries = glob(&format!("{}/*.dtable", directory)).map_err(|_| BaseError::CorruptedFiles)?;

        self.directory = directory.to_owned();
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

    pub fn insert(&mut self, row: &str, updates: Vec<mtable::MUpdate>) -> query::QueryResult {
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

    pub fn update(&mut self, row: &str, updates: Vec<mtable::MUpdate>) -> query::QueryResult {
        match self.memtable.update(row, updates) {
            Ok(_) => query::QueryResult::Done,
            Err(dtable::TError::NotFound) => query::QueryResult::RowNotFound,
            Err(_) => query::QueryResult::InternalError
        }
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
