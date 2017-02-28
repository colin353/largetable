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

use time;
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
    CorruptedFiles,
    Problem{reason: String}
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

    // new_stub creates a database based in the /tmp/largetable directory.
    // It'll ensure that the directory is cleared out before before initializing
    // so it has a blank slate.
    pub fn new_stub() -> Base {
        // First, delete the /tmp/largetable directory and it's
        // contents. Then recreate the directory.
        let directory = format!("/tmp/largetable/largetable-{}", time::precise_time_ns());
        std::fs::create_dir_all(&directory).unwrap_or(());

        let log = std::fs::File::create(format!("{}/commit.log", directory)).unwrap();

        Base{
            directory: String::from(directory),
            disktable_index: 0,
            memtable: mtable::MTable::new(),
            disktables: vec![],
            commit_log: log
        }
    }

    // Try to load the complete state of the database from the filesystem.
    pub fn load(&mut self) -> Result<(), BaseError> {
        self.load_mtable()?;
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
            let size = match commit_log.read_u32::<LittleEndian>() {
                Ok(n)   => n,
                // If we reach end of file, we'll quit.
                Err(_) => {
                    return Ok(())
                }
            };

            // Next, load the next few bytes into a CommitLogUpdate.
            let mut buf = vec![0; size as usize]; //Vec::<u8>::with_capacity(size as usize);
            commit_log.read_exact(&mut buf)
                .map_err(|_| BaseError::CorruptedFiles)?;
            let clu = protobuf::parse_from_bytes::<CommitLogEntry>(&buf)
                .map_err(|_| BaseError::CorruptedFiles)?;

            // Write the commit log update to the memtable.
            match self.direct_update(
                clu.get_key(),
                clu.get_updates()
                    .iter()
                    .map(|u| query::MUpdate::new(
                        u.get_column(),
                        u.get_value().to_owned()
                    )).collect::<Vec<_>>()
                    .as_slice(),
                clu.get_timestamp()
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
            let mat = file_scanner.captures(data).ok_or(BaseError::CorruptedFiles)?;
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

        println!("Creating dtable header.");
        let mut h = std::fs::File::create(
            format!("{}/{}.dtable.header", self.directory, self.disktable_index)
        ).map_err(|e| BaseError::Problem{
            reason: format!("Unable to create file: {}", e)
        })?;

        println!("Creating dtable file.");
        let mut f = std::fs::File::create(
            format!("{}/{}.dtable", self.directory, self.disktable_index)
        ).map_err(|_| BaseError::CorruptedFiles)?;

        println!("Writing memtable to disk.");
        let dheader = self.memtable.write_to_writer(&mut f, &mut h)
            .map_err(|_| BaseError::Problem{
                reason: String::from("Unable to write DTable to disk.")
            }
        )?;

        // Flush all buffers to disk.
        f.sync_all().map_err(|_| BaseError::CorruptedFiles)?;
        h.sync_all().map_err(|_| BaseError::CorruptedFiles)?;

        println!("Emptying memtable.");
        mem::replace(&mut self.memtable, mtable::MTable::new());

        self.disktables.push(dtable::DTable::from_dtableheader(
            format!("{}/{}.dtable", self.directory, self.disktable_index),
            dheader
        ));

        // Delete the commit log, since we are writing it to disk.
        println!("Truncating commit log.");
        mem::replace(
            &mut self.commit_log,
            std::fs::File::create(format!("{}/commit.log", self.directory))
                .map_err(|_| BaseError::CorruptedFiles)?
        );

        Ok(())
    }

    // Merge the disktables into a single disktable.
    pub fn merge_disktables(&mut self) -> Result<(), BaseError> {
        self.disktable_index += 1;

        let new_disktables = match dtable::DTable::from_vec(
            format!("{}/{}.dtable", self.directory, self.disktable_index).as_str(),
            self.disktables.as_slice()
        ) {
            Ok(d)   => vec![d],
            Err(_)  => return Err(BaseError::CorruptedFiles)
        };

        mem::replace(&mut self.disktables, new_disktables);

        Ok(())
    }

    // Run a query with timestamp set to now.
    pub fn query_now(&mut self, q: query::Query) -> query::QueryResult {
        self.query(q, time::precise_time_ns())
    }

    pub fn query(&mut self, q: query::Query, timestamp: u64) -> query::QueryResult {
        match q {
            query::Query::Select{row: r, get: g} => {
                self.select(
                    &r,
                    g.iter()
                      .map(|s| s.as_str())
                      .collect::<Vec<&str>>()
                      .as_slice(),
                    timestamp
                 )
            },
            query::Query::Insert{row: r, set: s} => {
                self.insert(
                    &r,
                    s.into_iter().map(|(key, value)|
                        query::MUpdate::new(key.as_str(), value)
                    ).collect::<Vec<_>>(),
                    timestamp
                )
            },
            query::Query::Update{row: r, set: s} => {
                self.update(
                    &r,
                    s.into_iter().map(|(key, value)|
                        query::MUpdate::new(key.as_str(), value)
                    ).collect::<Vec<_>>(),
                    timestamp
                )
            }
        }
    }

    // Publish an insert/update to the commit log.
    pub fn commit(&mut self, row: &str, updates: &[query::MUpdate], timestamp: u64) -> Result<(), BaseError> {
        let mut c = CommitLogEntry::new();
        c.set_key(row.to_owned());
        c.set_timestamp(timestamp);
        c.set_updates(::protobuf::RepeatedField::from_iter(
            updates.iter()
                .map(|u| {
                    let mut cu = CommitLogUpdate::new();
                    cu.set_column(u.key.to_owned());
                    cu.set_value(u.value.to_owned());
                    cu
                })
        ));

        let size = c.compute_size();
        self.commit_log.write_u32::<LittleEndian>(size).map_err(|_| BaseError::CorruptedFiles)?;

        c.write_to_writer(&mut self.commit_log).map_err(|_| BaseError::CorruptedFiles)?;
        self.commit_log.sync_all().map_err(|_| BaseError::CorruptedFiles)?;
        Ok(())
    }

    pub fn insert(&mut self, row: &str, updates: Vec<query::MUpdate>, timestamp: u64) -> query::QueryResult {
        match self.memtable.insert(row, &updates, timestamp) {
            Ok(_)   => (),
            Err(dtable::TError::AlreadyExists)  => return query::QueryResult::RowAlreadyExists,
            Err(_) => return query::QueryResult::InternalError
        };

        match self.commit(row, &updates, timestamp) {
            Ok(_)   => query::QueryResult::Done,
            Err(_)  => query::QueryResult::PartialCommit
        }
    }

    #[cfg(test)]
    pub fn str_query(&mut self, input: &str) -> String {
        format!("{}", self.query_now(query::Query::parse(input).unwrap()))
    }

    // This private method does an update without creating a commit log entry.
    fn direct_update(&mut self, row: &str, updates: &[query::MUpdate], timestamp: u64) -> query::QueryResult {
        match self.memtable.update(row, updates, timestamp) {
            Ok(_) => query::QueryResult::Done,
            Err(dtable::TError::NotFound) => query::QueryResult::RowNotFound,
            Err(_) => query::QueryResult::InternalError
        }
    }

    // This function does a commit-then-update, using the private direct_update method.
    pub fn update(&mut self, row: &str, updates: Vec<query::MUpdate>, timestamp: u64) -> query::QueryResult {
        match self.direct_update(row, &updates, timestamp) {
            query::QueryResult::Done => (),
            x   => return x
        };

        match self.commit(row, &updates, timestamp) {
            Ok(_)   => query::QueryResult::Done,
            Err(_)  => query::QueryResult::PartialCommit
        }
    }

    pub fn select(&self, row: &str, cols: &[&str], timestamp: u64) -> query::QueryResult {
        // First, try to query the mtable.
        let mresult = iter::once(&self.memtable)
            .map(|m| m.select(row, cols, timestamp));

        // Now, merge the results with those in the dtables.
        let dresults = self.disktables
            .iter()
            .map(|d| d.select(row, cols, timestamp));

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
                    let mut newest_timestamp = 0;
                    let mut newest_index = 0;
                    for (j, row) in results.iter().enumerate() {
                        match row[i] {
                            Some(ref r) if r.get_timestamp() <= timestamp && r.get_timestamp() > newest_timestamp => {
                                newest_timestamp = r.get_timestamp();
                                newest_index = j;
                            },
                            Some(_) | None => continue
                        }
                    }
                    match newest_timestamp {
                        0 => None,
                        _ => Some(match results[newest_index][i] {
                            Some(ref r) => r.get_value().to_vec(),
                            None        => panic!("This should never occur.")
                        })
                    }
                }).collect::<Vec<_>>()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use query;
    use glob::glob;
    use std::io;
    use std::fs;
    use std::io::BufRead;
    use std::mem;
    use mtable;
    use rand::random;
    use std::u64;

    #[test]
    fn can_merge_disktables() {
        let mut database = super::Base::new_stub();
        assert_eq!(
            database.str_query(r#"{"insert": {"row": "dtable_one","set": {"status": "alright"}}}"#),
            format!("{}", query::QueryResult::Done)
        );
        assert_eq!(
            database.str_query(r#"{"insert": {"row": "dtable_z","set": {"status": "working"}}}"#),
            format!("{}", query::QueryResult::Done)
        );
        database.empty_memtable().unwrap();

        assert_eq!(
            database.str_query(r#"{"insert": {"row": "dtable_two","set": {"status": "ok"}}}"#),
            format!("{}", query::QueryResult::Done)
        );
        database.empty_memtable().unwrap();

        database.merge_disktables().unwrap();

        assert_eq!(
            format!("{:?}", database.disktables[0].lookup.get_entries()
                .iter()
                .map(|e| e.get_key())
                .collect::<Vec<_>>()
            ),
            r#"["dtable_one", "dtable_two", "dtable_z"]"#
        );

        assert_eq!(
            database.str_query(r#"{"select": {"row": "dtable_two","get":["status"]}}"#),
            r#"Data: ["ok"]"#
        );
    }

    // This function generates 25 random bytes of data to write to the
    // database.
    fn random_bytes() -> Vec<u8> {
        (0..25).map(|_| random::<u8>()).collect::<Vec<_>>()
    }

    // This function generates a 25 character long ASCII-printable string.
    fn random_string() -> String {
        (0..25).map(|_| (0x20u8 + (random::<f32>() * 96.0) as u8) as char).collect()
    }

    // This method checks that the two methods on dtables which compute
    // offsets, get_offset_from_index and get_row_offset, match exactly.
    #[test]
    fn row_offset_methods_match() {
        let mut database = super::Base::new_stub();
        for _ in 0..10 {
            database.insert(
                random_string().as_str(),
                (0..10)
                    .map(|_| query::MUpdate::new(random_string().as_str(), random_bytes()))
                    .collect::<Vec<_>>(),
                random::<u64>()
            );
        }

        database.empty_memtable().unwrap();

        let key_list = database.disktables[0].lookup.get_entries()
            .iter()
            .map(|e| e.get_key())
            .collect::<Vec<_>>();

        for (i, k) in key_list.iter().enumerate() {
            let o1 = database.disktables[0].get_row_offset(k).unwrap();
            let o2 = database.disktables[0].get_offset_from_index(i);

            assert_eq!(o1.start, o2.start);
            assert_eq!(o1.length, o2.length);
            if o1.length.is_some() {
                assert_eq!(
                    o1.length,
                    Some(670),
                    "Expected struct length to be exactly 670 bytes.
                    If you changed the struct, this error might be a false positive."
                );
            }
        }
    }

    #[test]
    fn can_multi_merge_disktables() {
        // In this test, we'll generate a series of DTables with random data
        // in several rows. The DTables will be merged, and the resulting table
        // will be checked by a series of queries.
        let mut database = super::Base::new_stub();
        let mut max_timestamp = 0;
        for j in 0..4 {
            // Write ten rows with random junk data.
            for i in 0..4 {
                database.insert(
                    format!("row{}x{}", j, i).as_str(),
                    (0..4)
                        .map(|_| query::MUpdate::new(random_string().as_str(), random_bytes()))
                        .chain(vec![query::MUpdate::new("canary", format!("ok:{}", i).into_bytes())])
                        .collect::<Vec<_>>(),
                    random::<u64>()
                );
            }

            let t = random::<u64>();
            if t > max_timestamp {
                max_timestamp = t;
            }

            // Write one row which will overlap in every dtable.
            database.update(
                "zcanary_row",
                vec![query::MUpdate::new("canary", format!("ok:{}", t).into_bytes())],
                t
            );

            database.empty_memtable().unwrap();
        }

        // This will merge all 10 disktables.
        database.merge_disktables().unwrap();

        println!("{:?}", database.disktables[0].get_row("zcanary_row"));
        println!("{:?}", database.disktables[0].get_row("row0x0"));
        println!("{:?}", database.disktables[0].get_row("row0x1"));

        // Now we just need to query to make sure that all of the merged data
        // follows the expected properties.
        for i in 0..4 {
            for j in 0..4 {
                assert_eq!(
                    format!("{}", database.query(
                        query::Query::parse(format!(r#"{{"select": {{"row": "row{}x{}", "get": ["canary"]}}}}"#, i, j).as_str()).unwrap(),
                        u64::MAX
                    )),
                    format!(r#"Data: ["ok:{}"]"#, j),
                    "expected row{}x{} to contain data: ok:{}", i, j, j
                );
            }
        }
    }

    #[test]
    fn can_merge_colliding_disktables() {
        let mut database = super::Base::new_stub();
        assert_eq!(
            database.str_query(r#"{"insert": {"row": "test_row","set": {"status": "old_status"}}}"#),
            format!("{}", query::QueryResult::Done)
        );
        database.empty_memtable().unwrap();

        assert_eq!(
            database.str_query(r#"{"update": {"row": "test_row", "set": {"status": "new_status"}}}"#),
            format!("{}", query::QueryResult::Done)
        );
        database.empty_memtable().unwrap();

        database.merge_disktables().unwrap();

        assert_eq!(
            database.str_query(r#"{"select": {"row": "test_row", "get":["status"]}}"#),
            r#"Data: ["new_status"]"#
        );
    }

    #[test]
    fn can_save_and_reload_dtables() {
        let directory;
        {
            let mut database = super::Base::new_stub();
            directory = database.directory.to_owned();
            assert_eq!(
                database.str_query(r#"{"insert": {"row": "dtable_checker","set": {"status": "alright"}}}"#),
                format!("{}", query::QueryResult::Done)
            );
            // Write to disk.
            database.empty_memtable().unwrap();
        }

        // Load up the new database using the old directory, and load in the
        // dtable files from that run.
        let mut database = super::Base::new(&directory);
        database.load().unwrap();

        assert_eq!(
            database.str_query(r#"{"select": {"row": "dtable_checker","get": ["status"]}}"#),
            r#"Data: ["alright"]"#
        );
    }

    #[test]
    fn test_insert() {
        let mut database = super::Base::new("./data");

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

    #[test]
    fn can_flush_and_query() {
        let mut database = super::Base::new_stub();
        database.load().unwrap();

        database.query_now(
            query::Query::parse(r#"{"insert": {"row": "write_test", "set": {"value": "OK"}}}"#).unwrap()
        );
        database.query_now(
            query::Query::parse(r#"{"insert": {"row": "write_test2", "set": {"value": "OK"}}}"#).unwrap()
        );

        println!("About to empty memtable.");
        database.empty_memtable().unwrap();

        assert_eq!(
            database.str_query(r#"{"select": {"row": "write_test", "get": ["value"]}}"#),
            r#"Data: ["OK"]"#
        );
    }

    #[test]
    fn check_timestamp_select() {
        // We need to make sure that the system will serve data from
        // a DTable if it has a newer timestamp than that in the MTable.
        let mut database = super::Base::new_stub();
        database.load().unwrap();

        database.query(
            query::Query::parse(r#"{"insert": {"row": "timestamp_test", "set": {"clock": "dtable"}}}"#).unwrap(),
            120
        );
        // Flush the memtable to disk.
        database.empty_memtable().unwrap();

        // Write an older record to the memtable.
        database.query(
            query::Query::parse(r#"{"update": {"row": "timestamp_test", "set": {"clock": "memtable", "clock2": "t=100"}}}"#).unwrap(),
            100
        );

        // Now when we request the data back, we expect the value from the dtable.
        assert_eq!(
            database.str_query(r#"{"select": {"row": "timestamp_test", "get": ["clock"]}}"#),
            r#"Data: ["dtable"]"#
        );

        assert_eq!(
            database.disktables[0].len(),
            1
        );

        // As an extra trick, write older data to the memtable, and then
        // query it to see if still returns the most recent value.
        database.query(
            query::Query::parse(r#"{"update": {"row": "timestamp_test", "set": {"clock2": "t=90"}}}"#).unwrap(),
            90
        );
        database.query(
            query::Query::parse(r#"{"update": {"row": "timestamp_test", "set": {"clock2": "t=95"}}}"#).unwrap(),
            95
        );
        assert_eq!(
            database.str_query(r#"{"select": {"row": "timestamp_test", "get": ["clock2"]}}"#),
            r#"Data: ["t=100"]"#
        );
        database.query(
            query::Query::parse(r#"{"update": {"row": "timestamp_test", "set": {"clock2": "t=110"}}}"#).unwrap(),
            110
        );
        assert_eq!(
            database.str_query(r#"{"select": {"row": "timestamp_test", "get": ["clock2"]}}"#),
            r#"Data: ["t=110"]"#
        );

        // When selecting at a specific timestamp, should get an older
        // snapshot.
        assert_eq!(
            format!("{}", database.query(
                query::Query::parse(r#"{"select": {"row": "timestamp_test", "get": ["clock2"]}}"#).unwrap(),
                105
            )),
            r#"Data: ["t=100"]"#
        );
    }

    #[test]
    fn can_write_and_restore_commit_log() {
        let mut database = super::Base::new_stub();

        // Write some stuff to the memtable and commit log.
        assert_eq!(
            database.str_query(r#"{"insert": {"row": "my_test_row","set": {"status": "OK"}}}"#),
            format!("{}", query::QueryResult::Done)
        );

        // Kill the memtable.
        mem::replace(&mut database.memtable, mtable::MTable::new());

        // Now the data shouldn't be available.
        assert_eq!(
            database.str_query(r#"{"select": {"row": "my_test_row","get": ["status"]}}"#),
            format!("{}", query::QueryResult::RowNotFound)
        );

        // Load the memtable back up via the commit log.
        database.load_mtable().unwrap();

        assert_eq!(
            database.str_query(r#"{"select": {"row": "my_test_row","get": ["status"]}}"#),
            r#"Data: ["OK"]"#
        );
    }

    #[test]
    fn test_cases() {
        let mut database = super::Base::new_stub();

        let entries = glob(&format!("./src/testcases/*.txt")).unwrap();
        for entry in entries {
            let data_path = entry.unwrap();
            let mut f = io::BufReader::new(fs::File::open(&data_path.to_str().unwrap()).unwrap());

            loop {
                let mut command = String::new();
                let mut result  = String::new();
                match f.read_line(&mut command) {
                    Ok(s)   => s,
                    Err(_)  => break
                };

                // In case we leave some blank lines, don't fail.
                if command.trim() == "" {
                    break;
                }

                f.read_line(&mut result).unwrap();

                assert_eq!(
                    database.str_query(command.as_str().trim()),
                    result.trim()
                );
            }
        }

    }
}
