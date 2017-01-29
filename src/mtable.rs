/*
    memtable.rs

    The memtable is basically an SSTable in memory.
*/

use std::collections::BTreeMap;
use std::io;

use protobuf::Message;

use generated::dtable::DEntry as DEntry;
use byteorder::{LittleEndian, ReadBytesExt};

struct MUpdate {
    value: Vec<u8>,
    key: String
}

struct MColumn {
    values: Vec<DEntry>
}

struct MRow {
    columns: BTreeMap<String, MColumn>
}

struct MTable {
    rows: BTreeMap<String, MRow>
}

impl MColumn {
    fn write_to_writer(&self, w: &mut io::Write) -> Result<u64, io::Error> {

    }
}

impl MTable {
    fn new() -> MTable {
        return MTable{rows: BTreeMap::new()};
    }

    pub fn update(&mut self, row: String, updates: Vec<MUpdate>) -> Result<(), io::Error>{
        return match self.rows.get_mut(&row) {
            None    => Err(io::Error::new(io::ErrorKind::NotFound, "No such row.")),
            Some(r) => Ok(r.update(updates))
        };
    }

    pub fn insert(&mut self, row: String, updates: Vec<MUpdate>) {
        let r = MRow{
            columns: updates.into_iter().map(|update| {
                let mut e = DEntry::new();
                e.set_timestamp(100);
                e.set_value(update.value);
                (update.key, MColumn{
                    values: vec![e]
                })
            }).collect()
        };
        self.rows.insert(row, r);
    }

    pub fn select(&self, row: String, column: String) -> Option<&[u8]> {
        match self.rows.get(&row) {
            Some(r) => {
                match r.columns.get(&column) {
                    Some(c) => match c.values.len() {
                        0 => None,
                        n => Some(&c.values[n-1].value)
                    },
                    None => None
                }
            }
            None => None
        }
    }
}

impl MRow {
    fn update(&mut self, updates: Vec<MUpdate>) {
        for update in updates {
            match self.columns.get_mut(&update.key) {
                Some(col) => {
                    let mut e = DEntry::new();
                    e.set_timestamp(100);
                    e.set_value(update.value);
                    col.values.push(e);
                    continue;
                },
                None    => ()
            }

            let mut e = DEntry::new();
            e.set_timestamp(100);
            e.set_value(update.value);

            self.columns.insert(update.key,
                MColumn{
                    values: vec![e]
                }
            );
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn can_insert() {
        let mut m = super::MTable::new();

        m.insert(String::from("colin"), vec![super::MUpdate{
            key: String::from("marfans"),
            value: vec![1]
        }]);

        m.update(String::from("colin"), vec![super::MUpdate{
            key: String::from("friends"),
            value: vec![12,23]
        }]).unwrap();

        // Limited scope, so we free the borrowed terms.
        {
            let has_disease = m.select(String::from("colin"), String::from("marfans")).unwrap();
            assert_eq!(has_disease[0], 1);
        }

        match m.select(String::from("colin"), String::from("marfonzo")) {
            Some(_) => panic!("Shouldn't have disease"),
            None    => ()
        }

        m.update(String::from("colin"), vec![super::MUpdate{
            key: String::from("marfans"),
            value: vec![0]
        }]).unwrap();

        {
            let has_disease = m.select(String::from("colin"), String::from("marfans")).unwrap();
            assert_eq!(has_disease[0], 0);
        }
    }
}
