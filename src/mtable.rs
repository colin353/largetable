/*
    memtable.rs

    The memtable is basically an SSTable in memory.
*/

use std::collections::BTreeMap;
use std::iter::FromIterator;
use std::io;

use std::fs::File;

use protobuf;
use protobuf::Message;
use protobuf::RepeatedField;

use generated::dtable::DEntry as DEntry;
use generated::dtable::DColumn as DColumn;
use generated::dtable::DRow as DRow;
use byteorder::{LittleEndian, ReadBytesExt};

struct MUpdate {
    value: Vec<u8>,
    key: String
}

struct MRow {
    columns: BTreeMap<String, DColumn>
}

struct MTable {
    rows: BTreeMap<String, MRow>
}

impl MRow {
    fn write_to_writer(&self, w: &mut io::Write) -> Result<u64, io::Error> {
        // First, construct a DRow using this MRow, then
        // write out that DRow using write_to_writer.
        let mut drow = DRow::new();
        drow.set_columns(protobuf::RepeatedField::from_iter(
            self.columns.iter().map(|(key, value)| value.clone())
        ));

        // Next, construct the DRow lookup table. One DRow is intended
        // to be read into memory in a single read, then binary search
        // is used to find the columns to probe using the lookup table.
        drow.set_keys(protobuf::RepeatedField::from_iter(
            self.columns.iter().map(|(key, value)| key.clone())
        ));

        drow.write_to_writer(w)?;

        return Ok(0);
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

    pub fn get_row(&self, row: String) -> Option<&MRow> {
        self.rows.get(&row)
    }

    pub fn insert(&mut self, row: String, updates: Vec<MUpdate>) {
        let r = MRow{
            columns: updates.into_iter().map(|update| {
                let mut e = DEntry::new();
                e.set_timestamp(100);
                e.set_value(update.value);

                let mut c = DColumn::new();
                c.set_entries(protobuf::RepeatedField::from_vec(vec![e]));

                (update.key, c)

            }).collect()
        };
        self.rows.insert(row, r);
    }

    pub fn select(&self, row: String, column: String) -> Option<&[u8]> {
        match self.rows.get(&row) {
            Some(r) => {
                match r.columns.get(&column) {
                    Some(c) => match c.get_entries().last() {
                        Some(e) => Some(&e.value),
                        None => None
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
                    col.mut_entries().push(e);
                    continue;
                },
                None    => ()
            }

            let mut e = DEntry::new();
            e.set_timestamp(100);
            e.set_value(update.value);

            let mut c = DColumn::new();
            c.set_entries(protobuf::RepeatedField::from_vec(vec![e]));

            self.columns.insert(update.key, c);
        }
    }
}

#[cfg(test)]
mod tests {
    use std;
    use protobuf;
    use generated::dtable::DRow as DRow;

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

        let mut f = std::fs::File::create("./data/state.bin").unwrap();

        match m.get_row(String::from("colin")) {
            Some(r) => {
                r.write_to_writer(&mut f);
                ()
            },
            None    => panic!("Should be able to get + write row."),
        }

        let mut g = std::fs::File::open("./data/state.bin").unwrap();

        let row = protobuf::parse_from_reader::<DRow>(&mut g).unwrap();
        assert_eq!(["friends", "marfans"], row.get_keys());
    }
}
