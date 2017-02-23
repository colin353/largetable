/*
    mtable.rs

    The MTable is basically a mutable DTable in memory.
*/

use std::io;
use std::fmt;
use std::str::FromStr;
use std::u64;
use std::collections::BTreeMap;
use std::iter::FromIterator;

use protobuf;
use protobuf::Message;

use generated::dtable::*;
use dtable;
use query::MUpdate;

pub type TOption = Option<Vec<Option<DEntry>>>;

pub struct MRow {
    columns: BTreeMap<String, DColumn>
}

pub struct MTable {
    rows: BTreeMap<String, MRow>
}

impl MRow {
    fn write_to_writer(&self, w: &mut io::Write) -> Result<u64, io::Error> {
        // First, construct a DRow using this MRow, then
        // write out that DRow using write_to_writer.
        let mut drow = DRow::new();
        drow.set_columns(protobuf::RepeatedField::from_iter(
            self.columns.iter().map(|(_, value)| value.clone())
        ));

        // Next, construct the DRow lookup table. One DRow is intended
        // to be read into memory in a single read, then binary search
        // is used to find the columns to probe using the lookup table.
        drow.set_keys(protobuf::RepeatedField::from_iter(
            self.columns.iter().map(|(key, _)| String::from_str(key).unwrap())
        ));

        drow.write_to_writer(w)?;

        Ok(drow.get_cached_size() as u64)
    }
}

impl fmt::Display for MRow {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "MRow: {{ {} }}",
                self.columns
                .iter()
                .map(|(k, v)| format!("{}: {:?}", k, v.get_latest_value().unwrap().get_value()))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl MTable {
    pub fn new() -> MTable {
        MTable{rows: BTreeMap::new()}
    }

    pub fn update(&mut self, row: &str, updates: &[MUpdate], timestamp: u64) -> Result<(), dtable::TError>{
        match self.rows.get_mut(row) {
            None    => (),
            Some(r) => return Ok(r.update(updates, timestamp))
        };

        self.insert(row, updates, timestamp)
    }

    pub fn get_row(&self, row: &str) -> Option<&MRow> {
        self.rows.get(row)
    }

    pub fn insert(&mut self, row: &str, updates: &[MUpdate], timestamp: u64) -> Result<(), dtable::TError> {
        if self.rows.get(row).is_some() {
            return Err(dtable::TError::AlreadyExists);
        }

        let r = MRow{
            columns: updates.into_iter().map(|update| {
                let mut e = DEntry::new();
                e.set_timestamp(timestamp);
                e.set_value(update.value.clone());

                let mut c = DColumn::new();
                c.set_entries(protobuf::RepeatedField::from_vec(vec![e]));

                (update.key.to_string(), c)

            }).collect()
        };
        self.rows.insert(row.to_string(), r);

        Ok(())
    }

    #[cfg(test)]
    pub fn select_one(&self, row: &str, col: &str) -> Option<DEntry> {
        match self.select(row, &[col], ::std::u64::MAX) {
            Some(ref result) => match result[0] {
                Some(ref value) => {
                    Some(value.clone())
                }
                None => None
            },
            None => None
        }
    }

    pub fn select(&self, row: &str, cols: &[&str], timestamp: u64) -> TOption {
        let r = match self.rows.get(row) {
            Some(r) => r,
            None    => return None
        };

        Some(cols.iter()
                 .map(|column| match r.columns.get(*column) {
                    Some(c) => c.get_value(timestamp).ok(),
                    None => None
            }).collect::<Vec<_>>()
        )
    }

    pub fn write_to_writer(&self, data: &mut io::Write, header: &mut io::Write) -> Result<DTableHeader, io::Error> {
        let mut headers = vec![];
        let mut offset = 0;
        let mut count = 0;
        for (key, row) in &self.rows {
            let length = row.write_to_writer(data)?;
            let mut h = DTableHeaderEntry::new();
            h.set_offset(offset);
            h.set_key(String::from_str(key).unwrap());
            headers.push(h);
            offset += length;
            count += 1;
        }

        println!("Diagnostic: wrote {} lines in mtable to dtable.", count);

        let mut table_header = DTableHeader::new();
        table_header.set_entries(protobuf::RepeatedField::from_vec(headers));

        table_header.write_to_writer(header)?;

        Ok(table_header)
    }
}

impl MRow {
    fn update(&mut self, updates: &[MUpdate], timestamp: u64) {
        for update in updates {
            if let Some(col) = self.columns.get_mut(&*update.key) {
                let mut e = DEntry::new();
                e.set_timestamp(timestamp);
                e.set_value(update.value.clone());

                // We need to make sure we are inserting it at the
                // correct point. We'll start from the end of the array
                // and search backward until we see a number less than our
                // target timestamp, and then insert at that index.
                let mut entries = col.mut_entries();
                let mut insertion_index = 0;
                for (index, value) in entries.iter().enumerate().rev() {
                    if value.get_timestamp() <= timestamp {
                        insertion_index = index + 1;
                        break;
                    }
                }
                entries.insert(insertion_index, e);
                continue;
            }

            let mut e = DEntry::new();
            e.set_timestamp(timestamp);
            e.set_value(update.value.clone());

            let mut c = DColumn::new();
            c.set_entries(protobuf::RepeatedField::from_vec(vec![e]));

            self.columns.insert(update.key.clone(), c);
        }
    }
}

#[cfg(test)]
mod tests {
    use std;
    use protobuf;
    use generated::dtable::DRow as DRow;
    use dtable;
    use time;

    #[test]
    fn can_print_mrow() {
        let mut m = super::MTable::new();
        m.insert("rowname", &[
            super::MUpdate::new("attr1", vec![1,2,3]),
            super::MUpdate::new("attr2", vec![4,5,6])
        ], time::precise_time_ns()).unwrap();
        assert_eq!(
            format!("{}", m.get_row("rowname").unwrap()),
            "MRow: { attr1: [1, 2, 3], attr2: [4, 5, 6] }"
        )
    }

    #[test]
    fn can_insert_update_and_select() {
        let mut m = super::MTable::new();

        m.insert("colin", &[super::MUpdate::new(
            "asdf",
            vec![1]
        )], time::precise_time_ns()).unwrap();

        m.update("colin", &[super::MUpdate::new(
            "asdf",
            vec![5]
        )], time::precise_time_ns()).unwrap();

        m.update("colin", &[super::MUpdate::new(
            "fdsa",
            vec![12,23]
        )], time::precise_time_ns()).unwrap();

        assert_eq!(m.select_one("colin", "asdf").unwrap().get_value(), &[5]);
        assert_eq!(m.select_one("colin", "fdsa").unwrap().get_value(), &[12,23]);
        assert!(m.select_one("colin", "fake").is_none());
    }

    #[test]
    fn can_read_and_write_mrow() {
        let mut m = super::MTable::new();

        // This is just a list of random words to insert as columns
        // into the table.
        let w = vec![
            "seed", "load", "performance", "premium", "heap", "momentous",
            "harmony", "bell", "true", "imperfect", "towering", "icy", "belong"
        ];
        // Insert an empty row.
        m.insert("colin", &[], time::precise_time_ns()).unwrap();

        // Write all of the columns to the table.
        m.update(
            "colin",
            w.iter()
            .enumerate()
            .map(|(index, value)| super::MUpdate::new(
                value, vec![index as u8]
            )).collect::<Vec<_>>().as_slice(),
            time::precise_time_ns()
        ).unwrap();

        // Write the MRow to a file.
        let mut f = std::fs::File::create("./data/state.bin").unwrap();
        m.get_row("colin").unwrap().write_to_writer(&mut f).unwrap();

        // Read the MRow back from the file.
        let mut g = std::fs::File::open("./data/state.bin").unwrap();
        let row = protobuf::parse_from_reader::<DRow>(&mut g).unwrap();

        // Check that every entry in the list of original words is
        // correctly inserted into the row.
        for (index, value) in w.iter().enumerate() {
            assert_eq!(
                row.get_latest_value(value).unwrap().get_value(),
                &[index as u8]
            )
        }

        // Check that invalid entries are not present.
        row.get_latest_value("clapton").unwrap_err();
    }

    #[test]
    fn can_convert_mtable_to_dtable() {
        let mut m = super::MTable::new();

        // Create a bunch of random strings.
        let x = vec![
            "790123889", "5378035978", "7329395933", "7556669891", "8317521945",
            "5473915008", "0540417761", "3783421087", "5583364306", "6454289889"
        ];
        let y = vec![
            "3855519000", "693463382", "0309758752", "6492176736", "9273285817",
            "2847849405", "5745075665", "1626955318", "0691323875", "0694793474"
        ];

        // Insert the strings as columns into two rows in the database.
        m.insert(
            "row1",
            x.iter()
            .enumerate()
            .map(|(index, word)| super::MUpdate::new(word, vec![index as u8]))
            .collect::<Vec<_>>().as_slice(),
            time::precise_time_ns()
        ).unwrap();

        m.insert(
            "row2",
            y.iter()
            .enumerate()
            .map(|(index, word)| super::MUpdate::new(word, vec![index as u8]))
            .collect::<Vec<_>>().as_slice(),
            time::precise_time_ns()
        ).unwrap();

        // Now write the MTable to a file.
        let mut data = std::fs::File::create("./data/0.dtable").unwrap();
        let mut head = std::fs::File::create("./data/0.dtable.header").unwrap();
        m.write_to_writer(&mut data, &mut head).unwrap();

        // Now construct a DTable from the MTable and query it.
        let header = std::fs::File::open("./data/0.dtable.header").unwrap();
        let d = dtable::DTable::new(
            String::from("./data/0.dtable"),
            header
        ).unwrap();

        // Check for existence of columns and correct values.
        for (index, word) in y.iter().enumerate() {
            println!("Exists: {}?", word);
            assert_eq!(
                d.select_one("row2", word).unwrap(),
                &[index as u8]
            );
        }

        // Make sure that when we search for non-existant columns
        // we don't get any problems.
        for word in y {
            assert!(d.select_one("row1", word).is_none());
        }

        // Double-check that the format string looks correct.
        assert_eq!(
            format!("{}", d.get_row("row1").unwrap()),
            "DRow: { 0540417761: [6], 3783421087: [7], 5378035978: [1], 5473915008: [5], 5583364306: [8], 6454289889: [9], 7329395933: [2], 7556669891: [3], 790123889: [0], 8317521945: [4] }"
        );
        assert_eq!(
            format!("{}", d.get_row("row2").unwrap()),
            "DRow: { 0309758752: [2], 0691323875: [8], 0694793474: [9], 1626955318: [7], 2847849405: [5], 3855519000: [0], 5745075665: [6], 6492176736: [3], 693463382: [1], 9273285817: [4] }"
        );
    }
}
