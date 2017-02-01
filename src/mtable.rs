/*
    memtable.rs

    The memtable is basically an SSTable in memory.
*/

use std::collections::BTreeMap;
use std::iter::FromIterator;
use std::io;
use std::str::FromStr;
use std::fmt;

use protobuf;
use protobuf::Message;

use generated::dtable::DEntry as DEntry;
use generated::dtable::DColumn as DColumn;
use generated::dtable::DRow as DRow;
use generated::dtable::DTableHeaderEntry as DTableHeaderEntry;
use generated::dtable::DTableHeader as DTableHeader;

struct MUpdate<'a> {
    value: Vec<u8>,
    key: &'a str
}

struct MRow<'a> {
    columns: BTreeMap<&'a str, DColumn>
}

struct MTable<'a> {
    rows: BTreeMap<&'a str, MRow<'a>>
}

impl <'a> MRow<'a> {
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

        return Ok(drow.get_cached_size() as u64);
    }
}

impl <'a> fmt::Display for MRow<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "MRow: {{ {} }}",
                self.columns
                .iter()
                .map(|(k, v)| format!("{}: {:?}", k, v.get_value().unwrap()))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl <'a> MTable<'a> {
    fn new() -> MTable<'a> {
        return MTable{rows: BTreeMap::new()};
    }

    pub fn update(&mut self, row: &'a str, updates: Vec<MUpdate<'a>>) -> Result<(), io::Error>{
        return match self.rows.get_mut(&row) {
            None    => Err(io::Error::new(io::ErrorKind::NotFound, "No such row.")),
            Some(r) => Ok(r.update(updates))
        };
    }

    pub fn get_row(&self, row: &'a str) -> Option<&MRow> {
        self.rows.get(&row)
    }

    pub fn insert(&mut self, row: &'a str, updates: Vec<MUpdate<'a>>) {
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

    pub fn select(&'a self, row: &'a str, column: &'a str) -> Option<&[u8]> {
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

    pub fn write_to_writer(&self, data: &mut io::Write, header: &mut io::Write) -> Result<u64, io::Error> {
        let mut headers = vec![];
        let mut offset = 0;
        for (key, row) in &self.rows {
            let length = row.write_to_writer(data)?;
            let mut h = DTableHeaderEntry::new();
            h.set_offset(offset);
            h.set_key(String::from_str(key).unwrap());
            headers.push(h);
            offset += length;
        }

        let mut table_header = DTableHeader::new();
        table_header.set_entries(protobuf::RepeatedField::from_vec(headers));

        table_header.write_to_writer(header)?;

        return Ok(offset);
    }
}

impl <'a> MRow<'a> {
    fn update(&mut self, updates: Vec<MUpdate<'a>>) {
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
    use dtable;

    #[test]
    fn can_insert_and_retrieve() {
        let mut m = super::MTable::new();

        m.insert("colin", vec![super::MUpdate{
            key: "marfans",
            value: vec![1]
        }]);

        m.update("colin", vec![super::MUpdate{
            key: "friends",
            value: vec![12,23]
        }]).unwrap();

        // Limited scope, so we free the borrowed terms.
        {
            let has_disease = m.select("colin", "marfans").unwrap();
            assert_eq!(has_disease[0], 1);
        }

        match m.select("colin", "marfonzo") {
            Some(_) => panic!("Shouldn't have disease"),
            None    => ()
        }

        m.update("colin", vec![super::MUpdate{
            key: "marfans",
            value: vec![0]
        }]).unwrap();

        m.update("colin", vec![super::MUpdate{
            key: "christmas",
            value: vec![44]
        }]).unwrap();

        m.update("colin", vec![super::MUpdate{
            key: "mormons",
            value: vec![0]
        }]).unwrap();

        m.update("colin", vec![super::MUpdate{
            key: "jesus",
            value: vec![66]
        }]).unwrap();

        {
            let has_disease = m.select("colin", "marfans").unwrap();
            assert_eq!(has_disease[0], 0);
        }

        let mut f = std::fs::File::create("./data/state.bin").unwrap();

        match m.get_row("colin") {
            Some(r) => {
                r.write_to_writer(&mut f).unwrap();
                ()
            },
            None    => panic!("Should be able to get + write row."),
        }

        let mut g = std::fs::File::open("./data/state.bin").unwrap();

        let row = protobuf::parse_from_reader::<DRow>(&mut g).unwrap();
        assert_eq!(["christmas", "friends", "jesus", "marfans", "mormons"], row.get_keys());

        row.get_value("marfans").unwrap();
        row.get_value("mormons").unwrap();
        row.get_value("friends").unwrap();
        row.get_value("jesus").unwrap();
        row.get_value("christmas").unwrap();
        row.get_value("clapton").unwrap_err();
        assert_eq!(row.get_value("jesus").unwrap(), &[66]);
    }

    #[test]
    fn can_convert_mtable_to_dtable() {
        let mut m = super::MTable::new();
        m.insert("colin", vec![super::MUpdate{
            key: "marfans",
            value: vec![1]
        }]);

        m.insert("rebecca", vec![super::MUpdate{
            key: "marfans",
            value: vec![0]
        }]);

        m.update("rebecca", vec![super::MUpdate{
            key: "height",
            value: vec![3]
        }]).unwrap();

        m.update("colin", vec![super::MUpdate{
            key: "height",
            value: vec![5]
        }]).unwrap();

        assert_eq!(
            m.select("colin", "marfans").unwrap(),
            &[1]
        );

        println!("{}", m.get_row("colin").unwrap());

        // Now write the MTable to a file.
        let mut data = std::fs::File::create("./data/test.dtable").unwrap();
        let mut head = std::fs::File::create("./data/data.dheader").unwrap();

        m.write_to_writer(&mut data, &mut head).unwrap();

        // Now construct a DTable from the MTable and query it.
        let header = std::fs::File::open("./data/data.dheader").unwrap();
        let mut d = dtable::DTable::new(
            String::from("./data/test.dtable"),
            header
        ).unwrap();

        println!("{}", d.get_row("colin").unwrap());

        assert_eq!(
            d.select("colin", "marfans").unwrap(),
            &[1]
        );

    }
}
