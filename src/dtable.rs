#[derive(Debug)]
pub struct DEntry {
    timestamp: i64,
    value: Vec<u8>,
}

#[derive(Debug)]
pub struct DColumn {
    key: String,
    offset: i64,
    entries: Vec<DEntry>,
}

#[derive(Debug)]
pub struct DRow {
    key: String,
    offset: i64,
    columns: Vec<DColumn>,
}

#[derive(Debug)]
pub struct DTable {
    filename: String,
    rows: Vec<DRow>
}

impl DTable {
    pub fn from_vec(v: Vec<String>) -> DTable {
        DTable{
            filename: String::from("1.table"),
            rows: v.into_iter()
                    .map(|s| DRow{
                        key: s,
                        offset: 0,
                        columns: vec![]
                    })
                    .collect::<Vec<DRow>>()
        }
    }

    pub fn get_value(&self, row: String, column: String, timestamp: i64) -> Option<&[u8]> {
        return match self.get_row(row) {
            Some(r) => r.get_value(column, timestamp),
            None    => None
        }
    }

    #[allow(dead_code)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let output = vec![];

        return output;
    }

    pub fn get_row(&self, key: String) -> Option<&DRow> {
        let mut i = 1;
        let length = self.rows.len();
        let mut offset = length >> 1;
        while (length >> i) > 0 {
            i += 1;
            if key == self.rows[offset].key {
                return Some(&self.rows[offset]);
            }
            else if key > self.rows[offset].key {
                offset += length >> i;
            }
            else {
                offset -= length >> i;
            }
        }
        return None;
    }
}

impl DRow {
    pub fn get_column(&self, key: String) -> Option<&DColumn> {
        let mut i = 1;
        let length = self.columns.len();
        let mut offset = length >> 1;
        while (length >> i) > 0 {
            i += 1;
            if key == self.columns[offset].key {
                return Some(&self.columns[offset])
            }
            else if key > self.columns[offset].key {
                offset += length >> i;
            }
            else {
                offset -= length >> i;
            }
        }
        return None;
    }

    pub fn get_value(&self, column: String, timestamp: i64) -> Option<&[u8]> {
        return match self.get_column(column) {
            Some(c) => c.get_value(timestamp),
            None    => None
        }
    }
}

impl DColumn {
    pub fn get_value(&self, timestamp: i64) -> Option<&[u8]> {
        return None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_create_and_probe() {
        let d = DTable::from_vec(vec![
            String::from("apples"),
            String::from("bananas"),
            String::from("oranges"),
            String::from("yams")
        ]);

        let yams = d.get_row(String::from("yams"));
        assert_eq!(yams.unwrap().offset, 0);

        match d.get_row(String::from("clams")) {
            Some(_) => panic!("Shouldn't get a value."),
            None    => ()
        }

        match d.get_value(String::from("apples"), String::from("nutrition"), 10) {
            Some(_) => panic!("Shouldn't get a value."),
            None    => ()
        }
    }
}
