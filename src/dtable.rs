use generated::dtable::DEntry as DEntry;
use generated::dtable::DColumn as DColumn;
use generated::dtable::DRow as DRow;

impl DColumn {
    pub fn get_value(&self) -> Option<&[u8]> {
        let entries = self.get_entries();
        match entries.len() {
            0 => None,
            n => Some(entries[n-1].get_value())
        }
    }
}

impl DRow {
    pub fn get_column(&self, key: &str) -> Option<&DColumn> {
        println!("searching for column: {}", key);
        let keys = self.get_keys();
        let mut l: i32 = 0;
        let mut r: i32 = keys.len() as i32 - 1;

        while l <= r {
            let index = (l + r) >> 1;
            println!("index = {}", index);
            match &keys[index as usize] {
                k if key == k => return Some(&self.get_columns()[index as usize]),
                k if key > k  => l = index + 1,
                _             => r = index - 1
            }
        }
        return None;
    }

    pub fn get_value(&self, key: &str) -> Option<&[u8]> {
        return match self.get_column(key) {
            Some(col) => col.get_value(),
            None      => None
        };
    }
}
