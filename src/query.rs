/*
    query.rs

    This library parses queries from strings and creates
    query objects.
*/

use std::fmt;

use std::collections::BTreeMap as Map;

use mtable;

#[derive(Debug)]
pub enum QError {
    ParseError,
    NotAllowed,
}

struct QueryJSON {
    key: String,
    select: Vec<String>,
    update: Map<String, String>,
    insert: Map<String, String>
}

#[derive(Serialize, Deserialize, Debug)]
enum QueryAction {
    Select { columns: Vec<String> },
    Update { updates: Vec<mtable::MUpdate> },
    Insert { updates: Vec<mtable::MUpdate> },
}

impl QueryAction {
    fn SelectFromColumns(columns: &[&str]) -> QueryAction {
        QueryAction::Select{ columns: columns.iter().map(|s| s.to_string()).collect() }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Query {
    key: String,
    action: QueryAction
}

impl Query {
    pub fn new(key: &str, action: QueryAction) -> Query {
        Query{
            key: key.to_string(),
            action: action
        }
    }

    // This function parses an arbitrary string and returns
    // a query or an error.
    pub fn parse(input: &str) -> Result<Query, QError> {
        Ok(Query{
            key: String::from("row"),
            action: QueryAction::Select{ columns: vec![String::from("test")] }
        })
    }
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {

        fn express_updates(ups: &[mtable::MUpdate]) -> String {
            ups.iter()
               .map(|m| format!("{} = {:?}", m.key, m.value))
               .collect::<Vec<_>>()
               .join(", ")
        }

        let action = match self.action {
            QueryAction::Select{columns: ref cols } =>
                format!("SELECT {}", cols.join(", ")),
            QueryAction::Update{updates: ref ups } =>
                format!("UPDATE {}", express_updates(ups)),
            QueryAction::Insert{updates: ref ups } =>
                format!("INSERT {}", express_updates(ups))
        };

        write!(f, "{} WHERE KEY = {}", action, self.key)
    }
}

#[cfg(test)]
mod tests {
    use std;
    use mtable;

    #[test]
    fn can_print_select() {
        let mut q = super::Query::new(
            "row1",
            super::QueryAction::SelectFromColumns(&["test", "column2", "col3"])
        );

        assert_eq!(
            format!("{}", q),
            "SELECT test, column2, col3 WHERE KEY = row1"
        )
    }

    #[test]
    fn can_print_update() {
        let mut q = super::Query::new("row1",
            super::QueryAction::Update{
                updates: vec![mtable::MUpdate::new("test", vec![1, 2])]
            }
        );

        assert_eq!(
            format!("{}", q),
            "UPDATE test = [1, 2] WHERE KEY = row1"
        );
    }

    #[test]
    fn can_parse_query() {
        super::Query::parse("select: {test, west} where key = fest").unwrap();
    }
}
