use std::{
    fmt,
    fs::File,
    io::{BufRead, BufReader},
};

// This gives us sorting for free (?)
#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
enum Value {
    Int(i64),
    Str(String),
    Bool(bool),
    Null,
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Int(v) => write!(f, "{v}"),
            Value::Str(s) => write!(f, "{s}"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Null => write!(f, "NULL"),
        }
    }
}

#[derive(Clone, Debug)]
enum Expr {
    Column(String),
    Literal(Value),
    Eq(Box<Expr>, Box<Expr>),
    // TODO: Exercise for the reader
    // NotEq(Box<Expr>, Box<Expr>),
}

impl Expr {
    /// Evaluate expression against a row using its schema.
    pub fn eval(&self, row: &[Value], schema: &[String]) -> Value {
        match self {
            Expr::Column(name) => {
                let idx = schema.iter().position(|c| c.eq_ignore_ascii_case(name));
                match idx {
                    Some(i) => row[i].clone(),
                    None => Value::Null,
                }
            }
            Expr::Literal(v) => v.clone(),
            Expr::Eq(l, r) => {
                let lv = l.eval(row, schema);
                let rv = r.eval(row, schema);
                Value::Bool(eq_values(&lv, &rv))
            }
        }
    }
}

fn eq_values(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Int(x), Value::Int(y)) => x == y,
        (Value::Str(x), Value::Str(y)) => x == y,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        (Value::Null, Value::Null) => true,
        _ => false,
    }
}

#[derive(Debug)]
struct CSVFileScan {
    reader: BufReader<File>,
    /// All columns in file; discovered on startup
    columns: Vec<String>,
}

impl CSVFileScan {
    fn new(filename: String) -> CSVFileScan {
        let file = File::open(filename).unwrap();
        let mut reader = BufReader::new(file);

        // Assume first line is the header, and get the column names
        let mut buf = String::new();
        reader.read_line(&mut buf).unwrap();
        let strs: Vec<&str> = buf.split(',').into_iter().collect();
        let columns: Vec<String> = strs.into_iter().map(|s| s.trim().to_string()).collect();

        CSVFileScan { reader, columns }
    }

    fn next(&mut self) -> Option<Vec<Value>> {
        // Read line
        let mut buf = String::new();
        self.reader.read_line(&mut buf).unwrap(); // TODO

        // If line is empty, assume we've reached the end of the file
        if buf.is_empty() {
            return None;
        }

        // Split line into values
        let strs: Vec<&str> = buf.split(',').into_iter().collect();

        // Parse into values
        let values: Vec<Value> = strs
            .into_iter()
            .map(|s| {
                // Remove whitespace
                let s = s.trim();

                // TODO: Infer and use schema, but for now try to parse as values

                // Attempt to parse as int
                match s.parse() {
                    Ok(int_value) => return Value::Int(int_value),
                    Err(_) => {}
                };
                // Attempt to parse as bool
                match s.parse() {
                    Ok(bool_value) => return Value::Bool(bool_value),
                    Err(_) => {}
                };

                // Return a string
                Value::Str(s.to_string())
            })
            .collect();
        Some(values)
    }

    /// The schema of the file (the columns)
    /// TODO: Add types?
    fn schema(&self) -> Vec<String> {
        self.columns.clone()
    }
}

struct Limit {
    limit: i32,
    offset: i32,

    /// Next row to emit. 0 until we've skipped past `offset` number of rows.
    current: i32,
    input: Box<Node>,
}

impl Limit {
    fn new(limit: i32, offset: i32, input: Box<Node>) -> Limit {
        Limit {
            limit,
            offset,
            current: 0,
            input,
        }
    }

    fn next(&mut self) -> Option<Vec<Value>> {
        // Loop until we don't have any rows to skip.
        while self.offset > 0 {
            let _ = self.input.as_mut().next();
            self.offset -= 1;
        }
        // At this point we've skipped `self.offset` rows.
        // We will now emit up to `self.limit` rows.
        if self.current >= self.limit {
            None
        } else {
            self.current += 1;
            self.input.as_mut().next()
        }
    }

    fn schema(&self) -> Vec<String> {
        self.input.as_ref().schema()
    }
}

struct Select {
    predicate: Expr,
    input: Box<Node>,
}

impl Select {
    fn new(predicate: Expr, input: Box<Node>) -> Select {
        Select { predicate, input }
    }

    fn next(&mut self) -> Option<Vec<Value>> {
        let schema = self.schema();
        // Loops until input is empty; returns first match
        while let Some(row) = self.input.next() {
            match self.predicate.eval(&row, &schema) {
                Value::Bool(true) => return Some(row),
                Value::Bool(false) | Value::Null => {
                    // Keep scanning.
                }
                other => todo!("select: unhandled {:?}", other),
            }
        }
        None
    }

    fn schema(&self) -> Vec<String> {
        self.input.as_ref().schema()
    }
}

struct Project {
    columns: Vec<String>,
    input: Box<Node>,
}

impl Project {
    fn new(columns: Vec<String>, input: Box<Node>) -> Project {
        Project { columns, input }
    }

    fn next(&mut self) -> Option<Vec<Value>> {
        // Figure out which columns to select
        let mut select_indices: Vec<usize> = vec![];
        for select_column in self.columns.clone() {
            for (i, input_column) in self.input.schema().iter().enumerate() {
                if *input_column == *select_column {
                    select_indices.push(i);
                }
            }
        }

        // Loop over
        self.input.next().map(|row| {
            let mut out = vec![];
            for select_idx in select_indices.clone() {
                for (i, value) in row.clone().into_iter().enumerate() {
                    if i == select_idx {
                        out.push(value.clone());
                    }
                }
            }
            out
        })
    }

    fn schema(&self) -> Vec<String> {
        self.columns.clone()
    }
}

struct Sort {
    by: Box<Expr>,
    input: Box<Node>,
    is_sorted: bool,
    rows: Vec<Vec<Value>>,
    rows_idx: usize,
}

impl Sort {
    fn new(by: Box<Expr>, input: Box<Node>) -> Sort {
        Sort {
            by,
            input,
            is_sorted: false,
            rows: vec![],
            rows_idx: 0,
        }
    }

    fn next(&mut self) -> Option<Vec<Value>> {
        // If not sorted yet, sort
        if !self.is_sorted {
            if self.rows.len() != 0 {
                panic!("Is not sorted but buffer is not empty!");
            }
            if self.rows_idx != 0 {
                panic!("Is not sorted but rows_idx != 0!");
            }
            while let Some(row) = self.input.next() {
                self.rows.push(row);
            }
            let schema = &self.schema();
            self.rows.sort_by_key(|row| self.by.eval(row, &schema));
            self.is_sorted = true;
        }

        // Return rows from buffer until empty
        if self.rows_idx >= self.rows.len() - 1 {
            None
        } else {
            let to_return = self.rows[self.rows_idx].clone();
            self.rows_idx += 1;
            Some(to_return)
        }
    }

    fn schema(&self) -> Vec<String> {
        self.input.as_ref().schema()
    }
}

enum Node {
    CSVFileScan(CSVFileScan),
    Select(Select),
    Project(Project),
    Limit(Limit),
    Sort(Sort),
}

impl Node {
    fn next(&mut self) -> Option<Vec<Value>> {
        match self {
            Node::CSVFileScan(scan) => scan.next(),
            Node::Project(project) => project.next(),
            Node::Limit(limit) => limit.next(),
            Node::Select(select) => select.next(),
            Node::Sort(sort) => sort.next(),
        }
    }

    fn schema(&self) -> Vec<String> {
        match self {
            Node::CSVFileScan(scan) => scan.schema(),
            Node::Project(project) => project.schema(),
            Node::Limit(limit) => limit.schema(),
            Node::Select(select) => select.schema(),
            Node::Sort(sort) => sort.schema(),
        }
    }
}

fn main() {
    let scan_node = Node::CSVFileScan(CSVFileScan::new("values.csv".to_string()));
    let sort_by = Expr::Column("a".to_string());
    let sort_node = Node::Sort(Sort::new(Box::new(sort_by), Box::new(scan_node)));
    let predicate = Expr::Eq(
        Box::new(Expr::Column("b".to_string())),
        Box::new(Expr::Column("b".to_string())),
    );
    let select = Select::new(predicate, Box::new(sort_node));
    let select_node = Node::Select(select);
    let project_node = Node::Project(Project::new(
        vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "b".to_string(),
            "a".to_string(),
        ],
        Box::new(select_node),
    ));
    let mut limit_node = Node::Limit(Limit::new(10, 1, Box::new(project_node)));

    // Print schema
    print!("[");
    let schema = limit_node.schema();
    for (i, value) in schema.iter().enumerate() {
        print!("{}", value);
        if i < schema.len() - 1 {
            print!(",");
        }
    }
    print!("]");
    println!();
    // Execute query and print output rows
    loop {
        // println!("loop");
        match limit_node.next() {
            Some(row) => {
                print!("[");
                for (i, value) in row.iter().enumerate() {
                    print!("{}", value);
                    if i < row.len() - 1 {
                        print!(",");
                    }
                }
                print!("]");
                println!();
            }
            None => break,
        }
    }
}
