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
    /// Column names in the file, in same order as in the file
    columns: Vec<String>,
    /// Storage for the current output row
    row_buf: Vec<Value>,
    /// Reusable buffer for reading a line
    line_buf: String,
}

impl CSVFileScan {
    fn new(filename: String) -> CSVFileScan {
        let file = File::open(filename).unwrap();
        let mut reader = BufReader::new(file);

        // Assume first line is the header, and read the column names
        let mut buf = String::new();
        reader.read_line(&mut buf).unwrap();
        let strs: Vec<&str> = buf.split(',').into_iter().collect();
        let columns: Vec<String> = strs.into_iter().map(|s| s.trim().to_string()).collect();

        CSVFileScan {
            reader,
            columns,
            row_buf: Vec::new(),
            line_buf: String::new(),
        }
    }

    fn next(&mut self) -> Option<&[Value]> {
        self.line_buf.clear();
        let read_bytes = self
            .reader
            .read_line(&mut self.line_buf)
            .expect("failed to read");
        if read_bytes == 0 {
            // EOF
            return None;
        }

        self.row_buf.clear();
        for s in self.line_buf.split(',') {
            let s = s.trim();

            // TODO: Use schema, but for now try to parse as values.
            // This will fail if e.g. we have a column where some values are strings and some are ints
            if let Ok(i) = s.parse::<i64>() {
                self.row_buf.push(Value::Int(i));
            } else if let Ok(b) = s.parse::<bool>() {
                self.row_buf.push(Value::Bool(b));
            } else if s.eq_ignore_ascii_case("null") {
                self.row_buf.push(Value::Null);
            } else {
                self.row_buf.push(Value::Str(s.to_string()));
            }
        }

        Some(&self.row_buf)
    }

    /// The schema of the file (the columns)
    /// TODO: Add types
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

    fn next(&mut self) -> Option<&[Value]> {
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
    row_buf: Vec<Value>,
}

impl Select {
    fn new(predicate: Expr, input: Box<Node>) -> Select {
        Select {
            predicate,
            input,
            row_buf: Vec::new(),
        }
    }

    fn next(&mut self) -> Option<&[Value]> {
        let schema = self.schema();
        while let Some(row) = self.input.as_mut().next() {
            match self.predicate.eval(row, &schema) {
                Value::Bool(true) => {
                    self.row_buf.clear();
                    self.row_buf.extend_from_slice(row);
                    return Some(&self.row_buf);
                }
                Value::Bool(false) | Value::Null => {
                    // Keep scanning.
                }
                other => panic!("predicate should evaluate to boolean, not {:?}", other),
            }
        }
        None
    }

    fn schema(&self) -> Vec<String> {
        self.input.as_ref().schema()
    }
}

struct Project {
    /// Vector of expressions to project and their aliases
    exprs: Vec<(Expr, String)>,
    input: Box<Node>,
    row_buf: Vec<Value>,
}

impl Project {
    fn new(exprs: Vec<(Expr, String)>, input: Box<Node>) -> Project {
        Project {
            exprs,
            input,
            row_buf: Vec::new(),
        }
    }

    fn next(&mut self) -> Option<&[Value]> {
        let schema = self.input.schema();
        if let Some(in_row) = self.input.next() {
            self.row_buf.clear();
            for (expr, _alias) in &self.exprs {
                self.row_buf.push(expr.eval(in_row, &schema));
            }
            Some(&self.row_buf)
        } else {
            None
        }
    }

    fn schema(&self) -> Vec<String> {
        self.exprs
            .iter()
            .map(|(_expr, alias)| alias.to_string())
            .collect()
    }
}

struct Sort {
    /// The expression to sort by (in ascending order)
    sort_by: Box<Expr>,
    input: Box<Node>,
    buffer: Vec<Vec<Value>>,
    /// Index into the sorted buffer
    idx: usize,
}

impl Sort {
    fn new(sort_by: Box<Expr>, input: Box<Node>) -> Sort {
        Sort {
            sort_by,
            input,
            buffer: vec![],
            idx: 0,
        }
    }

    fn next(&mut self) -> Option<&[Value]> {
        // Materialize the sort into `self.buffer` (only happens once)
        if self.buffer.len() == 0 {
            while let Some(row) = self.input.next() {
                self.buffer.push(row.to_vec());
            }
            let schema = &self.schema();
            self.buffer
                .sort_by_key(|row| self.sort_by.eval(row, &schema));
        }

        // Return rows from buffer until empty
        if self.idx >= self.buffer.len() {
            None
        } else {
            let to_return = &self.buffer[self.idx];
            self.idx += 1;
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
    fn next(&mut self) -> Option<&[Value]> {
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
            (Expr::Literal(Value::Null), "null".to_string()),
            (Expr::Literal(Value::Bool(true)), "bool_literal".to_string()),
            (Expr::Literal(Value::Int(1)), "int_literal".to_string()),
            (
                Expr::Literal(Value::Str("hello world".to_string())),
                "hello_world".to_string(),
            ),
            (Expr::Column("a".to_string()), "a".to_string()),
            (Expr::Column("b".to_string()), "b".to_string()),
            (Expr::Column("c".to_string()), "c".to_string()),
            (Expr::Column("b".to_string()), "b".to_string()),
            (Expr::Column("a".to_string()), "a".to_string()),
        ],
        Box::new(select_node),
    ));
    let mut limit_node = Node::Limit(Limit::new(2, 1, Box::new(project_node)));

    // Print column names
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
