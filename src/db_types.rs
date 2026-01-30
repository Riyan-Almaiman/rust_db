use std::collections::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ColumnType {
    Int,
    Text,
    Bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    Bool(bool), 
    Int(i64),
    Text(String),
}
#[derive(Debug, Clone, Serialize)]
pub struct Column {
    pub name: String,
    pub col_type: ColumnType,
}

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: HashMap<u64, Vec<Value>>,
    pub next_row_id: u64,
}

