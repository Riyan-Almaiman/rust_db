use std::collections::HashMap;
#[derive(Debug, Clone)]

pub enum ColumnType {
    Int,
    Text,
    Bool,
}

#[derive(Debug, Clone)]
pub enum Value {
    Int(i64),
    Text(String),
    Bool(bool),
}
#[derive(Debug, Clone)]
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
#[derive(Debug, Default)]
pub struct Database {
    pub tables: HashMap<String, Table>,
}
#[derive(Debug)]
pub enum DbCommand {
    CreateTable {
        table: String,
        columns: Vec<(String, ColumnType)>,
    },
    InsertRow {
        table: String,
        values: Vec<Value>,
    },
    UpdateRow {
        table: String,
        row_id: u64,
        updates: Vec<(String, Value)>,
    },
    SelectAll {
        table: String,
    },
}pub enum DbResult {
    Ok,
    Rows {
        columns: Vec<String>,
        rows: Vec<(u64, Vec<Value>)>,
    },
}


fn value_matches_type(value: &Value, col_type: &ColumnType) -> bool {
    matches!(
        (value, col_type),
        (Value::Int(_), ColumnType::Int)
            | (Value::Text(_), ColumnType::Text)
            | (Value::Bool(_), ColumnType::Bool)
    )
}
impl Database {
pub fn execute(&mut self, cmd: DbCommand) -> Result<DbResult, String>{
        match cmd {
            DbCommand::SelectAll { table } => {
    let table = self.tables.get(&table).ok_or("Table not found")?;

    let columns = table.columns.iter().map(|c| c.name.clone()).collect();

    let mut rows: Vec<(u64, Vec<Value>)> = table
        .rows
        .iter()
        .map(|(id, values)| (*id, values.clone()))
        .collect();

    rows.sort_by_key(|(id, _)| *id);

    Ok(DbResult::Rows { columns, rows })
}

            DbCommand::CreateTable { table, columns } => {
                if self.tables.contains_key(&table) {
                    return Err("Table already exists".into());
                }

                let columns = columns
                    .into_iter()
                    .map(|(name, col_type)| Column { name, col_type })
                    .collect();

                let table_obj = Table {
                    name: table.clone(),
                    columns,
                    rows: HashMap::new(),
                    next_row_id: 1,
                };

                self.tables.insert(table, table_obj);
Ok(DbResult::Ok)
            }

            DbCommand::InsertRow { table, values } => {
                let table = self.tables.get_mut(&table).ok_or("Table not found")?;

                if values.len() != table.columns.len() {
                    return Err("Column count mismatch".into());
                }

                for (value, column) in values.iter().zip(&table.columns) {
                    if !value_matches_type(value, &column.col_type) {
                        return Err(format!("Type mismatch for column {}", column.name));
                    }
                }

                let row_id = table.next_row_id;
                table.next_row_id += 1;
                table.rows.insert(row_id, values);

Ok(DbResult::Ok)
            }

            DbCommand::UpdateRow {
                table,
                row_id,
                updates,
            } => {
                let table = self.tables.get_mut(&table).ok_or("Table not found")?;
                let row = table.rows.get_mut(&row_id).ok_or("Row not found")?;

                for (col_name, new_value) in updates {
                    let index = table
                        .columns
                        .iter()
                        .position(|c| c.name == col_name)
                        .ok_or("Column not found")?;

                    if !value_matches_type(&new_value, &table.columns[index].col_type) {
                        return Err(format!("Type mismatch for column {}", col_name));
                    }

                    row[index] = new_value;
                }

Ok(DbResult::Ok)
            }
        }
    }
}
