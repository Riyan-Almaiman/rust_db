
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

use crate::db::Database;
use crate::db_types::{Column, ColumnType, Table, Value};

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum DbCommand {
    CreateTable {
        table: String,
        columns: Vec<(String, ColumnType)>,
    },
    #[serde(rename = "insert")]
    InsertRow {
        table: String,
        values: Vec<Value>,
    },
    #[serde(rename = "update")]
    UpdateRow {
        table: String,
        #[serde(rename = "rowId")]
        row_id: u64,
        updates: HashMap<String, Value>,
    },
    SelectAll {
        table: String,
    },
       GetTables {
      
    },
}

#[derive(Debug, Serialize)]
pub enum DbResult {
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
  pub fn get_tables(&self) -> Result<DbResult, String> {
      let mut rows = Vec::new();
      let mut id = 1;

      for (table_name, table) in &self.tables {
          for col in &table.columns {
              rows.push((id, vec![
                  Value::Text(table_name.clone()),
                  Value::Text(col.name.clone()),
                  Value::Text(format!("{:?}", col.col_type).to_lowercase()),
              ]));
              id += 1;
          }
      }

      Ok(DbResult::Rows {
          columns: vec!["table_name".into(), "column_name".into(), "column_type".into()],
          rows,
      })
  }
    pub fn create_table(
        &mut self,
        table: String,
        columns: Vec<(String, ColumnType)>,
    ) -> Result<DbResult, String> {
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

    pub fn insert_row(
        &mut self,
        table: String,
        values: Vec<Value>,
    ) -> Result<DbResult, String> {
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

    pub fn update_row(
        &mut self,
        table: String,
        row_id: u64,
        updates: HashMap<String, Value>,
    ) -> Result<DbResult, String> {
        let table = self.tables.get_mut(&table).ok_or("Table not found")?;
        let row = table.rows.get_mut(&row_id).ok_or("Row not found")?;

        for (col_name, new_value) in updates.into_iter() {
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

    pub fn select_all(
        &self,
        table: String,
    ) -> Result<DbResult, String> {
        let table = self.tables.get(&table).ok_or("Table not found")?;

        let columns = table.columns.iter().map(|c| c.name.clone()).collect();

        let mut rows: Vec<_> = table
            .rows
            .iter()
            .map(|(id, values)| (*id, values.clone()))
            .collect();

        rows.sort_by_key(|(id, _)| *id);

        Ok(DbResult::Rows { columns, rows })
    }
}