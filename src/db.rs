use std::collections::HashMap;

use crate::commands::{DbCommand, DbResult};
use crate::db_types::Table;

#[derive(Debug, Default)]
pub struct Database {
    pub tables: HashMap<String, Table>,
}

impl Database {
    pub fn execute(&mut self, cmd: DbCommand) -> Result<DbResult, String> {
        match cmd {
            DbCommand::CreateTable { table, columns } =>
                self.create_table(table, columns),

            DbCommand::InsertRow { table, values } =>
                self.insert_row(table, values),

            DbCommand::UpdateRow { table, row_id, updates } =>
                self.update_row(table, row_id, updates),

            DbCommand::SelectAll { table } =>
                self.select_all(table),
                
            DbCommand::GetTables {} =>
                self.get_tables(),
        }
    }
}
