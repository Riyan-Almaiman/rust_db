use std::collections::HashMap;
use tokio::sync::mpsc::Receiver;

use crate::{Command, protocol};
use crate::commands::{DbCommand, DbResult};
use crate::db_types::Table;

#[derive(Debug, Default)]
pub struct Database {
    pub tables: HashMap<String, Table>,
}

impl Database {
    
    pub async  fn run(&mut self, mut rec: Receiver<Command>) -> () {
           while let Some(cmd) = rec.recv().await {
            let response = match protocol::parse_command(&cmd.data) {
                Ok(db_cmd) => match self.execute(db_cmd) {
                    Ok(result) => protocol::encode_result(&result),
                    Err(e) => protocol::encode_error(&e),
                },
                Err(e) => protocol::encode_error(&format!("Protocol error: {}", e)),
            };
            let _ = cmd.respond_to.send(response);
        }
    }
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
