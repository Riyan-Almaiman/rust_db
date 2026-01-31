use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use crate::db_types::{Column, ColumnType, Value};
use crate::commands::{DbCommand, DbResult};
// Command opcodes
const OP_CREATE_TABLE: u8 = 0x01;
const OP_INSERT_ROW: u8 = 0x02;
const OP_UPDATE_ROW: u8 = 0x03;
const OP_SELECT_ALL: u8 = 0x04;
const OP_GET_TABLES: u8 = 0x05;
// Value/Column type opcodes
const TYPE_INT: u8 = 0x01;
const TYPE_TEXT: u8 = 0x02;
const TYPE_BOOL: u8 = 0x03;

// Response opcodes
const RESP_OK: u8 = 0x00;
const RESP_ERR: u8 = 0x01;


pub struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> anyhow::Result<&'a [u8]> {
        if self.pos + n > self.buf.len() {
            anyhow::bail!("Unexpected end of buffer");
        }
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn u8(&mut self) -> anyhow::Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> anyhow::Result<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into()?))
    }

    fn u64(&mut self) -> anyhow::Result<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into()?))
    }

    fn string(&mut self) -> anyhow::Result<String> {
        let len = self.u16()? as usize;
        let bytes = self.take(len)?;
        Ok(String::from_utf8(bytes.to_vec())?)
    }
}
pub fn parse_command(buf: &[u8]) -> anyhow::Result<DbCommand> {
    let mut c = Cursor::new(buf);
    let opcode = c.u8()?;

    match opcode {
        OP_CREATE_TABLE => {
            let table = c.string()?;
            let count = c.u8()? as usize;
            let mut columns = Vec::with_capacity(count);

            for _ in 0..count {
                let name = c.string()?;
                let ty = match c.u8()? {
                    TYPE_INT => ColumnType::Int,
                    TYPE_TEXT => ColumnType::Text,
                    TYPE_BOOL => ColumnType::Bool,
                    _ => anyhow::bail!("Unknown column type"),
                };
                columns.push((name, ty));
            }

            Ok(DbCommand::CreateTable { table, columns })
        }
        OP_INSERT_ROW => {
            let table = c.string()?;
            let count = c.u8()? as usize;
            let mut values = Vec::with_capacity(count);

            for _ in 0..count {
                values.push(parse_value(&mut c)?);
            }

            Ok(DbCommand::InsertRow { table, values })
        }
        OP_UPDATE_ROW => {
            let table = c.string()?;
            let row_id = c.u64()?;
            let count = c.u8()? as usize;
            let mut updates = HashMap::with_capacity(count);

            for _ in 0..count {
                let name = c.string()?;
                let val = parse_value(&mut c)?;
                updates.insert(name, val);
            }

            Ok(DbCommand::UpdateRow {
                table,
                row_id,
                updates,
            })
        }
        OP_SELECT_ALL => {
            let table = c.string()?;
            Ok(DbCommand::SelectAll { table })
        }
        OP_GET_TABLES => {
            Ok(DbCommand::GetTables {})
        }
        _ => anyhow::bail!("Unknown command opcode"),
    }
}

pub fn encode_command(cmd: &DbCommand) -> Vec<u8> {
    let mut buf = Vec::new();

    match cmd {
        DbCommand::GetTables {} => {
            buf.push(OP_GET_TABLES);
        }
        DbCommand::CreateTable { table, columns } => {
            buf.push(OP_CREATE_TABLE);
            write_string(&mut buf, table);
            buf.push(columns.len() as u8);
            for (name, col_type) in columns {
                write_string(&mut buf, name);
                buf.push(match col_type {
                    ColumnType::Int => TYPE_INT,
                    ColumnType::Text => TYPE_TEXT,
                    ColumnType::Bool => TYPE_BOOL,
                });
            }
        }
        DbCommand::InsertRow { table, values } => {
            buf.push(OP_INSERT_ROW);
            write_string(&mut buf, table);
            buf.push(values.len() as u8);
            for v in values {
                encode_value(&mut buf, v);
            }
        }
        DbCommand::UpdateRow { table, row_id, updates } => {
            buf.push(OP_UPDATE_ROW);
            write_string(&mut buf, table);
            buf.extend_from_slice(&row_id.to_be_bytes());
            buf.push(updates.len() as u8);
            for (col, val) in updates {
                write_string(&mut buf, col);
                encode_value(&mut buf, val);
            }
        }
        DbCommand::SelectAll { table } => {
            buf.push(OP_SELECT_ALL);
            write_string(&mut buf, table);
        }
    }

    buf
}

pub fn decode_response(data: &[u8]) -> Result<DbResult, String> {
    if data.is_empty() {
        return Err("Empty response".into());
    }

    match data[0] {
        RESP_OK => {
            if data.len() == 1 {
                return Ok(DbResult::Ok);
            }

            let mut pos = 1;
            let col_count = data[pos] as usize;
            pos += 1;

            let mut columns = Vec::with_capacity(col_count);
            for _ in 0..col_count {
                let len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
                pos += 2;
                columns.push(String::from_utf8_lossy(&data[pos..pos + len]).to_string());
                pos += len;
            }

            let row_count = u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
            pos += 4;

            let mut rows = Vec::with_capacity(row_count);
            for _ in 0..row_count {
                let row_id = u64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
                pos += 8;

                let mut values = Vec::with_capacity(col_count);
                for _ in 0..col_count {
                    let val_type = data[pos];
                    pos += 1;

                    let val = match val_type {
                        TYPE_INT => {
                            let i = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
                            pos += 8;
                            Value::Int(i)
                        }
                        TYPE_TEXT => {
                            let len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
                            pos += 2;
                            let s = String::from_utf8_lossy(&data[pos..pos + len]).to_string();
                            pos += len;
                            Value::Text(s)
                        }
                        TYPE_BOOL => {
                            let b = data[pos] != 0;
                            pos += 1;
                            Value::Bool(b)
                        }
                        _ => return Err("Unknown value type".into()),
                    };
                    values.push(val);
                }

                rows.push((row_id, values));
            }

            Ok(DbResult::Rows { columns, rows })
        }
        RESP_ERR => {
            let len = u16::from_be_bytes([data[1], data[2]]) as usize;
            let msg = String::from_utf8_lossy(&data[3..3 + len]).to_string();
            Err(msg)
        }
        _ => Err("Unknown response type".into()),
    }
}

fn parse_value(c: &mut Cursor) -> anyhow::Result<Value> {
    match c.u8()? {
        TYPE_INT => Ok(Value::Int(c.u64()? as i64)),
        TYPE_TEXT => Ok(Value::Text(c.string()?)),
        TYPE_BOOL => Ok(Value::Bool(c.u8()? != 0)),
        _ => anyhow::bail!("Unknown value type"),
    }
}

pub fn encode_result(result: &DbResult) -> Vec<u8> {
    match result {
        DbResult::Ok => vec![RESP_OK],
        DbResult::Rows { columns, rows } => encode_rows(columns, rows),
    }
}

pub fn encode_error(msg: &str) -> Vec<u8> {
    let mut buf = vec![RESP_ERR];
    let bytes = msg.as_bytes();
    buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(bytes);
    buf
}

fn encode_rows(
    columns: &[String],
    rows: &[(u64, Vec<Value>)],
) -> Vec<u8> {
    let mut buf = vec![RESP_OK];

    buf.push(columns.len() as u8);
    for c in columns {
        write_string(&mut buf, c);
    }

    buf.extend_from_slice(&(rows.len() as u32).to_be_bytes());

    for (row_id, values) in rows {
        buf.extend_from_slice(&row_id.to_be_bytes());
        for v in values {
            encode_value(&mut buf, v);
        }
    }

    buf
}


fn write_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(bytes);
}
fn encode_value(buf: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Int(i) => {
            buf.push(TYPE_INT);
            buf.extend_from_slice(&i.to_be_bytes());
        }
        Value::Text(s) => {
            buf.push(TYPE_TEXT);
            write_string(buf, s);
        }
        Value::Bool(b) => {
            buf.push(TYPE_BOOL);
            buf.push(if *b { 1 } else { 0 });
        }
    }
}


pub async fn read_frame(stream: &mut TcpStream) -> std::io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];

    if stream.read_exact(&mut len_buf).await.is_err() {
        return Ok(None);
    }

    let len = u32::from_be_bytes(len_buf) as usize;

    if len > 1024 * 1024 {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Frame too large"));
    }

    let mut data = vec![0u8; len];
    stream.read_exact(&mut data).await?;

    Ok(Some(data))
}

pub async fn write_frame(stream: &mut TcpStream, data: &[u8]) -> std::io::Result<()> {
    let len = (data.len() as u32).to_be_bytes();
    stream.write_all(&len).await?;
    stream.write_all(data).await?;
    Ok(())
}
