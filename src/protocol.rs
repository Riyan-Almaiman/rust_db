use crate::db::{ColumnType, DbCommand, Value};

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
        0x01 => {
            let table = c.string()?;
            let count = c.u8()? as usize;
            let mut columns = Vec::with_capacity(count);

            for _ in 0..count {
                let name = c.string()?;
                let ty = match c.u8()? {
                    0x01 => ColumnType::Int,
                    0x02 => ColumnType::Text,
                    0x03 => ColumnType::Bool,
                    _ => anyhow::bail!("Unknown column type"),
                };
                columns.push((name, ty));
            }

            Ok(DbCommand::CreateTable { table, columns })
        }
0x04 => {
    let table = c.string()?;
    Ok(DbCommand::SelectAll { table })
}

        0x02 => {
            let table = c.string()?;
            let count = c.u8()? as usize;
            let mut values = Vec::with_capacity(count);

            for _ in 0..count {
                values.push(parse_value(&mut c)?);
            }

            Ok(DbCommand::InsertRow { table, values })
        }

        0x03 => {
            let table = c.string()?;
            let row_id = c.u64()?;
            let count = c.u8()? as usize;
            let mut updates = Vec::with_capacity(count);

            for _ in 0..count {
                let name = c.string()?;
                let val = parse_value(&mut c)?;
                updates.push((name, val));
            }

            Ok(DbCommand::UpdateRow {
                table,
                row_id,
                updates,
            })
        }

        _ => anyhow::bail!("Unknown opcode"),
    }
}
fn parse_value(c: &mut Cursor) -> anyhow::Result<Value> {
    match c.u8()? {
        0x01 => Ok(Value::Int(c.u64()? as i64)),
        0x02 => Ok(Value::Text(c.string()?)),
        0x03 => Ok(Value::Bool(c.u8()? != 0)),
        _ => anyhow::bail!("Unknown value type"),
    }
}
pub fn encode_ok() -> Vec<u8> {
    vec![0x00]
}

pub fn encode_error(msg: &str) -> Vec<u8> {
    let mut buf = vec![0x01];
    let bytes = msg.as_bytes();
    buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(bytes);
    buf
}
pub fn encode_rows(
    columns: &[String],
    rows: &[(u64, Vec<Value>)],
) -> Vec<u8> {
    let mut buf = vec![0x00];

    buf.push(columns.len() as u8);
    for c in columns {
        buf = write_string(buf, c);
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
fn write_string(mut buf: Vec<u8>, s: &str) -> Vec<u8> {
    let bytes = s.as_bytes();
    buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(bytes);
    buf
}
fn encode_value(buf: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Int(i) => {
            buf.push(0x01);
            buf.extend_from_slice(&i.to_be_bytes());
        }
        Value::Text(s) => {
            buf.push(0x02);
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
            buf.extend_from_slice(bytes);
        }
        Value::Bool(b) => {
            buf.push(0x03);
            buf.push(if *b { 1 } else { 0 });
        }
    }
}
