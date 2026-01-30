class DbClient {
    constructor() {
        this.ws = null;
        this.pendingResolve = null;
        this.connect();
    }

    connect() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        this.ws = new WebSocket(`${protocol}//${window.location.host}/ws`);

        this.ws.onopen = () => {
            document.getElementById('connectionStatus').className = 'status connected';
            document.getElementById('connectionStatus').textContent = 'Connected';
            refreshTables();
        };

        this.ws.onclose = () => {
            document.getElementById('connectionStatus').className = 'status disconnected';
            document.getElementById('connectionStatus').textContent = 'Disconnected - Reconnecting...';
            setTimeout(() => this.connect(), 2000);
        };

        this.ws.onmessage = (event) => {
            if (this.pendingResolve) {
                const resolve = this.pendingResolve;
                this.pendingResolve = null;
                resolve(JSON.parse(event.data));
            }
        };
    }

    send(cmd) {
        return new Promise((resolve) => {
            this.pendingResolve = resolve;
            this.ws.send(JSON.stringify(cmd));
        });
    }
    
    createTable(table, columns) {
        return this.send({ type: 'createTable', table, columns });
    }

    insert(table, values) {
        return this.send({ type: 'insert', table, values });
    }

    update(table, rowId, updates) {
        return this.send({ type: 'update', table, rowId, updates });
    }

    selectAll(table) {
        return this.send({ type: 'selectAll', table });
    }

    getTables() {
        return this.send({ type: 'getTables' });
    }
}

const client = new DbClient();

// Store table schemas: { tableName: [{name, type}, ...] }
let tableSchemas = {};

async function refreshTables() {
    const result = await client.getTables();
    if (!result.ok) return;

    // Parse rows into schema map
    tableSchemas = {};
    for (const row of result.rows) {
        const tableName = row.table_name;
        if (!tableSchemas[tableName]) {
            tableSchemas[tableName] = [];
        }
        tableSchemas[tableName].push({
            name: row.column_name,
            type: row.column_type
        });
    }

    // Update all table selects
    const tableNames = Object.keys(tableSchemas);
    const selects = document.querySelectorAll('.table-select');

    for (const select of selects) {
        const current = select.value;
        select.innerHTML = '<option value="">Select table...</option>';
        for (const table of tableNames) {
            const opt = document.createElement('option');
            opt.value = table;
            opt.textContent = table;
            select.appendChild(opt);
        }
        if (tableNames.includes(current)) {
            select.value = current;
        }
    }
}

function getTableSchema(tableName) {
    return tableSchemas[tableName] || [];
}

function showInsertColumns() {
    const tableName = document.getElementById('insertTableName').value;
    const container = document.getElementById('insertColumns');
    const schema = getTableSchema(tableName);

    if (!schema.length) {
        container.innerHTML = '';
        return;
    }

    let html = '';
    for (const col of schema) {
        const inputType = col.type === 'bool' ? 'checkbox' : (col.type === 'int' ? 'number' : 'text');
        html += `<div class="column-row">
            <label style="flex:1">${escapeHtml(col.name)} (${col.type})</label>
            <input type="${inputType}" class="insert-value" data-type="${col.type}" style="flex:2">
        </div>`;
    }
    container.innerHTML = html;
}

function showUpdateColumns() {
    const tableName = document.getElementById('updateTableName').value;
    const container = document.getElementById('updateColumns');
    const schema = getTableSchema(tableName);

    if (!schema.length) {
        container.innerHTML = '';
        return;
    }

    let html = '';
    for (const col of schema) {
        const inputType = col.type === 'bool' ? 'checkbox' : (col.type === 'int' ? 'number' : 'text');
        html += `<div class="column-row">
            <input type="checkbox" class="update-check" style="flex:0;width:auto">
            <label style="flex:1">${escapeHtml(col.name)} (${col.type})</label>
            <input type="${inputType}" class="update-value" data-col="${col.name}" data-type="${col.type}" style="flex:2">
        </div>`;
    }
    container.innerHTML = html;
}

function addColumn() {
    const container = document.getElementById('columnsContainer');
    const row = document.createElement('div');
    row.className = 'column-row';
    row.innerHTML = `
        <input type="text" placeholder="Column name" class="col-name">
        <select class="col-type">
            <option value="int">Int</option>
            <option value="text">Text</option>
            <option value="bool">Bool</option>
        </select>
        <button class="danger" onclick="removeColumn(this)">X</button>
    `;
    container.appendChild(row);
}

function removeColumn(btn) {
    const container = document.getElementById('columnsContainer');
    if (container.children.length > 1) {
        btn.parentElement.remove();
    }
}

function showResult(elementId, result) {
    const el = document.getElementById(elementId);
    if (result.ok) {
        el.className = 'success';
        el.textContent = 'Success!';
    } else {
        el.className = 'error';
        el.textContent = 'Error: ' + result.error;
    }
}

async function createTable() {
    const tableName = document.getElementById('createTableName').value.trim();
    if (!tableName) {
        showResult('createResult', { ok: false, error: 'Table name required' });
        return;
    }

    const columns = [];
    const rows = document.querySelectorAll('#columnsContainer .column-row');
    for (const row of rows) {
        const name = row.querySelector('.col-name').value.trim();
        const type = row.querySelector('.col-type').value;
        if (name) {
            columns.push([name, type]);
        }
    }

    if (columns.length === 0) {
        showResult('createResult', { ok: false, error: 'At least one column required' });
        return;
    }

    const result = await client.createTable(tableName, columns);
    showResult('createResult', result);
    if (result.ok) {
        refreshTables();
    }
}

function parseValue(str) {
    str = str.trim();
    if (str === 'true') return true;
    if (str === 'false') return false;
    if (/^-?\d+$/.test(str)) return parseInt(str);
    return str;
}

async function insertRow() {
    const tableName = document.getElementById('insertTableName').value;

    if (!tableName) {
        showResult('insertResult', { ok: false, error: 'Table required' });
        return;
    }

    const inputs = document.querySelectorAll('#insertColumns .insert-value');
    const values = [];
    for (const input of inputs) {
        const type = input.dataset.type;
        if (type === 'bool') {
            values.push(input.checked);
        } else if (type === 'int') {
            values.push(parseInt(input.value) || 0);
        } else {
            values.push(input.value);
        }
    }

    const result = await client.insert(tableName, values);
    showResult('insertResult', result);
}

async function updateRow() {
    const tableName = document.getElementById('updateTableName').value;
    const rowId = parseInt(document.getElementById('updateRowId').value);

    if (!tableName || isNaN(rowId)) {
        showResult('updateResult', { ok: false, error: 'Table and row ID required' });
        return;
    }

    const rows = document.querySelectorAll('#updateColumns .column-row');
    const updates = {};
    for (const row of rows) {
        const check = row.querySelector('.update-check');
        if (!check.checked) continue;

        const input = row.querySelector('.update-value');
        const col = input.dataset.col;
        const type = input.dataset.type;

        if (type === 'bool') {
            updates[col] = input.checked;
        } else if (type === 'int') {
            updates[col] = parseInt(input.value) || 0;
        } else {
            updates[col] = input.value;
        }
    }

    if (Object.keys(updates).length === 0) {
        showResult('updateResult', { ok: false, error: 'Select at least one column to update' });
        return;
    }

    const result = await client.update(tableName, rowId, updates);
    showResult('updateResult', result);
}

async function selectAll() {
    const tableName = document.getElementById('selectTableName').value;

    if (!tableName) {
        showResult('selectResult', { ok: false, error: 'Table name required' });
        return;
    }

    const result = await client.selectAll(tableName);

    if (!result.ok) {
        showResult('selectResult', result);
        document.getElementById('results').innerHTML = '';
        return;
    }

    showResult('selectResult', { ok: true });

    if (!result.rows || result.rows.length === 0) {
        document.getElementById('results').innerHTML = '<p>No rows found</p>';
        return;
    }

    let html = '<table><tr><th>ID</th>';
    for (const col of result.columns) {
        html += `<th>${escapeHtml(col)}</th>`;
    }
    html += '</tr>';

    for (const row of result.rows) {
        html += `<tr><td>${row._id}</td>`;
        for (const col of result.columns) {
            html += `<td>${escapeHtml(String(row[col]))}</td>`;
        }
        html += '</tr>';
    }
    html += '</table>';

    document.getElementById('results').innerHTML = html;
}

function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}
