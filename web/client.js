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
}

const client = new DbClient();

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
}

function parseValue(str) {
    str = str.trim();
    if (str === 'true') return true;
    if (str === 'false') return false;
    if (/^-?\d+$/.test(str)) return parseInt(str);
    return str;
}

async function insertRow() {
    const tableName = document.getElementById('insertTableName').value.trim();
    const valuesStr = document.getElementById('insertValues').value.trim();

    if (!tableName) {
        showResult('insertResult', { ok: false, error: 'Table name required' });
        return;
    }

    const values = valuesStr.split(',').map(parseValue);
    const result = await client.insert(tableName, values);
    showResult('insertResult', result);
}

async function updateRow() {
    const tableName = document.getElementById('updateTableName').value.trim();
    const rowId = parseInt(document.getElementById('updateRowId').value);
    const updatesStr = document.getElementById('updateValues').value.trim();

    if (!tableName || isNaN(rowId)) {
        showResult('updateResult', { ok: false, error: 'Table name and row ID required' });
        return;
    }

    const updates = {};
    for (const pair of updatesStr.split(',')) {
        const [col, val] = pair.split('=').map(s => s.trim());
        if (col && val !== undefined) {
            updates[col] = parseValue(val);
        }
    }

    const result = await client.update(tableName, rowId, updates);
    showResult('updateResult', result);
}

async function selectAll() {
    const tableName = document.getElementById('selectTableName').value.trim();

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
