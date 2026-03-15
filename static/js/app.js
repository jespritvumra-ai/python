// --- State ---
const state = {
    uploadId: null,
    instances: new Map()
};

// --- Socket.IO ---
const socket = io();

socket.on('connect', () => {
    showToast('Connected to Nexus Server', 'success');
});

socket.on('disconnect', () => {
    // Hidden since reload triggers it
    console.log('Socket disconnected');
});

socket.on('log_update', (data) => {
    appendLog(data.id, data.log);
});

socket.on('status_update', (data) => {
    // We rely on polling and refresh now, but we can optionally update badges here
    updateInstanceStatus(data.id, data.status);
});

socket.on('instance_deleted', (data) => {
    // Only remove card if it hasn't reloaded yet
    removeInstanceCard(data.id);
});

async function fetchMetrics() {
    try {
        const res = await fetch('/api/metrics');
        const data = await res.json();

        if (data.success) {
            // Global Metrics
            const cpuEl = document.getElementById('global-cpu');
            if (cpuEl) cpuEl.textContent = `${data.global.cpu}%`;

            const ramEl = document.getElementById('global-ram');
            if (ramEl) ramEl.textContent = `${data.global.ram_percent}%`;

            const diskEl = document.getElementById('global-disk');
            if (diskEl) diskEl.textContent = `${data.global.disk_percent}%`;

            // Sub-metrics
            const speedEl = document.getElementById('cpu-speed');
            if (speedEl) speedEl.textContent = `${data.global.cpu_speed} GHz`;

            const coresEl = document.getElementById('cpu-cores');
            if (coresEl) coresEl.textContent = `${data.global.cpu_cores}`;

            const totalRamEl = document.getElementById('ram-total');
            if (totalRamEl) totalRamEl.textContent = `${data.global.ram_total} MB`;

            // Instance Uptimes and Metrics
            for (const [id, uptime] of Object.entries(data.uptimes)) {
                const uptimeSpan = document.getElementById(`uptime-${id}`);
                if (uptimeSpan) {
                    const innerSpan = uptimeSpan.querySelector('span');
                    if (innerSpan) innerSpan.textContent = uptime;
                }
            }

            if (data.process_metrics) {
                for (const [id, metrics] of Object.entries(data.process_metrics)) {
                    const procCpu = document.getElementById(`proc-cpu-${id}`);
                    const procRam = document.getElementById(`proc-ram-${id}`);
                    if (procCpu) procCpu.innerHTML = `<i data-lucide="cpu"></i> ${metrics.cpu}%`;
                    if (procRam) procRam.innerHTML = `<i data-lucide="memory-stick"></i> ${metrics.ram} MB`;
                }
                lucide.createIcons(); // refresh dynamically added icons inside proc metrics if any
            }
        }
    } catch (e) {
        console.error("Failed to fetch metrics", e);
    }
}

// --- DOM Elements (Initialized on Load) ---
let grid, emptyState, btnUploadModal, btnNewInstance;
let uploadModal, spawnModal, dropZone, fileInput, btnConfirmSpawn;
let credModal, credUidInput, credPassInput, credInstIdInput, btnSaveCreds, credInstNameSpan;

document.addEventListener('DOMContentLoaded', () => {
    // Initialize Lucide Icons
    try { lucide.createIcons(); } catch (e) { }

    grid = document.getElementById('instance-grid');
    emptyState = document.getElementById('empty-state');
    btnUploadModal = document.getElementById('btn-upload-modal');
    btnNewInstance = document.getElementById('btn-new-instance');

    uploadModal = document.getElementById('upload-modal');
    spawnModal = document.getElementById('spawn-modal');
    credModal = document.getElementById('credentials-modal');

    dropZone = document.getElementById('drop-zone');
    fileInput = document.getElementById('file-input');
    btnConfirmSpawn = document.getElementById('btn-confirm-spawn');

    credUidInput = document.getElementById('cred-uid');
    credPassInput = document.getElementById('cred-pass');
    credInstIdInput = document.getElementById('cred-inst-id');
    btnSaveCreds = document.getElementById('btn-save-credentials');
    credInstNameSpan = document.getElementById('cred-inst-name');

    // Bind Event Listeners
    bindEvents();

    // Start App
    init();
});

// --- Initialization ---
async function init() {
    await fetchInstances();
    fetchMetrics(); // Initial fetch
    setInterval(fetchMetrics, 2000); // Poll metrics every 2 seconds
}

// --- Fetch & Render API ---
async function fetchInstances() {
    try {
        const urlParams = new URLSearchParams(window.location.search);
        const filter = urlParams.get('filter') || 'all';
        const res = await secureFetch(`/api/instances?filter=${filter}`);
        if (!res) return;
        const data = await res.json();

        if (data.success) {
            if (data.instances.length > 0) {
                emptyState.style.display = 'none';
                data.instances.forEach(inst => renderInstanceCard(inst));
            } else {
                emptyState.style.display = 'flex';
                grid.innerHTML = '';
            }
        }
    } catch (e) {
        console.error('Failed to fetch instances', e);
    }
}

async function spawnInstance(name, count = 1) {
    try {
        const res = await secureFetch('/api/instances/create', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ guild_id: name, count: count })
        });
        if (!res) return;
        const data = await res.json();

        if (data.success) {
            showToast('Console created. Refreshing...', 'success');
            setTimeout(() => location.reload(), 800);
        } else {
            showToast(data.error || 'Failed to spawn instance', 'error');
        }
    } catch (e) {
        console.error('Connection error during spawn', e);
    }
}

async function uploadInstancePackage(id, fileInput) {
    if (!fileInput.files.length) return;
    const file = fileInput.files[0];

    if (!file.name.endsWith('.zip')) {
        showToast('Only .zip files are allowed', 'error');
        fileInput.value = '';
        return;
    }

    const formData = new FormData();
    formData.append('file', file);

    // Show uploading state
    const term = document.getElementById(`term-${id}`);
    if (term) term.innerHTML = `<div class="log-line"><span class="timestamp">[SYSTEM]</span> <span class="text-primary">Uploading and Extracting package... Please wait.</span></div>`;

    try {
        const res = await secureFetch(`/api/instances/${id}/upload`, {
            method: 'POST',
            body: formData
        });
        if (!res) return;
        const data = await res.json();

        if (data.success) {
            showToast('Package extracted! Refreshing...', 'success');
            setTimeout(() => location.reload(), 800);
        } else {
            showToast(data.error, 'error');
            if (term) term.innerHTML += `<div class="log-line"><span style="color:var(--danger)">Error: ${data.error}</span></div>`;
        }
    } catch (e) {
        showToast('Upload failed', 'error');
    } finally {
        fileInput.value = '';
    }
}

async function startInstance(id) {
    try {
        const res = await secureFetch(`/api/instances/${id}/start`, { method: 'POST' });
        if (!res) return;
        const data = await res.json();
        if (data.success) {
            showToast('Starting instance... Refreshing...', 'success');
            setTimeout(() => location.reload(), 800);
        } else {
            showToast(data.error, 'error');
        }
    } catch (e) {
        showToast('Start signal failed', 'error')
    }
}

async function stopInstance(id) {
    try {
        const res = await secureFetch(`/api/instances/${id}/stop`, { method: 'POST' });
        if (!res) return;
        const data = await res.json();
        if (data.success) {
            showToast('Stopping instance... Refreshing...', 'success');
            setTimeout(() => location.reload(), 800);
        } else {
            showToast(data.error, 'error');
        }
    } catch (e) {
        showToast('Failed to send stop signal', 'error');
    }
}

async function deleteInstance(id) {
    if (!confirm('Are you sure you want to permanently delete this instance?')) return;

    try {
        const res = await secureFetch(`/api/instances/${id}`, { method: 'DELETE' });
        if (!res) return;
        const data = await res.json();
        if (data.success) {
            showToast('Instance deleted. Refreshing...', 'success');
            setTimeout(() => location.reload(), 800);
        } else {
            showToast(data.error, 'error');
        }
    } catch (e) {
        showToast('Delete operation failed', 'error');
    }
}

async function restartInstance(id) {
    try {
        const res = await secureFetch(`/api/instances/${id}/restart`, { method: 'POST' });
        if (!res) return;
        const data = await res.json();
        if (data.success) {
            showToast('Restarting instance... Refreshing...', 'success');
            setTimeout(() => location.reload(), 800);
        } else {
            showToast(data.error, 'error');
        }
    } catch (e) {
        showToast('Failed to restart instance', 'error');
    }
}

function cleanConsole(id) {
    const term = document.getElementById(`term-${id}`);
    if (term) term.innerHTML = '';
}

async function openCredentialsModal(id, name) {
    credInstNameSpan.textContent = name;
    credInstIdInput.value = id;
    credUidInput.value = '';
    credPassInput.value = '';

    credModal.classList.add('active');

    try {
        const res = await secureFetch(`/api/instances/${id}/credentials`);
        if (!res) return;
        const data = await res.json();
        if (data.success) {
            credUidInput.value = data.uid || '';
            credPassInput.value = data.password || '';
        } else {
            showToast('Could not fetch existing credentials.', 'error');
        }
    } catch (e) {
        showToast('Network error fetching credentials.', 'error');
    }
}

async function saveCredentials() {
    const id = credInstIdInput.value;
    const uid = credUidInput.value;
    const pass = credPassInput.value;

    if (!uid || !pass) {
        showToast('UID and Password are required!', 'error');
        return;
    }

    btnSaveCreds.disabled = true;
    btnSaveCreds.innerHTML = `<i data-lucide="loader" class="spin"></i> Saving...`;
    lucide.createIcons();

    try {
        const res = await secureFetch(`/api/instances/${id}/credentials`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ uid: uid, password: pass })
        });
        if (!res) return;
        const data = await res.json();

        if (data.success) {
            showToast('Credentials updated seamlessly!', 'success');
            credModal.classList.remove('active');
        } else {
            showToast(data.error || 'Failed to save credentials', 'error');
        }
    } catch (e) {
        showToast('Network error saving credentials.', 'error');
    } finally {
        btnSaveCreds.disabled = false;
        btnSaveCreds.innerHTML = 'Save Credentials';
        lucide.createIcons();
    }
}

// --- Rendering ---
function renderInstanceCard(inst) {
    const cardId = `card-${inst.id}`;
    let card = document.getElementById(cardId);
    let isNew = false;

    if (!card) {
        card = document.createElement('div');
        card.className = 'instance-card';
        card.id = cardId;
        isNew = true;
    }

    // Helper to generate context-aware buttons
    function renderButtons(id, status, name) {
        const nameEscaped = name.replace(/'/g, "\\'");
        if (status === 'running') {
            return `
            <button class="btn btn-outline" onclick="stopInstance('${id}')" title="Stop"><i data-lucide="square"></i> Stop</button>
            <button class="btn btn-warning" onclick="restartInstance('${id}')" title="Restart"><i data-lucide="refresh-cw"></i></button>
            ${window.userRole === 'admin' ? `<button class="btn btn-outline" onclick="openCredentialsModal('${id}', '${nameEscaped}')" title="Credentials" style="border-color:var(--primary);color:var(--primary)"><i data-lucide="key"></i></button>` : ''}
            <button class="btn btn-danger" onclick="deleteInstance('${id}')" title="Delete"><i data-lucide="trash-2"></i></button>
        `;
        } else if (status === 'empty') {
            if (window.userRole === 'admin') {
                return `
                <input type="file" id="file-${id}" accept=".zip" style="display:none;" onchange="uploadInstancePackage('${id}', this)">
                <button class="btn btn-primary" onclick="document.getElementById('file-${id}').click()" style="width:100%"><i data-lucide="upload"></i> Upload Zip</button>
                <button class="btn btn-danger" style="margin-left: 0.5rem" onclick="deleteInstance('${id}')" title="Delete"><i data-lucide="trash-2"></i></button>
            `;
            } else {
                return `
                <button class="btn btn-primary" disabled style="width:100%">Instance Empty</button>
                <button class="btn btn-danger" style="margin-left: 0.5rem" onclick="deleteInstance('${id}')" title="Delete"><i data-lucide="trash-2"></i></button>
            `;
            }
        } else if (status === 'installing') {
            return `
            <button class="btn btn-primary" disabled><i data-lucide="loader"></i> Installing...</button>
            <button class="btn btn-danger" style="margin-left: auto" onclick="deleteInstance('${id}')" title="Delete"><i data-lucide="trash-2"></i></button>
        `;
        } else {
            // Stopped
            return `
            <button class="btn btn-primary" onclick="startInstance('${id}')" title="Start"><i data-lucide="play"></i> Start</button>
            <button class="btn btn-warning" onclick="restartInstance('${id}')" title="Restart"><i data-lucide="refresh-cw"></i></button>
            ${window.userRole === 'admin' ? `<button class="btn btn-outline" onclick="openCredentialsModal('${id}', '${nameEscaped}')" title="Credentials" style="border-color:var(--primary);color:var(--primary)"><i data-lucide="key"></i></button>` : ''}
            <button class="btn btn-danger" style="margin-left: auto" onclick="deleteInstance('${id}')" title="Delete"><i data-lucide="trash-2"></i></button>
        `;
        }
    }

    // Only overwrite full HTML if it's new to preserve terminal scroll/contents, 
    // otherwise just update badge and footer.
    if (isNew) {
        const { id, name, status } = inst;
        card.innerHTML = `
        <div class="card-header">
            <div class="card-title">
                <i data-lucide="layout" class="icon-primary"></i>
                <div>
                    <span class="inst-name">${name}</span><br>
                    <span class="inst-id">${id}</span>
                    ${window.userRole === 'admin' ? `<div class="owner-badge"><i data-lucide="user"></i> ${inst.owner || 'unknown'}</div>` : ''}
                </div>
            </div>
            <span id="badge-${id}" class="badge ${status}">${status}</span>
        </div>
        <div class="card-body">
            <div class="terminal">
                <div class="terminal-bar">
                    <div class="t-dot red"></div>
                    <div class="t-dot yellow"></div>
                    <div class="t-dot green"></div>
                    <span class="t-title">bash - ${name}</span>
                </div>
                <div id="term-${id}" class="terminal-content">
                    <div class="line">Ready for action...</div>
                </div>
            </div>
        </div>
        
        <div id="metrics-${id}" class="instance-metrics-container" style="display: ${status === 'running' ? 'flex' : 'none'}">
            <div class="proc-metric" id="proc-cpu-${id}"><i data-lucide="cpu"></i> --</div>
            <div class="proc-metric" id="proc-ram-${id}"><i data-lucide="memory-stick"></i> --</div>
            <div class="uptime-badge" id="uptime-${id}">
                <i data-lucide="clock"></i> <span>00:00:00</span>
            </div>
        </div>
        
        <div class="card-footer" id="footer-${id}">
            <div id="controls-${id}" class="controls-grid" style="display: flex; gap: 0.5rem; justify-content: flex-end;">
                ${renderButtons(id, status, name)}
            </div>
        </div>
    `;
        grid.appendChild(card);
        state.instances.set(inst.id, true);

        if (inst.logs && inst.logs.length > 0) {
            inst.logs.forEach(l => appendLog(inst.id, l));
        }
    } else {
        // Just update dynamic parts
        updateInstanceStatus(inst.id, inst.status);
    }

    lucide.createIcons();
}

function removeInstanceCard(id) {
    const card = document.getElementById(`card-${id}`);
    if (card) {
        card.style.transform = 'scale(0.9)';
        card.style.opacity = '0';
        setTimeout(() => {
            card.remove();
            state.instances.delete(id);
            if (state.instances.size === 0) emptyState.style.display = 'flex';
        }, 300);
    }
}

function updateInstanceStatus(id, status) {
    const badge = document.getElementById(`badge-${id}`);
    if (badge) {
        badge.textContent = status;
        badge.className = `badge ${status}`;
    }

    const controls = document.getElementById(`controls-${id}`);
    const cardNameEl = document.querySelector(`#card-${id} .inst-name`);
    const name = cardNameEl ? cardNameEl.textContent : id;

    if (controls) {
        controls.innerHTML = renderButtons(id, status, name);
    }

    const metrics = document.getElementById(`metrics-${id}`);
    if (metrics) {
        metrics.style.display = (status === 'running') ? 'flex' : 'none';
    }

    lucide.createIcons();
}

function appendLog(id, log) {
    const term = document.getElementById(`term-${id}`);
    if (!term) return;

    const d = new Date();
    const ts = `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}:${d.getSeconds().toString().padStart(2, '0')}`;

    const line = document.createElement('div');
    line.className = 'log-line';

    // Basic syntax highlighting for common Python terms
    const formattedLog = log
        .replace(/Traceback/g, '<span style="color:#ff2a5f">Traceback</span>')
        .replace(/Error:/g, '<span style="color:#ff2a5f">Error:</span>')
        .replace(/Exception:/g, '<span style="color:#ff2a5f">Exception:</span>')
        .replace(/INFO/g, '<span style="color:#00f0ff">INFO</span>')
        .replace(/WARNING/g, '<span style="color:#ffb700">WARNING</span>');

    line.innerHTML = `<span class="timestamp">[${ts}]</span> ${formattedLog}`;
    term.appendChild(line);

    // Auto-scroll logic
    term.scrollTop = term.scrollHeight;
}


// --- UI Utilities: Toasts ---
function showToast(message, type = 'success') {
    const container = document.getElementById('toast-container');
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;

    const icon = type === 'success' ? 'check-circle' : 'alert-circle';
    toast.innerHTML = `<i data-lucide="${icon}"></i> <span>${message}</span>`;

    container.appendChild(toast);
    lucide.createIcons();

    // Trigger Reflow for animation
    void toast.offsetWidth;
    toast.classList.add('show');

    setTimeout(() => {
        toast.classList.remove('show');
        setTimeout(() => toast.remove(), 400);
    }, 4000);
}

// --- Modals & File Upload Events ---
function bindEvents() {
    function closeModals() {
        document.querySelectorAll('.modal-overlay').forEach(m => m.classList.remove('active'));
    }

    document.querySelectorAll('.btn-close').forEach(btn => {
        btn.onclick = closeModals;
    });

    if (btnNewInstance) btnNewInstance.onclick = () => spawnModal.classList.add('active');

    // Spawn Confirm btn
    if (btnConfirmSpawn) {
        btnConfirmSpawn.onclick = () => {
            let guildId = document.getElementById('guild-id').value;
            if (!guildId) {
                showToast('Guild ID is required!', 'error');
                return;
            }

            const countInput = document.getElementById('instance-count');
            const count = countInput ? parseInt(countInput.value) || 1 : 1;

            spawnInstance(guildId, count);
            document.getElementById('guild-id').value = '';
            if (countInput) countInput.value = '1';
        };
    }

    // Credentials Save btn
    if (btnSaveCreds) {
        btnSaveCreds.onclick = saveCredentials;
    }
}

// Helper for centralized error handling (Auth)
async function secureFetch(url, options = {}) {
    try {
        const res = await fetch(url, options);
        if (res.status === 401) {
            window.location.href = '/login';
            return null;
        }
        if (res.status === 403) {
            const data = await res.json();
            showToast(data.error || "Permission Denied", 'error');
            return null;
        }
        return res;
    } catch (e) {
        showToast("Network Error", "error");
        return null;
    }
}

async function uploadRestoreBackup(fileInput) {
    if (!fileInput.files.length) return;
    const file = fileInput.files[0];

    if (!file.name.endsWith('.zip')) {
        showToast('Only .zip files are allowed for restoring', 'error');
        fileInput.value = '';
        return;
    }

    if (!confirm('WARNING: Restoring a backup will override your current accounts.json and all existing instances. Are you sure you want to proceed?')) {
        fileInput.value = '';
        return;
    }

    const formData = new FormData();
    formData.append('file', file);

    const btn = document.getElementById('btn-restore-backup');
    const oldText = btn.innerHTML;
    btn.innerHTML = `<i data-lucide="loader" class="spin"></i> Restoring...`;
    btn.disabled = true;
    lucide.createIcons();
    showToast('Uploading & Extracting backup... Do not close the window.', 'success');

    try {
        const res = await secureFetch('/api/admin/restore_backup', {
            method: 'POST',
            body: formData
        });
        if (!res) return;
        const data = await res.json();

        if (data.success) {
            showToast('Server Restore Complete! Refreshing...', 'success');
            setTimeout(() => window.location.href = '/', 1500);
        } else {
            showToast(data.error || 'Restore failed', 'error');
        }
    } catch (e) {
        showToast('Upload failed', 'error');
    } finally {
        fileInput.value = '';
        btn.innerHTML = oldText;
        btn.disabled = false;
        lucide.createIcons();
    }
}
