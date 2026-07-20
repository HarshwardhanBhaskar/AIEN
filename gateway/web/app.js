let currentPrivateKey = null;
let currentPublicKeyHex = '';
let lastCompletedCount = 0;
let lastSyncTime = Date.now();

function getAuthHeader() {
  const token = localStorage.getItem("jwt_token");
  return token ? `Bearer ${token}` : '';
}

// DOM Elements
const btnGenerateKeys = document.getElementById('btnGenerateKeys');
const loginOverlay = document.getElementById('loginOverlay');
const loginForm = document.getElementById('loginForm');
const loginUsername = document.getElementById('loginUsername');
const loginPassword = document.getElementById('loginPassword');
const btnLogout = document.getElementById('btnLogout');
const userProfile = document.getElementById('userProfile');
const txtPublicKey = document.getElementById('publicKey');
const txtPrivateKey = document.getElementById('privateKey');
const btnSubmitIntent = document.getElementById('btnSubmitIntent');
const intentForm = document.getElementById('intentForm');
const selIntentType = document.getElementById('intentType');
const txtSubmitterId = document.getElementById('submitterId');
const txtPayload = document.getElementById('payload');
const sigBytesPreview = document.getElementById('sigBytesPreview');
const sigValidationBadge = document.getElementById('sigValidationBadge');
const tpsCounter = document.getElementById('tpsCounter');
const terminalLog = document.getElementById('terminalLog');

// Copy Field Helper
function copyField(fieldId) {
  const input = document.getElementById(fieldId);
  if (!input || !input.value) {
    showToast("Field is empty, nothing to copy.", "error");
    return;
  }
  input.select();
  input.setSelectionRange(0, 99999);
  navigator.clipboard.writeText(input.value);
  showToast("Copied to clipboard!", "success");
}

// Terminal Logging Helper
function appendTerminalLog(message, type = 'system') {
  const line = document.createElement('div');
  const now = new Date().toLocaleTimeString();
  line.className = `terminal-line ${type}`;
  line.textContent = `[${now}] ${type.toUpperCase()} - ${message}`;
  terminalLog.appendChild(line);
  
  // Auto scroll to bottom
  terminalLog.scrollTop = terminalLog.scrollHeight;
  
  // Limit terminal history to 100 lines
  while (terminalLog.children.length > 100) {
    terminalLog.removeChild(terminalLog.firstChild);
  }
}

function clearTerminal() {
  terminalLog.innerHTML = `<div class="terminal-line system">[SYSTEM] Console log cleared. Monitoring active...</div>`;
}

// Helpers: buffer to hex
function bufToHex(buffer) {
  return Array.from(new Uint8Array(buffer))
    .map(b => b.toString(16).padStart(2, '0'))
    .join('');
}

// Generate Ed25519 Keys
async function generateKeys() {
  try {
    const keyPair = await window.crypto.subtle.generateKey(
      {
        name: "Ed25519"
      },
      true,
      ["sign", "verify"]
    );

    currentPrivateKey = keyPair.privateKey;
    
    // Export raw public key
    const rawPubKey = await window.crypto.subtle.exportKey("raw", keyPair.publicKey);
    currentPublicKeyHex = bufToHex(rawPubKey);

    txtPublicKey.value = currentPublicKeyHex;
    txtPrivateKey.value = "ed25519_in_memory_session_seed_keys";
    
    btnSubmitIntent.disabled = false;
    sigValidationBadge.className = "sys-badge valid";
    sigValidationBadge.textContent = "KEY LOADED (VALID)";
    
    showToast("Keypair generated successfully!", "success");
    appendTerminalLog(`Generated local Ed25519 Keypair. PubKey: ${currentPublicKeyHex.substring(0, 16)}...`, 'system');
    updateSigPreview();
  } catch (err) {
    showToast("Keypair generation failed: " + err.message, "error");
  }
}

// Get deterministic sign string format
function getSignString() {
  const type = selIntentType.value;
  const submitterId = txtSubmitterId.value;
  const payload = txtPayload.value.trim();
  return `INTENT_TYPE_${type}|${submitterId}|${payload}`;
}

// Update Signature bytes preview area
function updateSigPreview() {
  sigBytesPreview.textContent = getSignString();
}

// Submit Intent
async function submitIntent(e) {
  e.preventDefault();
  if (!currentPrivateKey) {
    showToast("No active private key loaded.", "error");
    return;
  }

  try {
    const signString = getSignString();
    const encoder = new TextEncoder();
    const dataBytes = encoder.encode(signString);

    // Compute signature using Web Crypto API
    const signatureBuffer = await window.crypto.subtle.sign(
      {
        name: "Ed25519"
      },
      currentPrivateKey,
      dataBytes
    );
    const signatureHex = bufToHex(signatureBuffer);

    // Build JSON payload
    const requestData = {
      type: selIntentType.value,
      submitter_id: txtSubmitterId.value,
      payload: txtPayload.value.trim(),
      signature: signatureHex,
      submitter_public_key: currentPublicKeyHex
    };

    appendTerminalLog(`Signing intent payload using local Ed25519 certificate...`, 'system');

    // Post to gateway
    const response = await fetch('/intents', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': getAuthHeader()
      },
      body: JSON.stringify(requestData)
    });

    const resJSON = await response.json();
    if (!response.ok) {
      throw new Error(resJSON.error || "Submission failed");
    }

    showToast(`Intent submitted! ID: ${resJSON.intent_id.substring(0,8)}...`, "success");
    appendTerminalLog(`Successfully submitted intent ${resJSON.intent_id} (Type: ${requestData.type})`, 'nats');
    
    // Quick refresh of lists
    fetchIntents();
    fetchLedger();
  } catch (err) {
    showToast(err.message, "error");
    appendTerminalLog(`Failed to submit intent: ${err.message}`, 'exec');
  }
}

// Fetch intents list to populate pipeline columns
async function fetchIntents() {
  try {
    const auth = getAuthHeader();
    if (!auth) return;

    const response = await fetch('/intents', {
      headers: {
        'Authorization': auth
      }
    });
    if (!response.ok) return;
    const data = await response.json();

    const pendingList = document.getElementById('listPending');
    const scheduledList = document.getElementById('listScheduled');
    const executingList = document.getElementById('listExecuting');
    const completedList = document.getElementById('listCompleted');

    // Clear lists
    pendingList.innerHTML = '';
    scheduledList.innerHTML = '';
    executingList.innerHTML = '';
    completedList.innerHTML = '';

    const intents = data.intents || [];
    let countPen = 0, countSch = 0, countExe = 0, countCom = 0;

    intents.forEach(intent => {
      // Parse details from JSON payload
      let detailsHtml = '';
      try {
        const payloadData = JSON.parse(intent.payload);
        if (payloadData.from && payloadData.to) {
          detailsHtml = `
            <div class="tx-accounts">
              <span class="acc-pill" title="${payloadData.from}">${payloadData.from}</span>
              <span>➜</span>
              <span class="acc-pill" title="${payloadData.to}">${payloadData.to}</span>
            </div>
            <div class="tx-amount">$${parseFloat(payloadData.amount).toFixed(2)}</div>
          `;
        }
      } catch (e) {
        // Fallback for non-json
        detailsHtml = `<div class="tx-accounts" style="font-size:0.65rem; word-break:break-all;">${intent.payload}</div>`;
      }

      const card = document.createElement('div');
      card.className = 'transaction-card';
      const cleanType = intent.type.replace('INTENT_TYPE_', '').toLowerCase();
      card.innerHTML = `
        <div class="tx-header">
          <span class="tx-id" title="${intent.id}">${intent.id.substring(0, 8)}...</span>
          <span class="tx-badge ${cleanType}">${cleanType}</span>
        </div>
        ${detailsHtml}
      `;

      if (intent.status === 'INTENT_STATUS_PENDING') {
        pendingList.appendChild(card);
        countPen++;
      } else if (intent.status === 'INTENT_STATUS_SCHEDULED') {
        scheduledList.appendChild(card);
        countSch++;
      } else if (intent.status === 'INTENT_STATUS_EXECUTING') {
        executingList.appendChild(card);
        countExe++;
      } else if (intent.status === 'INTENT_STATUS_COMPLETED' || intent.status === 'INTENT_STATUS_FAILED') {
        completedList.appendChild(card);
        countCom++;
      }
    });

    // Update count labels
    document.getElementById('countPending').textContent = countPen;
    document.getElementById('countScheduled').textContent = countSch;
    document.getElementById('countExecuting').textContent = countExe;
    document.getElementById('countCompleted').textContent = countCom;

    // Calculate TPS
    const now = Date.now();
    const timeDelta = (now - lastSyncTime) / 1000.0;
    if (timeDelta > 0 && lastCompletedCount > 0 && countCom > lastCompletedCount) {
      const tps = (countCom - lastCompletedCount) / timeDelta;
      tpsCounter.textContent = tps.toFixed(1);
    }
    lastCompletedCount = countCom;
    lastSyncTime = now;

  } catch (err) {
    console.error("Failed to fetch intents pipeline", err);
  }
}

// Fetch blocks from ledger log
async function fetchLedger() {
  try {
    const auth = getAuthHeader();
    if (!auth) return;

    const response = await fetch('/api/ledger', {
      headers: {
        'Authorization': auth
      }
    });
    if (!response.ok) return;
    const blocks = await response.json();

    const tbody = document.getElementById('ledgerBody');
    if (blocks.length === 0) {
      tbody.innerHTML = `
        <tr>
          <td colspan="5" class="empty-state">No blocks mined yet. Submit an intent above!</td>
        </tr>
      `;
      return;
    }

    // Check if we have new blocks to log in the terminal
    const existingRows = tbody.querySelectorAll('tr').length;
    if (blocks.length > existingRows && existingRows > 1) {
      const newBlock = blocks[0];
      appendTerminalLog(`[BLOCK MINED] Index #${newBlock.index} appended to ledger with hash ${newBlock.hash.substring(0, 12)}...`, 'wal');
    }

    tbody.innerHTML = '';
    blocks.forEach((block, index) => {
      const tr = document.createElement('tr');
      // Highlight the first (newest) row with a pulsing fade
      if (index === 0) {
        tr.style.backgroundColor = 'rgba(59, 130, 246, 0.03)';
      }
      tr.innerHTML = `
        <td><strong>#${block.index}</strong></td>
        <td><span class="tx-id" title="${block.intent_id}">${block.intent_id.substring(0, 12)}...</span></td>
        <td><span class="sys-badge security">${block.type.replace('INTENT_TYPE_', '')}</span></td>
        <td><span class="sys-badge valid">${block.status.replace('INTENT_STATUS_', '')}</span></td>
        <td class="hash-text glow" title="${block.hash}">${block.hash.substring(0, 12)}...</td>
      `;
      tbody.appendChild(tr);
    });
  } catch (err) {
    console.error("Failed to fetch ledger", err);
  }
}

// Toast System
function showToast(message, type = 'info') {
  const container = document.getElementById('toastContainer');
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.innerHTML = `
    <span>${type === 'success' ? '✅' : '❌'}</span>
    <span>${message}</span>
  `;
  container.appendChild(toast);

  setTimeout(() => {
    toast.style.animation = 'slideIn 0.25s reverse ease-in';
    setTimeout(() => toast.remove(), 250);
  }, 4000);
}

// Session & Auth Handling
function checkSession() {
  const token = localStorage.getItem("jwt_token");
  const username = localStorage.getItem("jwt_user");
  if (token && username) {
    loginOverlay.classList.add('hidden');
    userProfile.textContent = username;
    
    // Initial fetch once authenticated
    fetchIntents();
    fetchLedger();
  } else {
    loginOverlay.classList.remove('hidden');
  }
}

async function handleLogin(e) {
  e.preventDefault();
  const username = loginUsername.value.trim();
  const password = loginPassword.value;

  try {
    const response = await fetch('/api/login', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ username, password })
    });

    const resJSON = await response.json();
    if (!response.ok) {
      throw new Error(resJSON.error || "Authentication failed");
    }

    localStorage.setItem("jwt_token", resJSON.token);
    localStorage.setItem("jwt_user", username);
    showToast("Authenticated successfully!", "success");
    appendTerminalLog(`Successfully authenticated administrator session: '${username}'`, 'system');
    
    loginUsername.value = '';
    loginPassword.value = '';
    checkSession();
  } catch (err) {
    showToast(err.message, "error");
  }
}

function logout() {
  localStorage.removeItem("jwt_token");
  localStorage.removeItem("jwt_user");
  showToast("Logged out successfully.", "success");
  checkSession();
}

// Event Listeners
btnGenerateKeys.addEventListener('click', generateKeys);
selIntentType.addEventListener('change', updateSigPreview);
txtSubmitterId.addEventListener('input', updateSigPreview);
txtPayload.addEventListener('input', updateSigPreview);
intentForm.addEventListener('submit', submitIntent);
loginForm.addEventListener('submit', handleLogin);
btnLogout.addEventListener('click', logout);

// Run initialization
checkSession();

// Start Periodic Syncing (Every 2 seconds)
setInterval(fetchIntents, 2000);
setInterval(fetchLedger, 2000);
