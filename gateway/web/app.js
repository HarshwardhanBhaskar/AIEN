let currentPrivateKey = null;
let currentPublicKeyHex = '';

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
    showToast("Keypair generated successfully!", "success");
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
    showToast("No active private key loaded. Click generate keys first.", "error");
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

    showToast(`Intent submitted! ID: ${resJSON.intent_id}`, "success");
    
    // Quick refresh of lists
    fetchIntents();
    fetchLedger();
  } catch (err) {
    showToast(err.message, "error");
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
    intents.forEach(intent => {
      const card = document.createElement('div');
      card.className = 'pipeline-item';
      card.innerHTML = `
        <span class="item-id" title="${intent.id}">${intent.id.substring(0, 8)}...</span>
        <span class="item-type">${intent.type.replace('INTENT_TYPE_', '')}</span>
      `;

      if (intent.status === 'INTENT_STATUS_PENDING') {
        pendingList.appendChild(card);
      } else if (intent.status === 'INTENT_STATUS_SCHEDULED') {
        scheduledList.appendChild(card);
      } else if (intent.status === 'INTENT_STATUS_EXECUTING') {
        executingList.appendChild(card);
      } else if (intent.status === 'INTENT_STATUS_COMPLETED' || intent.status === 'INTENT_STATUS_FAILED') {
        completedList.appendChild(card);
      }
    });
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
          <td colspan="6" class="empty-state">No blocks synced yet. Submit an intent above!</td>
        </tr>
      `;
      return;
    }

    tbody.innerHTML = '';
    blocks.forEach((block, index) => {
      const tr = document.createElement('tr');
      // Highlight the first (newest) row with a pulsing fade
      if (index === 0) {
        tr.style.backgroundColor = 'rgba(16, 185, 129, 0.03)';
      }
      tr.innerHTML = `
        <td><strong>#${block.index}</strong></td>
        <td><span class="item-id" title="${block.intent_id}">${block.intent_id.substring(0, 18)}...</span></td>
        <td><span class="badge badge-sec">${block.type.replace('INTENT_TYPE_', '')}</span></td>
        <td><span class="badge ${block.status === 'INTENT_STATUS_COMPLETED' ? 'online' : 'badge-sec'}">${block.status.replace('INTENT_STATUS_', '')}</span></td>
        <td class="hash-cell" title="${block.prev_hash}">${block.prev_hash.substring(0, 12)}...</td>
        <td class="hash-cell highlight" title="${block.hash}">${block.hash.substring(0, 12)}...</td>
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
    userProfile.style.display = 'inline';
    btnLogout.style.display = 'inline';
    
    // Initial fetch once authenticated
    fetchIntents();
    fetchLedger();
  } else {
    loginOverlay.classList.remove('hidden');
    userProfile.style.display = 'none';
    btnLogout.style.display = 'none';
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
