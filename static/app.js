/**
 * P2P WebRTC File Transfer - Frontend Logic (app.js)
 * Clean separation: WebSocket signaling + RTCPeerConnection + DataChannel + File chunking
 * All console.log are intentional for debugging as requested.
 */

let ws = null;
let pc = null;
let dc = null;
let isOfferer = false;
let currentRoomId = null;
let receivedChunks = [];
let expectedFileSize = 0;
let receivedFileName = '';
let receivedFileMime = 'application/octet-stream';
let currentTransferType = null; // 'send' | 'receive'

// ==================== UI HELPERS ====================
function updateStatus(message, isError = false) {
    const statusEl = document.getElementById('status-bar');
    if (!statusEl) return;
    statusEl.innerHTML = `<span class="${isError ? 'text-red-400' : 'text-zinc-400'}">${message}</span>`;
    if (!isError) {
        setTimeout(() => {
            if (statusEl.innerHTML.includes(message)) statusEl.innerHTML = '';
        }, 4500);
    }
}

function showSection(sectionId) {
    ['initial-section', 'waiting-section', 'connected-section'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.classList.add('hidden');
    });
    const target = document.getElementById(sectionId);
    if (target) target.classList.remove('hidden');
}

function showTransferUI(filename, type = 'send') {
    currentTransferType = type;
    const statusDiv = document.getElementById('transfer-status');
    const icon = document.getElementById('transfer-icon');
    const nameEl = document.getElementById('transfer-filename');
    const subEl = document.getElementById('transfer-subtitle');

    nameEl.textContent = filename;
    statusDiv.classList.remove('hidden');

    if (type === 'send') {
        icon.className = 'fa-solid fa-upload text-indigo-400';
        subEl.textContent = 'Đang gửi file qua P2P...';
    } else {
        icon.className = 'fa-solid fa-download text-emerald-400';
        subEl.textContent = 'Đang nhận file từ thiết bị kia...';
    }
}

function updateProgress(percent, transferredBytes = 0, totalBytes = 0) {
    const bar = document.getElementById('progress-bar');
    const percentEl = document.getElementById('transfer-percent');
    const sizeEl = document.getElementById('transfer-size');

    if (!bar || !percentEl) return;

    const clamped = Math.min(Math.max(percent, 0), 100);
    bar.style.width = `${clamped}%`;
    percentEl.textContent = `${Math.round(clamped)}%`;

    if (sizeEl && totalBytes > 0) {
        const transferredMB = (transferredBytes / (1024 * 1024)).toFixed(1);
        const totalMB = (totalBytes / (1024 * 1024)).toFixed(1);
        sizeEl.textContent = `${transferredMB} / ${totalMB} MB`;
    }
}

function hideTransferUI() {
    const statusDiv = document.getElementById('transfer-status');
    if (statusDiv) statusDiv.classList.add('hidden');
    const bar = document.getElementById('progress-bar');
    if (bar) bar.style.width = '0%';
    currentTransferType = null;
}

function resetApp() {
    if (ws) {
        ws.close();
        ws = null;
    }
    if (pc) {
        pc.close();
        pc = null;
    }
    dc = null;
    receivedChunks = [];
    expectedFileSize = 0;
    hideTransferUI();
    showSection('initial-section');
    updateStatus('Đã reset ứng dụng');
    // Clear QR if any
    const qrContainer = document.getElementById('qr-container');
    if (qrContainer) qrContainer.innerHTML = '';
}

// ==================== WEBSOCKET SIGNALING ====================
function connectWebSocket(roomId) {
    return new Promise((resolve, reject) => {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/ws/${roomId}`;
        
        console.log(`[WS] Connecting to ${wsUrl}`);
        ws = new WebSocket(wsUrl);

        ws.onopen = () => {
            console.log('[WS] Connected successfully');
            currentRoomId = roomId;
            // Send join event (as per design)
            ws.send(JSON.stringify({ type: 'join', room_id: roomId }));
            updateStatus('Đã kết nối signaling server');
            resolve();
        };

        ws.onmessage = (event) => {
            let msg;
            try {
                msg = JSON.parse(event.data);
            } catch (e) {
                console.warn('[WS] Received non-JSON message');
                return;
            }
            console.log(`[WS] Received: ${msg.type}`, msg);

            switch (msg.type) {
                case 'peer-joined':
                    console.log('[WebRTC] Peer joined the room → Starting WebRTC setup');
                    setupWebRTC();
                    if (isOfferer) {
                        createOffer();
                    }
                    break;

                case 'offer':
                    handleOffer(msg.sdp || msg.data?.sdp);
                    break;

                case 'answer':
                    handleAnswer(msg.sdp || msg.data?.sdp);
                    break;

                case 'ice_candidate':
                    handleIceCandidate(msg.candidate || msg.data?.candidate);
                    break;

                case 'peer-disconnected':
                    updateStatus('Thiết bị kia đã ngắt kết nối', true);
                    setTimeout(() => {
                        if (confirm('Thiết bị kia đã rời phòng. Reset ứng dụng?')) {
                            resetApp();
                        }
                    }, 800);
                    break;

                default:
                    console.log('[WS] Unknown message type:', msg.type);
            }
        };

        ws.onerror = (err) => {
            console.error('[WS] Error:', err);
            updateStatus('Lỗi kết nối WebSocket', true);
            reject(err);
        };

        ws.onclose = (event) => {
            console.log('[WS] Closed. Code:', event.code);
            if (event.code === 4000) {
                updateStatus('Phòng đã đầy (tối đa 2 thiết bị)', true);
            }
            ws = null;
        };
    });
}

// ==================== WEBRTC CORE ====================
function setupWebRTC() {
    if (pc) {
        console.log('[WebRTC] RTCPeerConnection already exists');
        return;
    }

    console.log('[WebRTC] Creating new RTCPeerConnection');
    const config = {
        iceServers: [
            { urls: 'stun:stun.l.google.com:19302' },
            { urls: 'stun:stun1.l.google.com:19302' }
        ]
    };

    pc = new RTCPeerConnection(config);

    // ICE Candidate handling
    pc.onicecandidate = (event) => {
        if (event.candidate && ws && ws.readyState === WebSocket.OPEN) {
            console.log('[WebRTC] New ICE candidate generated → sending to peer');
            ws.send(JSON.stringify({
                type: 'ice_candidate',
                candidate: event.candidate
            }));
        }
    };

    pc.oniceconnectionstatechange = () => {
        console.log('[WebRTC] ICE connection state:', pc.iceConnectionState);
        if (pc.iceConnectionState === 'connected') {
            console.log('[WebRTC] ICE connected successfully!');
        }
        if (['disconnected', 'failed', 'closed'].includes(pc.iceConnectionState)) {
            updateStatus('Kết nối P2P bị gián đoạn', true);
        }
    };

    pc.onconnectionstatechange = () => {
        console.log('[WebRTC] Connection state:', pc.connectionState);
    };

    // Data Channel setup
    if (isOfferer) {
        console.log('[WebRTC] Creating DataChannel as offerer');
        dc = pc.createDataChannel('fileTransfer', {
            ordered: true,
            maxRetransmits: 30
        });
        setupDataChannel(dc);
    } else {
        console.log('[WebRTC] Waiting for DataChannel (will receive via ondatachannel)');
        pc.ondatachannel = (event) => {
            console.log('[WebRTC] Received DataChannel from peer');
            dc = event.channel;
            setupDataChannel(dc);
        };
    }
}

function setupDataChannel(channel) {
    channel.binaryType = 'arraybuffer';

    channel.onopen = () => {
        console.log('%c[DataChannel] OPENED — P2P ready for file transfer!', 'color:#22c55e; font-weight:600');
        updateStatus('Kênh dữ liệu P2P đã sẵn sàng');
        showSection('connected-section');
        // Hide transfer UI if previously shown
        hideTransferUI();
    };

    channel.onmessage = (event) => {
        handleDataChannelMessage(event.data);
    };

    channel.onclose = () => {
        console.log('[DataChannel] Closed');
        updateStatus('Kênh dữ liệu đã đóng', true);
    };

    channel.onerror = (err) => {
        console.error('[DataChannel] Error:', err);
    };
}

// ==================== OFFER / ANSWER / ICE ====================
function createOffer() {
    if (!pc) {
        console.error('[WebRTC] No RTCPeerConnection');
        return;
    }
    console.log('[WebRTC] Creating SDP Offer...');
    
    pc.createOffer({
        offerToReceiveAudio: false,
        offerToReceiveVideo: false
    })
    .then(offer => {
        console.log('[WebRTC] Offer created, setting local description');
        return pc.setLocalDescription(offer);
    })
    .then(() => {
        if (ws && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({
                type: 'offer',
                sdp: pc.localDescription.sdp
            }));
            console.log('[WebRTC] Offer sent to peer via signaling');
        }
    })
    .catch(err => {
        console.error('[WebRTC] Failed to create offer:', err);
        updateStatus('Không thể tạo offer WebRTC', true);
    });
}

function handleOffer(sdp) {
    if (!pc) {
        console.error('[WebRTC] Received offer but no RTCPeerConnection');
        return;
    }
    console.log('[WebRTC] Received offer from peer. Setting remote description...');

    const offerDesc = new RTCSessionDescription({ type: 'offer', sdp });
    pc.setRemoteDescription(offerDesc)
        .then(() => {
            console.log('[WebRTC] Remote offer set. Creating answer...');
            return pc.createAnswer();
        })
        .then(answer => {
            return pc.setLocalDescription(answer);
        })
        .then(() => {
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({
                    type: 'answer',
                    sdp: pc.localDescription.sdp
                }));
                console.log('[WebRTC] Answer sent back to peer');
            }
        })
        .catch(err => {
            console.error('[WebRTC] Error handling offer:', err);
        });
}

function handleAnswer(sdp) {
    if (!pc) return;
    console.log('[WebRTC] Received answer. Setting remote description...');
    const answerDesc = new RTCSessionDescription({ type: 'answer', sdp });
    pc.setRemoteDescription(answerDesc)
        .then(() => {
            console.log('[WebRTC] Remote answer set successfully. ICE negotiation ongoing...');
        })
        .catch(err => console.error('[WebRTC] setRemoteDescription (answer) error:', err));
}

function handleIceCandidate(candidate) {
    if (!pc || !candidate) return;
    console.log('[WebRTC] Adding received ICE candidate');
    pc.addIceCandidate(new RTCIceCandidate(candidate))
        .catch(err => console.warn('[WebRTC] addIceCandidate error (can be normal):', err));
}

// ==================== FILE TRANSFER LOGIC ====================
const CHUNK_SIZE = 16 * 1024; // 16KB - good balance for browser

function handleFileSelect(event) {
    const file = event.target.files[0];
    if (!file) return;

    if (!dc || dc.readyState !== 'open') {
        alert('Kênh dữ liệu chưa sẵn sàng. Vui lòng đợi kết nối hoàn tất.');
        return;
    }

    console.log(`[File] Selected file: ${file.name} (${file.size} bytes)`);
    sendFile(file);
    // Reset input so same file can be selected again
    event.target.value = '';
}

async function sendFile(file) {
    if (!dc || dc.readyState !== 'open') {
        updateStatus('Không thể gửi — DataChannel chưa mở', true);
        return;
    }

    // 1. Send metadata first
    const meta = {
        type: 'file-meta',
        name: file.name,
        size: file.size,
        mime: file.type || 'application/octet-stream'
    };
    dc.send(JSON.stringify(meta));
    console.log('[File] Sent file metadata:', meta);

    showTransferUI(file.name, 'send');
    updateProgress(0, 0, file.size);

    // 2. Chunk and send
    let offset = 0;
    const totalSize = file.size;

    function readNextChunk() {
        if (offset >= totalSize) {
            // Done
            setTimeout(() => {
                console.log('%c[File] File transfer completed (all chunks sent)', 'color:#22c55e');
                updateProgress(100, totalSize, totalSize);
                setTimeout(() => {
                    hideTransferUI();
                    updateStatus(`Đã gửi xong: ${file.name}`);
                }, 1200);
            }, 300);
            return;
        }

        const chunk = file.slice(offset, offset + CHUNK_SIZE);
        const reader = new FileReader();

        reader.onload = (e) => {
            const buffer = e.target.result;
            try {
                dc.send(buffer);
            } catch (sendErr) {
                console.error('[File] dc.send error:', sendErr);
                return;
            }

            offset += CHUNK_SIZE;
            const progress = (offset / totalSize) * 100;
            updateProgress(progress, Math.min(offset, totalSize), totalSize);

            // Continue with next chunk (sequential to avoid buffer overflow)
            setTimeout(readNextChunk, 4); // tiny delay for browser responsiveness
        };

        reader.onerror = (err) => {
            console.error('[File] FileReader error:', err);
            updateStatus('Lỗi đọc file', true);
        };

        reader.readAsArrayBuffer(chunk);
    }

    // Start chunking
    readNextChunk();
}

function handleDataChannelMessage(data) {
    // Metadata (JSON string)
    if (typeof data === 'string') {
        try {
            const msg = JSON.parse(data);
            if (msg.type === 'file-meta') {
                console.log('[File] Receiving file metadata:', msg);
                receivedChunks = [];
                expectedFileSize = msg.size;
                receivedFileName = msg.name;
                receivedFileMime = msg.mime || 'application/octet-stream';

                showTransferUI(receivedFileName, 'receive');
                updateProgress(0, 0, expectedFileSize);
            }
        } catch (e) {
            console.warn('[File] Failed to parse string message as JSON');
        }
        return;
    }

    // Binary chunk (ArrayBuffer)
    if (data instanceof ArrayBuffer) {
        receivedChunks.push(data);
        const receivedBytes = receivedChunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);

        const progress = expectedFileSize > 0 ? (receivedBytes / expectedFileSize) * 100 : 0;
        updateProgress(progress, receivedBytes, expectedFileSize);

        console.log(`[File] Received chunk — total: ${receivedBytes}/${expectedFileSize} bytes`);

        if (receivedBytes >= expectedFileSize && expectedFileSize > 0) {
            console.log('%c[File] All chunks received. Assembling file...', 'color:#22c55e');
            assembleAndDownloadFile();
        }
    }
}

function assembleAndDownloadFile() {
    if (receivedChunks.length === 0) return;

    const blob = new Blob(receivedChunks, { type: receivedFileMime });
    const url = URL.createObjectURL(blob);

    const a = document.createElement('a');
    a.href = url;
    a.download = receivedFileName || 'received-file';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);

    console.log(`[File] File downloaded: ${receivedFileName}`);
    updateStatus(`Đã nhận và tải xuống: ${receivedFileName}`);

    // Cleanup
    setTimeout(() => {
        hideTransferUI();
        receivedChunks = [];
        expectedFileSize = 0;
    }, 1500);
}

function cancelTransfer() {
    hideTransferUI();
    receivedChunks = [];
    expectedFileSize = 0;
    updateStatus('Đã hủy transfer');
    // Note: We don't close dc here — user can still send/receive other files
}

// ==================== ROOM CREATION / JOIN ====================
async function createRoom() {
    isOfferer = true;
    const roomId = generateRoomId();
    currentRoomId = roomId;

    console.log(`[Room] Creating new room as OFFERER: ${roomId}`);

    // Show waiting UI
    showSection('waiting-section');
    document.getElementById('room-id-display').textContent = roomId;

    // Build shareable link
    const shareUrl = `${window.location.origin}/?room=${roomId}`;
    document.getElementById('share-link').textContent = shareUrl;

    // Generate QR Code
    const qrContainer = document.getElementById('qr-container');
    qrContainer.innerHTML = '';
    try {
        new QRCode(qrContainer, {
            text: shareUrl,
            width: 170,
            height: 170,
            colorDark: '#18181b',
            colorLight: '#ffffff',
            correctLevel: QRCode.CorrectLevel.M
        });
    } catch (e) {
        console.error('QR Code generation failed:', e);
        qrContainer.innerHTML = '<div class="text-xs text-red-400 p-4">Không thể tạo QR</div>';
    }

    // Connect WebSocket
    try {
        await connectWebSocket(roomId);
        updateStatus('Phòng đã tạo. Đang chờ thiết bị kia tham gia...');
    } catch (err) {
        updateStatus('Không thể kết nối signaling server', true);
        showSection('initial-section');
    }
}

function joinRoomFromInput() {
    const input = document.getElementById('room-input');
    const roomId = input.value.trim().toUpperCase();
    if (!roomId || roomId.length < 4) {
        alert('Vui lòng nhập Room ID hợp lệ (ít nhất 4 ký tự)');
        return;
    }
    joinRoom(roomId);
}

async function joinRoom(roomId) {
    isOfferer = false;
    console.log(`[Room] Joining room as ANSWERER: ${roomId}`);

    showSection('waiting-section');
    document.getElementById('room-id-display').textContent = roomId;
    document.getElementById('share-link').textContent = 'Đang tham gia phòng...';
    document.getElementById('qr-container').innerHTML = 
        '<div class="text-center p-6 text-xs text-zinc-400">Đang kết nối với phòng...</div>';

    try {
        await connectWebSocket(roomId);
        updateStatus('Đã tham gia phòng. Đang chờ peer...');
    } catch (err) {
        updateStatus('Không thể tham gia phòng', true);
        showSection('initial-section');
    }
}

function generateRoomId() {
    // 8 character uppercase alphanumeric (easy to read/share)
    return Math.random().toString(36).substring(2, 10).toUpperCase();
}

function copyShareLink() {
    const linkEl = document.getElementById('share-link');
    const text = linkEl.textContent;
    if (!text || text.includes('Đang')) return;

    navigator.clipboard.writeText(text).then(() => {
        const original = linkEl.textContent;
        linkEl.textContent = 'Đã copy!';
        setTimeout(() => {
            linkEl.textContent = original;
        }, 1600);
    }).catch(() => {
        // Fallback
        prompt('Copy link này:', text);
    });
}

// ==================== AUTO JOIN FROM URL ====================
function checkUrlForRoom() {
    const params = new URLSearchParams(window.location.search);
    const roomFromUrl = params.get('room');
    if (roomFromUrl) {
        console.log('[Init] Auto-join from URL param:', roomFromUrl);
        // Clean URL
        window.history.replaceState({}, document.title, window.location.pathname);
        // Auto join as receiver
        setTimeout(() => {
            joinRoom(roomFromUrl.toUpperCase());
        }, 300);
    }
}

// ==================== BOOTSTRAP ====================
document.addEventListener('DOMContentLoaded', () => {
    console.log('%c[P2P File Transfer] Frontend initialized', 'color:#64748b');
    
    // Keyboard support for join input
    const roomInput = document.getElementById('room-input');
    if (roomInput) {
        roomInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                joinRoomFromInput();
            }
        });
        // Auto uppercase
        roomInput.addEventListener('input', () => {
            roomInput.value = roomInput.value.toUpperCase();
        });
    }

    // Check for deep link (?room=XXXX)
    checkUrlForRoom();

    // Easter egg / dev hint
    console.log('%c[Hint] Open DevTools → Console to see detailed WebRTC signaling logs', 'color:#475569');
});
