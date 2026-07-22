// popup.js - Extension Popup Logic & Web DB Controller

const logOutput = document.getElementById('log-output');
const logContainer = document.getElementById('log-container');

let selectedScanItems = new Set();

function logToConsole(message, type = 'info') {
    const li = document.createElement('li');
    li.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
    switch(type) {
        case 'error': li.className = 'text-red-400'; break;
        case 'success': li.className = 'text-emerald-400'; break;
        case 'warn': li.className = 'text-amber-400'; break;
        default: li.className = 'text-slate-300';
    }
    logOutput.appendChild(li);
    logContainer.scrollTop = logContainer.scrollHeight;
}

// Tab Switching
function switchTab(tabId) {
    const tabs = ['url', 'account', 'db'];
    tabs.forEach(t => {
        const tabBtn = document.getElementById(`tab-${t}`);
        const view = document.getElementById(`view-${t}`);
        if (t === tabId) {
            tabBtn.classList.replace('text-slate-500', 'text-slate-100');
            tabBtn.classList.replace('border-transparent', 'border-slate-100');
            view.classList.remove('hidden');
            if (t === 'db') loadHistoryUI();
        } else {
            tabBtn.classList.replace('text-slate-100', 'text-slate-500');
            tabBtn.classList.replace('border-slate-100', 'border-transparent');
            view.classList.add('hidden');
        }
    });
}

// Direct URL Download
async function startDirectDownload() {
    const urlInput = document.getElementById('url-input').value.trim();
    if (!urlInput) {
        logToConsole('다운로드할 미디어 URL을 입력해주세요.', 'warn');
        return;
    }

    logToConsole(`URL 추출 시작: ${urlInput}`);
    
    // Analyze URL platform
    let platform = 'Web';
    if (urlInput.includes('tiktok.com')) platform = 'TikTok';
    else if (urlInput.includes('instagram.com')) platform = 'Instagram';
    else if (urlInput.includes('youtube.com') || urlInput.includes('youtu.be')) platform = 'YouTube';

    // Trigger download via Background Worker
    chrome.runtime.sendMessage({
        action: 'DOWNLOAD_MEDIA',
        payload: {
            url: urlInput,
            platform: platform,
            mediaType: 'video',
            filename: `${platform}_${Date.now()}`
        }
    }, (response) => {
        if (response && response.success) {
            logToConsole('다운로드가 정상적으로 시작되었습니다!', 'success');
        } else {
            logToConsole(`다운로드 오류: ${response ? response.error : '알 수 없는 오류'}`, 'error');
        }
    });
}

// Account Scan
async function startAccountScan() {
    const platform = document.getElementById('platform-select').value;
    const accountInput = document.getElementById('account-input').value.trim();
    
    if (!accountInput) {
        logToConsole('계정 아이디를 입력해주세요.', 'warn');
        return;
    }

    logToConsole(`@${accountInput} 계정의 ${platform} 피드를 스캔합니다...`);

    const galleryContainer = document.getElementById('account-gallery');
    const grid = document.getElementById('scan-grid');
    galleryContainer.classList.remove('hidden');
    grid.innerHTML = '';
    selectedScanItems.clear();
    updateScanBtnText();

    // Mock scan items for demonstration (In production, content script fetches feed items)
    const mockItems = [
        { id: '1', thumb: 'https://picsum.photos/200/300?random=1', url: 'https://picsum.photos/200/300?random=1', type: 'video' },
        { id: '2', thumb: 'https://picsum.photos/200/300?random=2', url: 'https://picsum.photos/200/300?random=2', type: 'photo' },
        { id: '3', thumb: 'https://picsum.photos/200/300?random=3', url: 'https://picsum.photos/200/300?random=3', type: 'photo' },
        { id: '4', thumb: 'https://picsum.photos/200/300?random=4', url: 'https://picsum.photos/200/300?random=4', type: 'video' },
    ];

    mockItems.forEach(item => {
        const card = document.createElement('div');
        card.className = 'gallery-card relative rounded border border-slate-800 bg-slate-900 aspect-[9/16] overflow-hidden cursor-pointer';
        card.onclick = () => {
            if (selectedScanItems.has(item.id)) {
                selectedScanItems.delete(item.id);
                card.classList.remove('selected');
            } else {
                selectedScanItems.add(item.id);
                card.classList.add('selected');
            }
            updateScanBtnText();
        };

        card.innerHTML = `
            <img src="${item.thumb}" class="w-full h-full object-cover" alt="thumb">
            <span class="absolute bottom-1 left-1 text-[9px] font-bold px-1 rounded bg-slate-950/80 text-white">${item.type.toUpperCase()}</span>
        `;
        grid.appendChild(card);
    });

    logToConsole(`스캔 완료: 총 ${mockItems.length}개의 미디어를 찾았습니다.`, 'success');
}

function updateScanBtnText() {
    const btn = document.getElementById('btn-download-selected');
    btn.textContent = `선택 다운로드 (${selectedScanItems.size})`;
}

function downloadSelectedFromScan() {
    if (selectedScanItems.size === 0) return;
    logToConsole(`${selectedScanItems.size}개 미디어 일괄 다운로드를 시작합니다.`);
    selectedScanItems.forEach(id => {
        chrome.runtime.sendMessage({
            action: 'DOWNLOAD_MEDIA',
            payload: {
                url: `https://picsum.photos/200/300?random=${id}`,
                platform: document.getElementById('platform-select').value,
                mediaType: 'photo',
                filename: `Scan_Batch_${id}`
            }
        });
    });
    logToConsole('일괄 다운로드 요청 완료!', 'success');
}

// Load Web DB History UI
async function loadHistoryUI() {
    const container = document.getElementById('history-container');
    container.innerHTML = '<p class="text-slate-500 text-center py-4">Web DB 로딩 중...</p>';

    chrome.runtime.sendMessage({ action: 'GET_HISTORY' }, (response) => {
        if (!response || !response.success || !response.data || response.data.length === 0) {
            container.innerHTML = '<p class="text-slate-500 text-center py-4">저장된 다운로드 기록이 없습니다.</p>';
            return;
        }

        container.innerHTML = '';
        response.data.forEach(item => {
            const div = document.createElement('div');
            div.className = 'p-2 rounded border border-slate-800 bg-slate-950 flex justify-between items-center';
            
            const isSuccess = item.status === 'Success';
            const dateStr = new Date(item.timestamp).toLocaleString('ko-KR', { hour12: false, month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit' });

            div.innerHTML = `
                <div class="overflow-hidden pr-2">
                    <div class="font-medium text-slate-200 truncate">${item.title}</div>
                    <div class="text-[10px] text-slate-500 flex items-center gap-1.5 mt-0.5">
                        <span class="px-1 rounded bg-slate-800 text-slate-300 font-bold">${item.platform}</span>
                        <span>${dateStr}</span>
                    </div>
                </div>
                <span class="text-[10px] px-1.5 py-0.5 rounded font-semibold ${isSuccess ? 'bg-emerald-950 text-emerald-400 border border-emerald-800' : 'bg-red-950 text-red-400 border border-red-800'}">
                    ${item.status}
                </span>
            `;
            container.appendChild(div);
        });
    });
}

function clearHistoryRecords() {
    if (!confirm('IndexedDB의 모든 다운로드 이력을 삭제하시겠습니까?')) return;
    chrome.runtime.sendMessage({ action: 'CLEAR_HISTORY' }, () => {
        loadHistoryUI();
        logToConsole('IndexedDB 다운로드 이력이 삭제되었습니다.', 'warn');
    });
}
