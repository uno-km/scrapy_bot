// app.js
let pyodideReady = false;
let pyodideInstance = null;

// UI Elements
const statusIndicator = document.getElementById('status-indicator');
const statusText = document.getElementById('status-text');
const btnDownloadUrl = document.getElementById('btn-download-url');
const btnSearchAccount = document.getElementById('btn-search-account');
const galleryContainer = document.getElementById('gallery-container');
const mediaGrid = document.getElementById('media-grid');
const btnDownloadSelected = document.getElementById('btn-download-selected');

let selectedMediaItems = new Set();
let currentScrapedItemsMap = new Map();
let currentActiveTab = 'url';

// Utility: Logging to UI per Tab
function logToTerminal(message, type = 'info', mode = null) {
    const targetMode = mode || currentActiveTab;
    const logOutput = document.getElementById(`log-output-${targetMode}`);
    const logContainer = document.getElementById(`log-container-${targetMode}`);

    if (!logOutput) return;

    const li = document.createElement('li');
    li.textContent = `[${targetMode.toUpperCase()}] ${message}`;
    switch(type) {
        case 'error': li.className = 'text-red-400'; break;
        case 'success': li.className = 'text-emerald-400'; break;
        case 'warn': li.className = 'text-amber-400'; break;
        default: li.className = 'text-slate-300';
    }
    logOutput.appendChild(li);
    if (logContainer) {
        logContainer.scrollTop = logContainer.scrollHeight;
    }
}

// Modal Controls
function openExtensionModal() {
    const modal = document.getElementById('extension-modal');
    modal.classList.remove('hidden');
    modal.classList.add('flex');
}

function closeExtensionModal() {
    const modal = document.getElementById('extension-modal');
    modal.classList.add('hidden');
    modal.classList.remove('flex');
}

function switchExtTab(browser) {
    const browsers = ['chrome', 'edge', 'whale', 'opera', 'firefox'];
    browsers.forEach(b => {
        const tabBtn = document.getElementById(`ext-tab-${b}`);
        const content = document.getElementById(`ext-content-${b}`);
        if (b === browser) {
            tabBtn.className = 'px-4 py-3 text-xs font-semibold text-slate-100 border-b-2 border-slate-100 whitespace-nowrap';
            content.classList.remove('hidden');
        } else {
            tabBtn.className = 'px-4 py-3 text-xs font-medium text-slate-500 border-b-2 border-transparent hover:text-slate-300 whitespace-nowrap';
            content.classList.add('hidden');
        }
    });
}

// Tab Switching Logic (Fixes Visual Highlight & Active View Display)
function switchTab(tabId) {
    currentActiveTab = tabId;

    const urlTab = document.getElementById('tab-url');
    const accountTab = document.getElementById('tab-account');
    const urlView = document.getElementById('view-url');
    const accountView = document.getElementById('view-account');
    
    // Reset Gallery
    galleryContainer.classList.add('hidden');
    mediaGrid.innerHTML = '';
    selectedMediaItems.clear();
    currentScrapedItemsMap.clear();

    if (tabId === 'url') {
        // Active URL Tab UI
        urlTab.className = 'flex-1 py-3 text-center font-bold text-sm text-white bg-slate-900 border-b-2 border-white rounded-t-xl transition-all';
        accountTab.className = 'flex-1 py-3 text-center font-medium text-sm text-slate-400 bg-transparent border-b-2 border-transparent rounded-t-xl hover:text-slate-200 transition-all';
        
        urlView.classList.remove('hidden');
        accountView.classList.add('hidden');
    } else {
        // Active Account Tab UI
        accountTab.className = 'flex-1 py-3 text-center font-bold text-sm text-white bg-slate-900 border-b-2 border-white rounded-t-xl transition-all';
        urlTab.className = 'flex-1 py-3 text-center font-medium text-sm text-slate-400 bg-transparent border-b-2 border-transparent rounded-t-xl hover:text-slate-200 transition-all';
        
        accountView.classList.remove('hidden');
        urlView.classList.add('hidden');
    }
}

// Initialize Pyodide Runtime
async function initWASM() {
    try {
        logToTerminal('Pyodide 런타임 다운로드 중...', 'info', 'url');
        pyodideInstance = await loadPyodide();
        logToTerminal('Pyodide 로드 완료. ssl 및 micropip 준비 중...', 'success', 'url');
        
        await pyodideInstance.loadPackage("micropip");
        await pyodideInstance.loadPackage("ssl");
        const micropip = pyodideInstance.pyimport("micropip");
        
        logToTerminal('WASM 내부 yt-dlp 설치 중...', 'warn', 'url');
        await micropip.install('yt-dlp');
        
        logToTerminal('yt-dlp 파이썬 엔진 준비 완료!', 'success', 'url');
        
        pyodideReady = true;
        statusIndicator.className = 'w-2.5 h-2.5 rounded-full bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.5)]';
        statusText.textContent = 'WASM 백업 엔진 준비 완료 (Native Fetch + yt-dlp)';
        
        btnDownloadUrl.disabled = false;
        btnSearchAccount.disabled = false;

    } catch (error) {
        console.error(error);
        logToTerminal(`WASM 초기화 실패: ${error.message}`, 'error', 'url');
        statusIndicator.className = 'w-2.5 h-2.5 rounded-full bg-red-500';
        statusText.textContent = '파이썬 백업 엔진 로드 실패';
        // Still enable buttons for Native JS Fetch
        btnDownloadUrl.disabled = false;
        btnSearchAccount.disabled = false;
    }
}

// Start URL Extraction
async function startUrlDownload() {
    const urlInput = document.getElementById('url-input').value.trim();
    if (!urlInput) {
        logToTerminal('다운로드할 URL을 입력해주세요.', 'error', 'url');
        return;
    }

    logToTerminal(`미디어 추출 시작: ${urlInput}`, 'info', 'url');
    btnDownloadUrl.disabled = true;
    btnDownloadUrl.innerHTML = '<span class="animate-pulse">페이지 분석 중...</span>';

    try {
        let mediaUrl = null;
        let mediaType = 'video';
        let title = 'AMEVA_Media';

        // 1. TikTok URL Extraction
        if (urlInput.includes('tiktok.com')) {
            title = 'TikTok_Video';
            logToTerminal('TikWM API 파서 연결 중...', 'info', 'url');
            try {
                const res = await fetch(`https://www.tikwm.com/api/?url=${encodeURIComponent(urlInput)}`);
                const json = await res.json();
                if (json && json.data && json.data.play) {
                    mediaUrl = json.data.play;
                    title = `TikTok_${json.data.id || Date.now()}`;
                }
            } catch (e) {
                console.log('TikWM GET fallback error:', e);
            }

            if (!mediaUrl) {
                const response = await fetch(urlInput);
                const htmlText = await response.text();
                const playAddrMatch = htmlText.match(/"playAddr":"([^"]+)"/) || htmlText.match(/"downloadAddr":"([^"]+)"/);
                if (playAddrMatch) mediaUrl = playAddrMatch[1].replace(/\\u0026/g, '&').replace(/\\/g, '');
            }
        } 
        // 2. Instagram URL Extraction
        else if (urlInput.includes('instagram.com')) {
            title = 'Instagram_Media';
            try {
                const response = await fetch(urlInput);
                const htmlText = await response.text();
                const ogVideoMatch = htmlText.match(/<meta[^>]+property="og:video"[^>]+content="([^"]+)"/);
                const ogImageMatch = htmlText.match(/<meta[^>]+property="og:image"[^>]+content="([^"]+)"/);
                if (ogVideoMatch) { mediaUrl = ogVideoMatch[1].replace(/&amp;/g, '&'); mediaType = 'video'; }
                else if (ogImageMatch) { mediaUrl = ogImageMatch[1].replace(/&amp;/g, '&'); mediaType = 'photo'; }
            } catch (e) {
                console.log('Instagram direct fetch error:', e);
            }
        }

        // 3. YouTube / General Fallback
        if (!mediaUrl && pyodideReady) {
            logToTerminal('WASM Pyodide 파이썬 백업 파서 실행 중...', 'warn', 'url');
            const pyCode = `
import yt_dlp, json
ydl_opts = {'quiet': True, 'skip_download': True}
url = "${urlInput}"
res = ""
try:
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        res = json.dumps(ydl.extract_info(url, download=False))
except Exception as e:
    res = json.dumps({"error": str(e)})
res
            `;
            const pyResStr = await pyodideInstance.runPythonAsync(pyCode);
            const pyRes = JSON.parse(pyResStr);
            if (pyRes.url) mediaUrl = pyRes.url;
        }

        if (!mediaUrl) {
            throw new Error('페이지에서 미디어 주소를 추출할 수 없습니다. CORS 확장 프로그램이 켜져 있는지 확인해주세요.');
        }

        logToTerminal(`추출 성공! 파일 다운로드 중...`, 'success', 'url');

        // Trigger Blob Download
        const fileRes = await fetch(mediaUrl);
        const fileBlob = await fileRes.blob();
        const blobUrl = URL.createObjectURL(fileBlob);

        const a = document.createElement('a');
        a.href = blobUrl;
        a.download = `${title}_${Date.now()}.${mediaType === 'video' ? 'mp4' : 'jpg'}`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(blobUrl);

        logToTerminal('✅ 다운로드가 완료되었습니다!', 'success', 'url');

    } catch (error) {
        logToTerminal(`추출/다운로드 실패: ${error.message}`, 'error', 'url');
    } finally {
        btnDownloadUrl.disabled = false;
        btnDownloadUrl.innerHTML = '추출 및 다운로드';
    }
}

// Start Account Search (Simple GET Fetch to avoid CORS Preflight)
async function startAccountSearch() {
    const platform = document.getElementById('platform-select').value;
    const accountInput = document.getElementById('account-input').value.replace('@', '').trim();
    
    if (!accountInput) {
        logToTerminal('계정 아이디를 입력해주세요.', 'error', 'account');
        return;
    }

    logToTerminal(`@${accountInput} 계정의 ${platform} 피드 스캔 중...`, 'info', 'account');
    btnSearchAccount.disabled = true;
    btnSearchAccount.innerHTML = '<span class="animate-pulse">스캔 중...</span>';

    try {
        let mediaItems = [];

        if (platform === 'tiktok') {
            logToTerminal('TikWM API 파서로 계정 피드 조회 중...', 'info', 'account');
            
            // Simple GET Request (No OPTIONS preflight)
            const res = await fetch(`https://www.tikwm.com/api/user/posts?unique_id=${encodeURIComponent(accountInput)}&count=12`);
            if (res.ok) {
                const json = await res.json();
                if (json && json.data && json.data.videos) {
                    mediaItems = json.data.videos.map(v => ({
                        id: v.id,
                        type: 'video',
                        thumb: v.cover,
                        url: v.play,
                        title: v.title || `TikTok_${v.id}`
                    }));
                }
            }

            // Fallback: Direct TikTok HTML Scraping
            if (mediaItems.length === 0) {
                logToTerminal('TikTok 웹프로필 백업 직접 스캔 중...', 'info', 'account');
                const tiktokHtmlRes = await fetch(`https://www.tiktok.com/@${encodeURIComponent(accountInput)}`);
                if (tiktokHtmlRes.ok) {
                    const htmlText = await tiktokHtmlRes.text();
                    const jsonMatch = htmlText.match(/<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>([^<]+)<\/script>/) || htmlText.match(/<script id="SIGI_STATE"[^>]*>([^<]+)<\/script>/);
                    if (jsonMatch) {
                        const jsonData = JSON.parse(jsonMatch[1]);
                        const itemList = jsonData?.default?.["user-post"]?.list || jsonData?.ItemModule || {};
                        const videos = Array.isArray(itemList) ? itemList : Object.values(itemList);
                        
                        mediaItems = videos.map(v => ({
                            id: v.id || v.video?.id,
                            type: 'video',
                            thumb: v.video?.cover || v.video?.originCover || 'https://picsum.photos/300/450',
                            url: v.video?.playAddr || v.video?.downloadAddr,
                            title: v.desc || `TikTok_${v.id}`
                        })).filter(item => item.url);
                    }
                }
            }

        } else if (platform === 'instagram') {
            logToTerminal('인스타그램 계정 프로필 조회 중...', 'info', 'account');
            const res = await fetch(`https://www.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(accountInput)}`, {
                headers: { 'X-IG-App-ID': '936619743392459' }
            });
            if (res.ok) {
                const json = await res.json();
                const edges = json.data?.user?.edge_owner_to_timeline_media?.edges || [];
                mediaItems = edges.map(e => {
                    const node = e.node;
                    return {
                        id: node.id,
                        type: node.is_video ? 'video' : 'photo',
                        thumb: node.display_url,
                        url: node.is_video ? node.video_url : node.display_url,
                        title: `Instagram_${node.id}`
                    };
                });
            }
        }

        if (mediaItems.length === 0) {
            throw new Error('해당 계정에서 공개 미디어를 찾지 못했습니다. 계정 아이디를 확인하고 확장 프로그램을 새로고침 해보세요.');
        }

        logToTerminal(`스캔 성공! 총 ${mediaItems.length}개의 미디어를 불러왔습니다.`, 'success', 'account');
        renderRealGallery(mediaItems);

    } catch (error) {
        logToTerminal(`계정 스캔 실패: ${error.message}`, 'error', 'account');
    } finally {
        btnSearchAccount.disabled = false;
        btnSearchAccount.innerHTML = '계정 전체 미디어 스캔';
    }
}

// Render Real Gallery
function renderRealGallery(items) {
    galleryContainer.classList.remove('hidden');
    mediaGrid.innerHTML = '';
    selectedMediaItems.clear();
    currentScrapedItemsMap.clear();
    updateDownloadButtonText();

    items.forEach(item => {
        currentScrapedItemsMap.set(item.id, item);

        const card = document.createElement('div');
        card.className = 'gallery-item relative rounded-xl overflow-hidden bg-slate-900 border border-slate-800 aspect-[9/16] cursor-pointer group';
        card.onclick = (e) => toggleMediaSelection(item.id, card, e);

        card.innerHTML = `
            <img src="${item.thumb}" class="w-full h-full object-cover opacity-80 group-hover:opacity-100 transition-opacity" alt="thumbnail">
            
            <div class="absolute top-2 right-2 z-10">
                <input type="checkbox" class="media-item-checkbox pointer-events-none" id="checkbox-${item.id}">
            </div>
            
            <div class="absolute bottom-0 inset-x-0 bg-gradient-to-t from-slate-950 to-transparent p-3 pt-8">
                <span class="inline-block px-1.5 py-0.5 bg-slate-900/90 rounded text-[10px] font-bold ${item.type === 'video' ? 'text-blue-400' : 'text-pink-400'} mb-1 border border-slate-800">
                    ${item.type.toUpperCase()}
                </span>
                <p class="text-white text-xs font-medium truncate">${item.title}</p>
            </div>
        `;
        mediaGrid.appendChild(card);
    });
}

function toggleMediaSelection(id, cardElement, event) {
    const checkbox = cardElement.querySelector('input[type="checkbox"]');
    
    if (selectedMediaItems.has(id)) {
        selectedMediaItems.delete(id);
        cardElement.classList.remove('selected');
        checkbox.checked = false;
    } else {
        selectedMediaItems.add(id);
        cardElement.classList.add('selected');
        checkbox.checked = true;
    }
    
    updateDownloadButtonText();
}

function updateDownloadButtonText() {
    btnDownloadSelected.textContent = `선택 다운로드 (${selectedMediaItems.size})`;
    if (selectedMediaItems.size > 0) {
        btnDownloadSelected.classList.remove('opacity-50', 'cursor-not-allowed');
    } else {
        btnDownloadSelected.classList.add('opacity-50', 'cursor-not-allowed');
    }
}

async function downloadSelectedMedia() {
    if (selectedMediaItems.size === 0) return;
    
    logToTerminal(`선택한 ${selectedMediaItems.size}개 미디어 일괄 다운로드를 시작합니다...`, 'info', 'account');

    for (const id of selectedMediaItems) {
        const item = currentScrapedItemsMap.get(id);
        if (!item || !item.url) continue;

        try {
            logToTerminal(`다운로드 중: ${item.title}...`, 'info', 'account');
            const fileRes = await fetch(item.url);
            const fileBlob = await fileRes.blob();
            const blobUrl = URL.createObjectURL(fileBlob);

            const a = document.createElement('a');
            a.href = blobUrl;
            a.download = `${item.title}_${Date.now()}.${item.type === 'video' ? 'mp4' : 'jpg'}`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(blobUrl);

            logToTerminal(`✅ 저장 완료: ${item.title}`, 'success', 'account');
        } catch (e) {
            logToTerminal(`❌ 저장 실패 (${item.title}): ${e.message}`, 'error', 'account');
        }
    }

    logToTerminal('🎉 일괄 다운로드 작업이 완료되었습니다!', 'success', 'account');
}

// Start Initialization on Load
window.addEventListener('DOMContentLoaded', () => {
    initWASM();
});
