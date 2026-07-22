// app.js
let pyodideReady = false;
let pyodideInstance = null;

// UI Elements
const logOutput = document.getElementById('log-output');
const statusIndicator = document.getElementById('status-indicator');
const statusText = document.getElementById('status-text');
const btnDownloadUrl = document.getElementById('btn-download-url');
const btnSearchAccount = document.getElementById('btn-search-account');
const galleryContainer = document.getElementById('gallery-container');
const mediaGrid = document.getElementById('media-grid');
const btnDownloadSelected = document.getElementById('btn-download-selected');

let selectedMediaItems = new Set();

// Utility: Logging to UI
function logToTerminal(message, type = 'info') {
    const li = document.createElement('li');
    li.textContent = `[System] ${message}`;
    switch(type) {
        case 'error': li.className = 'text-red-500'; break;
        case 'success': li.className = 'text-green-500'; break;
        case 'warn': li.className = 'text-yellow-500'; break;
        default: li.className = 'text-gray-300';
    }
    logOutput.appendChild(li);
    // Auto-scroll to bottom
    const container = document.getElementById('log-container');
    container.scrollTop = container.scrollHeight;
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
            tabBtn.classList.replace('text-slate-500', 'text-slate-100');
            tabBtn.classList.replace('border-transparent', 'border-slate-100');
            content.classList.remove('hidden');
        } else {
            tabBtn.classList.replace('text-slate-100', 'text-slate-500');
            tabBtn.classList.replace('border-slate-100', 'border-transparent');
            content.classList.add('hidden');
        }
    });
}

// Tab Switching Logic
function switchTab(tabId) {
    const urlTab = document.getElementById('tab-url');
    const accountTab = document.getElementById('tab-account');
    const urlView = document.getElementById('view-url');
    const accountView = document.getElementById('view-account');
    
    // Reset Gallery
    galleryContainer.classList.add('hidden');
    mediaGrid.innerHTML = '';
    selectedMediaItems.clear();
    updateDownloadButtonText();

    if (tabId === 'url') {
        urlTab.classList.replace('text-gray-500', 'text-brand-400');
        urlTab.classList.replace('border-transparent', 'border-brand-500');
        accountTab.classList.replace('text-brand-400', 'text-gray-500');
        accountTab.classList.replace('border-brand-500', 'border-transparent');
        
        urlView.classList.remove('hidden');
        setTimeout(() => urlView.classList.replace('opacity-0', 'opacity-100'), 10);
        
        accountView.classList.add('opacity-0');
        setTimeout(() => accountView.classList.add('hidden'), 500);
    } else {
        accountTab.classList.replace('text-gray-500', 'text-brand-400');
        accountTab.classList.replace('border-transparent', 'border-brand-500');
        urlTab.classList.replace('text-brand-400', 'text-gray-500');
        urlTab.classList.replace('border-brand-500', 'border-transparent');
        
        accountView.classList.remove('hidden');
        setTimeout(() => accountView.classList.replace('opacity-0', 'opacity-100'), 10);
        
        urlView.classList.add('opacity-0');
        setTimeout(() => urlView.classList.add('hidden'), 500);
    }
}

// Initialize Pyodide
async function initWASM() {
    try {
        logToTerminal('Downloading Pyodide Runtime...');
        pyodideInstance = await loadPyodide();
        logToTerminal('Pyodide loaded. Installing micropip...', 'success');
        
        await pyodideInstance.loadPackage("micropip");
        await pyodideInstance.loadPackage("ssl");
        const micropip = pyodideInstance.pyimport("micropip");
        
        logToTerminal('Installing yt-dlp inside WASM (This might take a minute)...', 'warn');
        await micropip.install('yt-dlp');
        
        logToTerminal('yt-dlp installed successfully!', 'success');
        
        // Update UI Status
        pyodideReady = true;
        statusIndicator.classList.replace('bg-yellow-500', 'bg-green-500');
        statusIndicator.classList.remove('animate-pulse');
        statusIndicator.style.boxShadow = '0 0 10px rgba(34,197,94,0.5)';
        statusText.textContent = 'WASM Engine Ready (Pyodide + yt-dlp)';
        
        // Enable Buttons
        btnDownloadUrl.disabled = false;
        btnSearchAccount.disabled = false;

    } catch (error) {
        console.error(error);
        logToTerminal(`WASM Initialization Failed: ${error.message}`, 'error');
        statusIndicator.classList.replace('bg-yellow-500', 'bg-red-500');
        statusIndicator.classList.remove('animate-pulse');
        statusText.textContent = 'Engine Initialization Failed';
    }
}

// Start URL Extraction using Native JS Fetch (Compatible with CORS Extension)
async function startUrlDownload() {
    const urlInput = document.getElementById('url-input').value.trim();
    if (!urlInput) {
        logToTerminal('다운로드할 URL을 입력해주세요.', 'error');
        return;
    }

    logToTerminal(`미디어 추출 시작: ${urlInput}`);
    btnDownloadUrl.disabled = true;
    btnDownloadUrl.innerHTML = '<span class="animate-pulse">페이지 분석 중...</span>';

    try {
        logToTerminal('웹 브라우저 HTTP Fetch 시도 중...', 'info');
        
        let mediaUrl = null;
        let mediaType = 'video';
        let title = 'AMEVA_Media';

        // 1. Try Direct Fetch first
        try {
            const response = await fetch(urlInput);
            if (response.ok) {
                const htmlText = await response.text();
                if (urlInput.includes('tiktok.com')) {
                    const playAddrMatch = htmlText.match(/"playAddr":"([^"]+)"/) || htmlText.match(/"downloadAddr":"([^"]+)"/);
                    if (playAddrMatch) mediaUrl = playAddrMatch[1].replace(/\\u0026/g, '&').replace(/\\/g, '');
                } else if (urlInput.includes('instagram.com')) {
                    const ogVideoMatch = htmlText.match(/<meta[^>]+property="og:video"[^>]+content="([^"]+)"/);
                    const ogImageMatch = htmlText.match(/<meta[^>]+property="og:image"[^>]+content="([^"]+)"/);
                    if (ogVideoMatch) { mediaUrl = ogVideoMatch[1].replace(/&amp;/g, '&'); mediaType = 'video'; }
                    else if (ogImageMatch) { mediaUrl = ogImageMatch[1].replace(/&amp;/g, '&'); mediaType = 'photo'; }
                }
            }
        } catch (e) {
            console.log('Direct HTML fetch skipped/failed, trying API parser fallback...');
        }

        // 2. Fallback: TikWM API for TikTok
        if (!mediaUrl && urlInput.includes('tiktok.com')) {
            logToTerminal('TikWM API 백업 엔진 연결 중...', 'info');
            const tikRes = await fetch(`https://www.tikwm.com/api/?url=${encodeURIComponent(urlInput)}`);
            const tikData = await tikRes.json();
            if (tikData && tikData.data && tikData.data.play) {
                mediaUrl = tikData.data.play;
                title = `TikTok_${tikData.data.id || Date.now()}`;
            }
        }

        // 3. Fallback: DDInstagram / Insta API for Instagram
        if (!mediaUrl && urlInput.includes('instagram.com')) {
            logToTerminal('Instagram API 백업 엔진 연결 중...', 'info');
            const cleanUrl = urlInput.split('?')[0];
            const ddRes = await fetch(`${cleanUrl}?__a=1&__d=dis`);
            if (ddRes.ok) {
                const ddData = await ddRes.json();
                const items = ddData.graphql ? ddData.graphql.shortcode_media : (ddData.items ? ddData.items[0] : null);
                if (items) {
                    mediaUrl = items.video_url || (items.image_versions2 ? items.image_versions2.candidates[0].url : null);
                }
            }
        }

        if (!mediaUrl) {
            throw new Error('미디어 주소를 추출하지 못했습니다. URL이 올바른지 확인해주세요.');
        }

        logToTerminal(`추출 성공! 다운로드를 시작합니다...`, 'success');

        // Trigger Blob Download
        const fileRes = await fetch(mediaUrl);
        const fileBlob = await fileRes.blob();
        const blobUrl = URL.revokeObjectURL ? URL.createObjectURL(fileBlob) : mediaUrl;

        const a = document.createElement('a');
        a.href = blobUrl;
        a.download = `${title}_${Date.now()}.${mediaType === 'video' ? 'mp4' : 'jpg'}`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);

        logToTerminal('✅ 다운로드가 완료되었습니다!', 'success');

    } catch (error) {
        logToTerminal(`추출/다운로드 실패: ${error.message}`, 'error');
        logToTerminal(`💡 팁: 'AMEVA Universal CORS Unblocker' 확장 프로그램이 켜져 있는지 확인하세요!`, 'warn');
    } finally {
        btnDownloadUrl.disabled = false;
        btnDownloadUrl.innerHTML = '추출 및 다운로드';
    }
}

// Start Account Search
let currentScrapedItemsMap = new Map();

async function startAccountSearch() {
    const platform = document.getElementById('platform-select').value;
    const accountInput = document.getElementById('account-input').value.replace('@', '').trim();
    
    if (!accountInput) {
        logToTerminal('계정 아이디를 입력해주세요.', 'error');
        return;
    }

    logToTerminal(`@${accountInput} 계정의 ${platform} 피드 스캔 중...`);
    btnSearchAccount.disabled = true;
    btnSearchAccount.innerHTML = '<span class="animate-pulse">스캔 중...</span>';

    try {
        let mediaItems = [];

        if (platform === 'tiktok') {
            logToTerminal('TikWM API로 틱톡 계정 미디어 목록 조회 중...', 'info');
            const res = await fetch(`https://www.tikwm.com/api/user/posts?unique_id=${encodeURIComponent(accountInput)}&count=12`);
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
        } else if (platform === 'instagram') {
            logToTerminal('인스타그램 계정 피드 조회 중...', 'info');
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
            throw new Error('해당 계정에서 공개 미디어를 찾지 못했습니다. 계정 아이디를 확인해주세요.');
        }

        logToTerminal(`스캔 성공! 총 ${mediaItems.length}개의 실제 미디어를 불러왔습니다.`, 'success');
        renderRealGallery(mediaItems);

    } catch (error) {
        logToTerminal(`계정 스캔 실패: ${error.message}`, 'error');
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
    
    logToTerminal(`선택한 ${selectedMediaItems.size}개 미디어 일괄 다운로드를 시작합니다...`);

    for (const id of selectedMediaItems) {
        const item = currentScrapedItemsMap.get(id);
        if (!item || !item.url) continue;

        try {
            logToTerminal(`다운로드 중: ${item.title}...`, 'info');
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

            logToTerminal(`✅ 저장 완료: ${item.title}`, 'success');
        } catch (e) {
            logToTerminal(`❌ 저장 실패 (${item.title}): ${e.message}`, 'error');
        }
    }

    logToTerminal('🎉 일괄 다운로드 작업이 완료되었습니다!', 'success');
}

// Start Initialization on Load
window.addEventListener('DOMContentLoaded', () => {
    initWASM();
});
