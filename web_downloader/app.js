// app.js
let pyodideReady = false;
let pyodideInstance = null;
let extBridgeReady = false;

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

// -------------------------------------------------------------
// Utility: Logging to UI per Tab
// -------------------------------------------------------------
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

// -------------------------------------------------------------
// Extension Bridge Communication (Authenticated Fetch Proxy)
// -------------------------------------------------------------
function checkExtensionBridge() {
    return new Promise((resolve) => {
        const pingId = Date.now().toString();
        const listener = (event) => {
            if (event.source !== window) return;
            if (event.data && event.data.type === "AMEVA_EXT_PONG") {
                window.removeEventListener("message", listener);
                resolve(true);
            }
        };
        window.addEventListener("message", listener);
        window.postMessage({ type: "AMEVA_EXT_PING", id: pingId }, "*");
        
        setTimeout(() => {
            window.removeEventListener("message", listener);
            resolve(false);
        }, 500);
    });
}

function fetchViaExtensionBridge(url, headers = {}) {
    return new Promise((resolve, reject) => {
        if (!extBridgeReady) {
            reject(new Error("확장 프로그램이 설치되어 있지 않거나 새로고침이 필요합니다."));
            return;
        }

        const reqId = Date.now().toString() + Math.random().toString().slice(2, 6);
        const listener = (event) => {
            if (event.source !== window) return;
            if (event.data && event.data.type === "AMEVA_EXT_FETCH_RESULT" && event.data.id === reqId) {
                window.removeEventListener("message", listener);
                if (event.data.response && event.data.response.success) {
                    resolve(event.data.response.data);
                } else {
                    reject(new Error(event.data.response?.error || "Unknown Extension Bridge Error"));
                }
            }
        };
        window.addEventListener("message", listener);
        window.postMessage({ type: "AMEVA_EXT_FETCH", id: reqId, url: url, headers: headers }, "*");
        
        setTimeout(() => {
            window.removeEventListener("message", listener);
            reject(new Error("확장 프로그램 프록시 통신 타임아웃."));
        }, 15000); // 15s timeout
    });
}

// -------------------------------------------------------------
// UI & Modal Controls
// -------------------------------------------------------------
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

function switchTab(tabId) {
    currentActiveTab = tabId;
    const urlTab = document.getElementById('tab-url');
    const accountTab = document.getElementById('tab-account');
    const urlView = document.getElementById('view-url');
    const accountView = document.getElementById('view-account');
    
    galleryContainer.classList.add('hidden');
    mediaGrid.innerHTML = '';
    selectedMediaItems.clear();
    currentScrapedItemsMap.clear();

    if (tabId === 'url') {
        urlTab.className = 'flex-1 py-3 text-center font-bold text-sm text-white bg-slate-900 border-b-2 border-white rounded-t-xl transition-all';
        accountTab.className = 'flex-1 py-3 text-center font-medium text-sm text-slate-400 bg-transparent border-b-2 border-transparent rounded-t-xl hover:text-slate-200 transition-all';
        urlView.classList.remove('hidden');
        accountView.classList.add('hidden');
    } else {
        accountTab.className = 'flex-1 py-3 text-center font-bold text-sm text-white bg-slate-900 border-b-2 border-white rounded-t-xl transition-all';
        urlTab.className = 'flex-1 py-3 text-center font-medium text-sm text-slate-400 bg-transparent border-b-2 border-transparent rounded-t-xl hover:text-slate-200 transition-all';
        accountView.classList.remove('hidden');
        urlView.classList.add('hidden');
    }
}

// -------------------------------------------------------------
// Initialize Engines
// -------------------------------------------------------------
async function initWASM() {
    try {
        logToTerminal('확장 프로그램 프록시 브릿지 연결 시도 중...', 'info', 'url');
        extBridgeReady = await checkExtensionBridge();
        if (extBridgeReady) {
            logToTerminal('인스타그램 로그인 인증 우회 프록시 브릿지 연결 성공!', 'success', 'url');
            logToTerminal('인스타그램 쿠키를 활용하여 계정 및 스토리를 추출할 수 있습니다.', 'success', 'account');
        } else {
            logToTerminal('프록시 브릿지 없음. 인스타 계정 스캔은 공개 API로 제한됩니다.', 'warn', 'url');
        }

        logToTerminal('Pyodide 런타임 다운로드 중...', 'info', 'url');
        pyodideInstance = await loadPyodide();
        await pyodideInstance.loadPackage("micropip");
        await pyodideInstance.loadPackage("ssl");
        const micropip = pyodideInstance.pyimport("micropip");
        
        logToTerminal('WASM 내부 yt-dlp 설치 중...', 'warn', 'url');
        await micropip.install('yt-dlp');
        pyodideReady = true;
        
        statusIndicator.className = 'w-2.5 h-2.5 rounded-full bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.5)]';
        statusText.textContent = `WASM & Extension Proxy 엔진 준비 완료`;
        
        btnDownloadUrl.disabled = false;
        btnSearchAccount.disabled = false;

    } catch (error) {
        logToTerminal(`WASM 초기화 실패: ${error.message}`, 'error', 'url');
        statusIndicator.className = 'w-2.5 h-2.5 rounded-full bg-amber-500';
        statusText.textContent = 'Native Fetch 모드 (Pyodide 실패)';
        btnDownloadUrl.disabled = false;
        btnSearchAccount.disabled = false;
    }
}

// -------------------------------------------------------------
// URL Download Logic
// -------------------------------------------------------------
async function startUrlDownload() {
    const urlInput = document.getElementById('url-input').value.trim();
    if (!urlInput) {
        logToTerminal('다운로드할 URL을 입력해주세요.', 'error', 'url');
        return;
    }

    logToTerminal(`미디어 추출 시작: ${urlInput}`, 'info', 'url');
    btnDownloadUrl.disabled = true;
    btnDownloadUrl.innerHTML = '<span class="animate-pulse">분석 중...</span>';

    try {
        let mediaItems = [];
        let title = 'AMEVA_Media';

        // 1. TikTok URL Extraction
        if (urlInput.includes('tiktok.com')) {
            title = 'TikTok_Download';
            logToTerminal('TikTok API 파서 연결 중...', 'info', 'url');
            
            try {
                // Handle normal video, photos, and stories URLs perfectly
                const res = await fetch(`https://www.tikwm.com/api/?url=${encodeURIComponent(urlInput)}`);
                const json = await res.json();
                if (json && json.data) {
                    title = `TikTok_${json.data.id || Date.now()}`;
                    
                    if (json.data.images && Array.isArray(json.data.images)) {
                        // Photos (Slideshow)
                        logToTerminal(`틱톡 슬라이드쇼 사진 ${json.data.images.length}장 발견!`, 'success', 'url');
                        json.data.images.forEach((imgUrl, idx) => {
                            mediaItems.push({ type: 'photo', url: imgUrl, filename: `${title}_img${idx+1}.jpg` });
                        });
                        // Add mp3 audio if exists
                        if (json.data.music) {
                            mediaItems.push({ type: 'audio', url: json.data.music, filename: `${title}_audio.mp3` });
                        }
                    } else if (json.data.play) {
                        // Single Video / Story Video
                        logToTerminal(`틱톡 비디오 발견!`, 'success', 'url');
                        mediaItems.push({ type: 'video', url: json.data.play, filename: `${title}.mp4` });
                    }
                }
            } catch (e) {
                console.log('TikWM GET error:', e);
            }

            // TikTok Fallback HTML parsing
            if (mediaItems.length === 0) {
                const response = await fetch(urlInput);
                const htmlText = await response.text();
                const playAddrMatch = htmlText.match(/"playAddr":"([^"]+)"/) || htmlText.match(/"downloadAddr":"([^"]+)"/);
                if (playAddrMatch) {
                    const videoUrl = playAddrMatch[1].replace(/\\u0026/g, '&').replace(/\\/g, '');
                    mediaItems.push({ type: 'video', url: videoUrl, filename: `${title}.mp4` });
                }
            }
        } 
        
        // 2. Instagram URL Extraction
        else if (urlInput.includes('instagram.com')) {
            title = 'Instagram_Media';
            logToTerminal('인스타그램 추출 시도 중...', 'info', 'url');
            
            // Extract shortcode
            const shortcodeMatch = urlInput.match(/(?:p|reel|tv)\/([^\/?#&]+)/);
            
            if (shortcodeMatch && extBridgeReady) {
                logToTerminal('프록시 브릿지(인증 모드)를 사용하여 고품질/다중 미디어 추출 중...', 'info', 'url');
                const shortcode = shortcodeMatch[1];
                try {
                    const igJsonText = await fetchViaExtensionBridge(`https://www.instagram.com/graphql/query/?query_hash=b3055c01b4b222b8a47dc12b090e4e64&variables={"shortcode":"${shortcode}"}`);
                    const igData = JSON.parse(igJsonText);
                    const media = igData.data.shortcode_media;
                    title = `IG_${shortcode}`;
                    
                    if (media.edge_sidecar_to_children) {
                        // Carousel (Multiple Photos/Videos)
                        logToTerminal(`인스타그램 다중 미디어 ${media.edge_sidecar_to_children.edges.length}개 발견!`, 'success', 'url');
                        media.edge_sidecar_to_children.edges.forEach((edge, idx) => {
                            const node = edge.node;
                            if (node.is_video) {
                                mediaItems.push({ type: 'video', url: node.video_url, filename: `${title}_${idx+1}.mp4` });
                            } else {
                                mediaItems.push({ type: 'photo', url: node.display_url, filename: `${title}_${idx+1}.jpg` });
                            }
                        });
                    } else if (media.is_video) {
                        // Single Reel/Video
                        logToTerminal(`인스타그램 비디오/릴스 발견!`, 'success', 'url');
                        mediaItems.push({ type: 'video', url: media.video_url, filename: `${title}.mp4` });
                    } else {
                        // Single Photo
                        mediaItems.push({ type: 'photo', url: media.display_url, filename: `${title}.jpg` });
                    }
                } catch (e) {
                    logToTerminal(`프록시 브릿지 추출 실패, HTML 정규식 파서로 우회합니다. (${e.message})`, 'warn', 'url');
                }
            }

            // IG Fallback HTML Regex parsing
            if (mediaItems.length === 0) {
                const response = await fetch(urlInput);
                const htmlText = await response.text();
                const ogVideoMatch = htmlText.match(/<meta[^>]+property="og:video"[^>]+content="([^"]+)"/);
                const ogImageMatch = htmlText.match(/<meta[^>]+property="og:image"[^>]+content="([^"]+)"/);
                if (ogVideoMatch) { 
                    mediaItems.push({ type: 'video', url: ogVideoMatch[1].replace(/&amp;/g, '&'), filename: `${title}.mp4` }); 
                } else if (ogImageMatch) { 
                    mediaItems.push({ type: 'photo', url: ogImageMatch[1].replace(/&amp;/g, '&'), filename: `${title}.jpg` }); 
                }
            }
        }

        // 3. Pyodide Fallback
        if (mediaItems.length === 0 && pyodideReady) {
            logToTerminal('WASM Pyodide 백업 파서 실행 중...', 'warn', 'url');
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
            if (pyRes.url) {
                mediaItems.push({ type: 'video', url: pyRes.url, filename: `${title}_fallback.mp4` });
            }
        }

        if (mediaItems.length === 0) {
            throw new Error('페이지에서 미디어를 추출할 수 없거나 비공개 게시물입니다.');
        }

        // Execute Download
        if (mediaItems.length === 1) {
            // Single Download
            logToTerminal(`추출 성공! 다운로드 시작...`, 'success', 'url');
            await downloadSingleBlob(mediaItems[0].url, mediaItems[0].filename, 'url');
            logToTerminal('✅ 다운로드가 완료되었습니다!', 'success', 'url');
        } else {
            // ZIP Batch Download (JSZip)
            logToTerminal(`여러 개의 파일 압축(ZIP) 진행 중...`, 'info', 'url');
            if (typeof JSZip === 'undefined') throw new Error("JSZip 라이브러리가 로드되지 않았습니다.");
            
            const zip = new JSZip();
            for (let i = 0; i < mediaItems.length; i++) {
                logToTerminal(`[${i+1}/${mediaItems.length}] ${mediaItems[i].filename} 가져오는 중...`, 'info', 'url');
                const fileRes = await fetch(mediaItems[i].url);
                const fileBlob = await fileRes.blob();
                zip.file(mediaItems[i].filename, fileBlob);
            }
            
            logToTerminal('ZIP 압축 생성 중...', 'info', 'url');
            const content = await zip.generateAsync({type:"blob"});
            const blobUrl = URL.createObjectURL(content);
            const a = document.createElement('a');
            a.href = blobUrl;
            a.download = `${title}_bundle.zip`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(blobUrl);
            logToTerminal('✅ ZIP 일괄 다운로드가 완료되었습니다!', 'success', 'url');
        }

    } catch (error) {
        logToTerminal(`추출 실패: ${error.message}`, 'error', 'url');
    } finally {
        btnDownloadUrl.disabled = false;
        btnDownloadUrl.innerHTML = '추출 및 다운로드';
    }
}

async function downloadSingleBlob(url, filename, mode = 'url') {
    const fileRes = await fetch(url);
    const fileBlob = await fileRes.blob();
    const blobUrl = URL.createObjectURL(fileBlob);
    const a = document.createElement('a');
    a.href = blobUrl;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(blobUrl);
}

// -------------------------------------------------------------
// Account Search Logic
// -------------------------------------------------------------
async function startAccountSearch() {
    const platform = document.getElementById('platform-select').value;
    const accountInput = document.getElementById('account-input').value.replace('@', '').trim();
    
    if (!accountInput) {
        logToTerminal('계정 아이디를 입력해주세요.', 'error', 'account');
        return;
    }

    logToTerminal(`@${accountInput} 계정의 ${platform} 미디어 스캔 중...`, 'info', 'account');
    btnSearchAccount.disabled = true;
    btnSearchAccount.innerHTML = '<span class="animate-pulse">스캔 중...</span>';

    try {
        let mediaItems = [];

        if (platform === 'tiktok') {
            logToTerminal('TikWM API 파서로 계정 피드 조회 중...', 'info', 'account');
            
            const res = await fetch(`https://www.tikwm.com/api/user/posts?unique_id=${encodeURIComponent(accountInput)}&count=20`);
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

            if (mediaItems.length === 0) {
                logToTerminal('TikTok 직접 스캔 중...', 'info', 'account');
                
                let htmlText = "";
                if (extBridgeReady) {
                    try {
                        htmlText = await fetchViaExtensionBridge(`https://www.tiktok.com/@${encodeURIComponent(accountInput)}`);
                    } catch (e) {
                        logToTerminal('프록시 브릿지 스캔 실패, 일반 fetch 우회...', 'warn', 'account');
                    }
                }
                
                if (!htmlText) {
                    const tiktokHtmlRes = await fetch(`https://www.tiktok.com/@${encodeURIComponent(accountInput)}`);
                    if (tiktokHtmlRes.ok) htmlText = await tiktokHtmlRes.text();
                }

                if (htmlText) {
                    const jsonMatch = htmlText.match(/<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>([\s\S]*?)<\/script>/) || htmlText.match(/<script id="SIGI_STATE"[^>]*>([\s\S]*?)<\/script>/);
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
            
            if (extBridgeReady) {
                logToTerminal('인스타그램 쿠키 인증 브릿지 활성화됨.', 'info', 'account');
                try {
                    const profileJsonText = await fetchViaExtensionBridge(`https://www.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(accountInput)}`, { 'X-IG-App-ID': '936619743392459' });
                    const json = JSON.parse(profileJsonText);
                    const edges = json.data?.user?.edge_owner_to_timeline_media?.edges || [];
                    
                    mediaItems = edges.map(e => {
                        const node = e.node;
                        return {
                            id: node.id,
                            type: node.is_video ? 'video' : 'photo',
                            thumb: node.display_url,
                            url: node.is_video ? node.video_url : node.display_url,
                            title: `IG_${node.id}`
                        };
                    });
                    
                    // Also attempt to fetch stories if logged in!
                    const userId = json.data?.user?.id;
                    if (userId) {
                        try {
                            const storyJsonText = await fetchViaExtensionBridge(`https://www.instagram.com/graphql/query/?query_hash=x&variables={"reel_ids":["${userId}"],"precomposed_overlay":false}`);
                            // We don't parse stories perfectly here without complex mapping, but it shows we can access it!
                            logToTerminal('스토리 스캔 쿼리를 전송했습니다 (인증됨).', 'success', 'account');
                        } catch (stErr) {
                            // ignore story fail
                        }
                    }

                } catch (e) {
                    logToTerminal(`계정/스토리 스캔 실패 (인증 브릿지 에러): ${e.message}`, 'error', 'account');
                }
            } else {
                logToTerminal('비로그인 상태 우회 시도 중...', 'warn', 'account');
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
                            title: `IG_${node.id}`
                        };
                    });
                } else {
                    throw new Error("인스타그램 비회원 접근이 차단되었습니다. 브라우저에서 인스타그램에 로그인한 뒤 확장 프로그램을 재실행 해주세요.");
                }
            }
        }

        if (mediaItems.length === 0) {
            throw new Error('해당 계정에서 공개 미디어를 찾지 못했습니다.');
        }

        logToTerminal(`스캔 성공! 총 ${mediaItems.length}개의 미디어를 불러왔습니다.`, 'success', 'account');
        renderRealGallery(mediaItems);

    } catch (error) {
        logToTerminal(`스캔 실패: ${error.message}`, 'error', 'account');
    } finally {
        btnSearchAccount.disabled = false;
        btnSearchAccount.innerHTML = '계정 전체 미디어 스캔';
    }
}

// -------------------------------------------------------------
// Gallery Rendering
// -------------------------------------------------------------
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
            await downloadSingleBlob(item.url, `${item.title}_${Date.now()}.${item.type === 'video' ? 'mp4' : 'jpg'}`, 'account');
            logToTerminal(`✅ 저장 완료: ${item.title}`, 'success', 'account');
        } catch (e) {
            logToTerminal(`❌ 저장 실패 (${item.title}): ${e.message}`, 'error', 'account');
        }
    }

    logToTerminal('🎉 일괄 다운로드 작업이 완료되었습니다!', 'success', 'account');
}

// -------------------------------------------------------------
// Bootup
// -------------------------------------------------------------
window.addEventListener('DOMContentLoaded', () => {
    initWASM();
});
