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
                resolve(event.data.version || true);
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
    
    // 1. View 토글
    document.getElementById('view-url').classList.add('hidden');
    document.getElementById('view-account').classList.add('hidden');
    document.getElementById(`view-${tabId}`).classList.remove('hidden');

    // 2. Tab 버튼 스타일링 (모두 비활성화 후 선택된 탭만 활성화)
    const urlTab = document.getElementById('tab-url');
    const accountTab = document.getElementById('tab-account');
    
    const inactiveClasses = ['font-medium', 'text-slate-400', 'bg-transparent', 'border-transparent', 'hover:text-slate-200'];
    const activeClasses = ['font-bold', 'text-white', 'bg-slate-900', 'border-white'];

    // 먼저 둘 다 비활성화 상태로 초기화
    urlTab.classList.remove(...activeClasses);
    urlTab.classList.add(...inactiveClasses);
    accountTab.classList.remove(...activeClasses);
    accountTab.classList.add(...inactiveClasses);

    // 선택된 탭만 활성화 상태로 변경
    const activeElem = document.getElementById(`tab-${tabId}`);
    activeElem.classList.remove(...inactiveClasses);
    activeElem.classList.add(...activeClasses);

    // 3. 상태 정리
    galleryContainer.classList.add('hidden');
    mediaGrid.innerHTML = '';
    selectedMediaItems.clear();
    currentScrapedItemsMap.clear();

    currentMode = tabId;
    localStorage.setItem('ameva_last_tab', tabId);
}

// -------------------------------------------------------------
// Initialize Engines
// -------------------------------------------------------------
async function initWASM() {
    try {
        logToTerminal('확장 프로그램 프록시 브릿지 연결 시도 중...', 'info', 'url');
        extBridgeReady = await checkExtensionBridge();
        if (extBridgeReady) {
            const v = typeof extBridgeReady === 'string' ? `v${extBridgeReady}` : '';
            logToTerminal(`인스타그램 로그인 인증 우회 프록시 브릿지 연결 성공! ${v}`, 'success', 'url');
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
            saveToHistory(`${title}_bundle.zip`, 'ZIP Archive');
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
    saveToHistory(filename, url, mode !== 'url' ? mode : null); // Here 'mode' is abused to pass mediaId in downloadSelectedMedia
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
                    console.log("[TikTok HTML Length]", htmlText.length);
                    const hasUniversal = htmlText.includes('__UNIVERSAL_DATA_FOR_REHYDRATION__');
                    const hasSigi = htmlText.includes('SIGI_STATE');
                    logToTerminal(`TikTok HTML 분석... (UNIVERSAL:${hasUniversal}, SIGI:${hasSigi})`, 'info', 'account');

                    const jsonMatch = htmlText.match(/<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>([\s\S]*?)<\/script>/) || htmlText.match(/<script id="SIGI_STATE"[^>]*>([\s\S]*?)<\/script>/);
                    
                    if (jsonMatch) {
                        try {
                            const jsonData = JSON.parse(jsonMatch[1]);
                            const itemList = jsonData?.default?.["user-post"]?.list || jsonData?.ItemModule || jsonData?.__DEFAULT_SCOPE__?.["webapp.user-detail"]?.itemList || {};
                            const videos = Array.isArray(itemList) ? itemList : Object.values(itemList);
                            logToTerminal(`JSON 파싱 성공. 비디오 배열 크기: ${videos.length}`, 'info', 'account');
                            
                            mediaItems = videos.map(v => ({
                                id: v.id || v.video?.id,
                                type: 'video',
                                thumb: v.video?.cover || v.video?.originCover || 'https://picsum.photos/300/450',
                                url: v.video?.playAddr || v.video?.downloadAddr,
                                title: v.desc || `TikTok_${v.id}`
                            })).filter(item => item.url);
                        } catch (parseErr) {
                            logToTerminal(`JSON 파싱 에러: ${parseErr.message}`, 'error', 'account');
                        }
                    } else {
                        // logToTerminal('정규식 추출 실패 (스크립트 태그를 찾지 못함)', 'error', 'account');
                    }
                } else {
                    // logToTerminal('TikTok HTML을 불러오지 못했습니다.', 'error', 'account');
                }
            }

            // --- UrleBird Fallback ---
            if (mediaItems.length === 0) {
                logToTerminal('TikTok 웹 스크래핑 차단됨. 퍼블릭 아카이브(UrleBird) 우회 탐색 시작...', 'info', 'account');
                try {
                    const urlebirdUrl = `https://urlebird.com/user/${encodeURIComponent(accountInput)}/`;
                    let ubHtml = '';
                    if (extBridgeReady) {
                        ubHtml = await fetchViaExtensionBridge(urlebirdUrl);
                    } else {
                        const ubRes = await fetch(urlebirdUrl);
                        if (ubRes.ok) ubHtml = await ubRes.text();
                    }
                    
                    if (ubHtml) {
                        const jsonRegex = /<script type="application\/ld\+json">([\s\S]*?)<\/script>/g;
                        let match;
                        while ((match = jsonRegex.exec(ubHtml)) !== null) {
                            try {
                                const data = JSON.parse(match[1]);
                                if (data['@type'] === 'ItemList' && Array.isArray(data.itemListElement)) {
                                    const itemsMap = new Map();
                                    for (const item of data.itemListElement) {
                                        if (!item.url) continue;
                                        const idMatch = item.url.match(/(\d+)\/?$/);
                                        if (idMatch && item.thumbnailUrl && item.thumbnailUrl.length > 0) {
                                            itemsMap.set(idMatch[1], item.thumbnailUrl[0]);
                                        }
                                    }
                                    
                                    if (itemsMap.size > 0) {
                                        mediaItems = Array.from(itemsMap.entries()).map(([id, thumb]) => ({
                                            id: id,
                                            type: 'video',
                                            thumb: thumb, // Use the proxy thumbnail from JSON-LD
                                            url: `https://www.tiktok.com/@${accountInput}/video/${id}`,
                                            title: `TikTok_${id}`
                                        }));
                                        logToTerminal(`퍼블릭 아카이브(UrleBird)에서 ${itemsMap.size}개의 영상을 찾았습니다!`, 'success', 'account');
                                        break; // Successfully parsed the video list
                                    }
                                }
                            } catch (e) {
                                // Ignore parse errors of other JSON-LD tags
                            }
                        }
                        
                        if (mediaItems.length === 0) {
                            logToTerminal('퍼블릭 아카이브(UrleBird)에서 영상을 찾지 못했습니다.', 'warn', 'account');
                        }
                    }
                } catch (ubErr) {
                    logToTerminal(`UrleBird 우회 탐색 실패: ${ubErr.message}`, 'error', 'account');
                }
            }

        } else if (platform === 'instagram') {
            logToTerminal('인스타그램 계정 프로필 조회 중...', 'info', 'account');
            
            if (extBridgeReady) {
                logToTerminal('인스타그램 쿠키 인증 브릿지 활성화됨.', 'info', 'account');
                try {
                    let profileJsonText = '';
                    let edges = [];
                    let user = null;

                    try {
                        try {
                            profileJsonText = await fetchViaExtensionBridge(`https://www.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(accountInput)}`, { 
                                'X-IG-App-ID': '936619743392459',
                                'X-Requested-With': 'XMLHttpRequest'
                            });
                            const json = JSON.parse(profileJsonText);
                            user = json.data?.user || json.graphql?.user;
                            edges = user?.edge_owner_to_timeline_media?.edges || [];
                        } catch (apiErr) {
                            logToTerminal(`기본 API 차단됨, HTML 분석 모드(Fallback)로 진입합니다...`, 'warn', 'account');
                            const html = await fetchViaExtensionBridge(`https://www.instagram.com/${encodeURIComponent(accountInput)}/`);
                            
                            // Extract JSON from HTML
                            const edgeMatch = html.match(/"edge_owner_to_timeline_media":\{"count":\d+,"page_info":\{.*?\},"edges":(\[.*?\])\}/);
                            if (edgeMatch) {
                                edges = JSON.parse(edgeMatch[1]);
                                const idMatch = html.match(/"profile_id":"(\d+)"/) || html.match(/"id":"(\d+)"/);
                                user = { id: idMatch ? idMatch[1] : null };
                                logToTerminal(`HTML 파싱 성공: ${edges.length}개의 미디어 감지.`, 'success', 'account');
                            } else {
                                // Extract user_id to use GraphQL as last resort
                                const idMatch = html.match(/"profile_id":"(\d+)"/) || html.match(/"user_id":"(\d+)"/);
                                if (idMatch) {
                                    logToTerminal(`사용자 ID(${idMatch[1]}) 획득, GraphQL API 우회 시도...`, 'info', 'account');
                                    const gqlText = await fetchViaExtensionBridge(`https://www.instagram.com/graphql/query/?query_hash=69cba40317214236af40e7efa697781d&variables={"id":"${idMatch[1]}","first":12}`);
                                    const gqlJson = JSON.parse(gqlText);
                                    user = gqlJson.data?.user;
                                    edges = user?.edge_owner_to_timeline_media?.edges || [];
                                } else {
                                    throw new Error("프로필 데이터 및 ID를 HTML에서 찾을 수 없습니다.");
                                }
                            }
                        }
                    } catch (parseErr) {
                        logToTerminal(`모든 내부 파싱 완전 차단됨(${parseErr.message.substring(0, 30)}). 궁극의 탭 스크래퍼(Tab Scraper)를 가동합니다...`, 'warn', 'account');
                        const response = await new Promise((resolve, reject) => {
                            const reqId = Date.now().toString();
                            window.postMessage({ type: 'AMEVA_EXT_IG_SCRAPE', id: reqId, username: accountInput }, '*');
                            
                            const listener = (event) => {
                                if (event.source !== window || !event.data || event.data.type !== 'AMEVA_EXT_IG_SCRAPE_RESULT') return;
                                if (event.data.id === reqId) {
                                    window.removeEventListener('message', listener);
                                    resolve(event.data.response);
                                }
                            };
                            window.addEventListener('message', listener);
                            
                            setTimeout(() => {
                                window.removeEventListener('message', listener);
                                reject(new Error("탭 스크래핑 시간 초과 (인스타그램 로그인이 필요할 수 있습니다)."));
                            }, 20000);
                        });

                        if (response && response.success && response.edges) {
                            edges = response.edges;
                            logToTerminal(`궁극의 탭 스크래핑 성공: ${edges.length}개의 미디어 감지!`, 'success', 'account');
                        } else {
                            throw new Error(response?.error || "탭 스크래핑에서도 데이터를 찾지 못했습니다. 새 창에서 인스타그램 로그인을 확인해주세요.");
                        }
                    }

                    if (!edges || edges.length === 0) {
                        throw new Error("가져올 수 있는 미디어가 없거나 비공개 계정입니다.");
                    }
                    
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
                    const userId = user?.id;
                    if (userId) {
                        try {
                            await fetchViaExtensionBridge(`https://www.instagram.com/graphql/query/?query_hash=x&variables={"reel_ids":["${userId}"],"precomposed_overlay":false}`);
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

    const history = JSON.parse(localStorage.getItem('ameva_download_history') || '[]');
    const downloadedIds = new Set(history.map(item => item.mediaId).filter(id => id));

    items.forEach(item => {
        currentScrapedItemsMap.set(item.id, item);
        const isDownloaded = downloadedIds.has(item.id);

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
                ${isDownloaded ? '<span class="inline-block ml-1 px-1.5 py-0.5 bg-emerald-500/90 rounded text-[10px] font-bold text-slate-950 mb-1 border border-emerald-400">✓ 다운로드됨</span>' : ''}
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

function selectAllMedia() {
    const isAllSelected = selectedMediaItems.size > 0 && selectedMediaItems.size === currentScrapedItemsMap.size;
    
    document.querySelectorAll('.gallery-item').forEach(card => {
        const checkbox = card.querySelector('input[type="checkbox"]');
        const id = checkbox.id.replace('checkbox-', '');
        
        if (isAllSelected) {
            // Deselect all
            selectedMediaItems.delete(id);
            card.classList.remove('selected');
            checkbox.checked = false;
        } else {
            // Select all
            selectedMediaItems.add(id);
            card.classList.add('selected');
            checkbox.checked = true;
        }
    });
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
    
    const history = JSON.parse(localStorage.getItem('ameva_download_history') || '[]');
    const downloadedIds = new Set(history.map(item => item.mediaId).filter(id => id));
    
    const duplicateItems = [];
    for (const id of selectedMediaItems) {
        if (downloadedIds.has(id)) {
            const item = currentScrapedItemsMap.get(id);
            if (item) duplicateItems.push(item);
        }
    }

    if (duplicateItems.length > 0) {
        // Show Redownload Modal
        return new Promise((resolve) => {
            const modal = document.getElementById('redownload-modal');
            const list = document.getElementById('redownload-list');
            const confirmBtn = document.getElementById('btn-confirm-redownload');
            
            list.innerHTML = duplicateItems.map(item => `<li>• ${item.title}</li>`).join('');
            modal.classList.remove('hidden');
            
            confirmBtn.onclick = async () => {
                modal.classList.add('hidden');
                await executeBatchDownload();
                resolve();
            };
            
            window.closeRedownloadModal = () => {
                modal.classList.add('hidden');
                logToTerminal('다운로드가 취소되었습니다.', 'warn', 'account');
                resolve();
            };
        });
    } else {
        await executeBatchDownload();
    }
}

async function executeBatchDownload() {
    logToTerminal(`선택한 ${selectedMediaItems.size}개 미디어 일괄 다운로드를 시작합니다...`, 'info', 'account');

    for (const id of selectedMediaItems) {
        const item = currentScrapedItemsMap.get(id);
        if (!item || !item.url) continue;

        try {
            logToTerminal(`다운로드 중: ${item.title}...`, 'info', 'account');
            let downloadUrl = item.url;
            
            // TikTok의 원본 페이지 URL인 경우 (UrleBird 폴백 등에서 획득) TikWM으로 실제 MP4 URL 변환
            if (downloadUrl.includes('tiktok.com') && !downloadUrl.includes('.mp4') && !downloadUrl.includes('tikwm')) {
                logToTerminal(`원본 URL 변환 중...`, 'info', 'account');
                const tikwmRes = await fetch(`https://www.tikwm.com/api/?url=${encodeURIComponent(downloadUrl)}`);
                const tikwmJson = await tikwmRes.json();
                if (tikwmJson.data && tikwmJson.data.play) {
                    downloadUrl = tikwmJson.data.play.startsWith('http') 
                        ? tikwmJson.data.play 
                        : "https://www.tikwm.com" + tikwmJson.data.play;
                } else {
                    throw new Error('원본 비디오 링크를 추출하지 못했습니다.');
                }
            }
            
            await downloadSingleBlob(downloadUrl, `${item.title}_${Date.now()}.${item.type === 'video' ? 'mp4' : 'jpg'}`, item.id);
            logToTerminal(`✅ 저장 완료: ${item.title}`, 'success', 'account');
        } catch (e) {
            logToTerminal(`❌ 저장 실패 (${item.title}): ${e.message}`, 'error', 'account');
            if (e.message.includes('Failed to fetch') && !extBridgeReady) {
                logToTerminal('확장프로그램이 설치되지 않아 CORS 차단이 발생했을 수 있습니다.', 'warn', 'account');
                document.getElementById('ext-modal').classList.remove('hidden');
            }
        }
    }

    logToTerminal('🎉 일괄 다운로드 작업이 완료되었습니다!', 'success', 'account');
}

// -------------------------------------------------------------
// Utilities & Bootup
// -------------------------------------------------------------
function copyToClipboard(text) {
    navigator.clipboard.writeText(text).then(() => {
        alert('주소가 클립보드에 복사되었습니다!\\n주소창에 붙여넣기 해주세요.');
    }).catch(err => {
        console.error('복사 실패:', err);
    });
}

function closeRedownloadModal() {
    document.getElementById('redownload-modal').classList.add('hidden');
}

window.addEventListener('DOMContentLoaded', () => {
    initWASM();
    
    // Restore state from localStorage
    const lastTab = localStorage.getItem('ameva_last_tab') || 'url';
    const lastPlatform = localStorage.getItem('ameva_last_platform') || 'tiktok';
    
    switchTab(lastTab);
    
    const platformSelect = document.getElementById('platform-select');
    if (platformSelect) {
        platformSelect.value = lastPlatform;
        platformSelect.addEventListener('change', (e) => {
            localStorage.setItem('ameva_last_platform', e.target.value);
        });
    }
});

// -------------------------------------------------------------
// History Database Logic
// -------------------------------------------------------------
function generateUUID() {
    if (crypto && crypto.randomUUID) {
        return crypto.randomUUID();
    }
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
}

function saveToHistory(filename, url, mediaId = null) {
    try {
        const history = JSON.parse(localStorage.getItem('ameva_download_history') || '[]');
        const type = filename.endsWith('.mp4') ? 'VIDEO' : (filename.endsWith('.zip') ? 'ZIP' : 'PHOTO');
        const now = new Date();
        const timeStr = now.toLocaleTimeString('ko-KR', { hour12: false, hour: '2-digit', minute:'2-digit', second:'2-digit' });
        
        history.unshift({
            uuid: generateUUID(),
            timestamp: timeStr,
            date: now.toLocaleDateString('ko-KR'),
            filename: filename,
            url: url,
            type: type,
            mediaId: mediaId
        });
        
        if (history.length > 2000) history.length = 2000;
        
        localStorage.setItem('ameva_download_history', JSON.stringify(history));
    } catch (e) {
        console.error("History save error:", e);
    }
}

function renderHistory() {
    const tbody = document.getElementById('history-table-body');
    if (!tbody) return;
    
    try {
        const history = JSON.parse(localStorage.getItem('ameva_download_history') || '[]');
        tbody.innerHTML = '';
        
        if (history.length === 0) {
            tbody.innerHTML = `<tr><td colspan="4" class="py-8 text-center text-slate-500">다운로드 기록이 없습니다.</td></tr>`;
            return;
        }
        
        history.forEach(item => {
            const tr = document.createElement('tr');
            tr.className = 'hover:bg-slate-900/50 transition-colors group';
            
            let typeColor = 'text-slate-400';
            if (item.type === 'VIDEO') typeColor = 'text-blue-400';
            else if (item.type === 'PHOTO') typeColor = 'text-pink-400';
            else if (item.type === 'ZIP') typeColor = 'text-amber-400';
            
            tr.innerHTML = `
                <td class="py-3 px-4 text-emerald-100">
                    <div class="text-xs text-slate-400">${item.date}</div>
                    ${item.timestamp}
                </td>
                <td class="py-3 px-4">
                    <span class="inline-block px-1.5 py-0.5 bg-black/40 rounded text-[10px] font-bold border border-white/5 ${typeColor}">
                        ${item.type}
                    </span>
                </td>
                <td class="py-3 px-4 text-emerald-100 font-medium truncate max-w-[250px]" title="${item.filename}">
                    ${item.filename}
                </td>
                <td class="py-3 px-4 text-center">
                    <div class="flex items-center justify-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                        <button onclick="shareHistoryItem('${item.url}')" class="px-2 py-1 bg-white/5 hover:bg-emerald-500/20 text-emerald-300 rounded text-xs font-semibold transition-colors border border-transparent hover:border-emerald-500/30" title="클립보드에 주소 복사">
                            공유
                        </button>
                        <button onclick="redownloadHistoryItem('${item.url}', '${item.filename}')" class="px-2 py-1 bg-white/5 hover:bg-emerald-500/20 text-emerald-300 rounded text-xs font-semibold transition-colors border border-transparent hover:border-emerald-500/30" title="다시 다운로드">
                            재다운로드
                        </button>
                    </div>
                </td>
            `;
            tbody.appendChild(tr);
        });
    } catch(e) {
        tbody.innerHTML = `<tr><td colspan="4" class="py-8 text-center text-red-400">기록을 불러오는데 실패했습니다.</td></tr>`;
    }
}

function shareHistoryItem(url) {
    if (!url) return alert('공유할 URL이 없습니다.');
    navigator.clipboard.writeText(url).then(() => {
        alert('✅ 다운로드 원본 링크가 클립보드에 복사되었습니다!');
    }).catch(err => {
        alert('복사 실패: ' + err);
    });
}

function redownloadHistoryItem(url, filename) {
    if (!url) return alert('다운로드할 URL이 없습니다.');
    downloadSingleBlob(url, filename, 'url').catch(e => alert('다운로드 실패: ' + e.message));
}

function openHistoryModal() {
    document.getElementById('history-modal').classList.remove('hidden');
    document.getElementById('history-modal').classList.add('flex');
    renderHistory();
}

function closeHistoryModal() {
    document.getElementById('history-modal').classList.add('hidden');
    document.getElementById('history-modal').classList.remove('flex');
}

function clearHistory() {
    if (confirm('모든 다운로드 기록을 삭제하시겠습니까?')) {
        localStorage.removeItem('ameva_download_history');
        renderHistory();
    }
}
