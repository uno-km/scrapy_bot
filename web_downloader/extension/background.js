// background.js
// Handles authenticated fetch requests on behalf of the web app

chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
    if (request.action === "fetch_proxy") {
        
        const fetchOptions = {
            method: 'GET',
            headers: request.headers || {},
            credentials: 'include'
        };

        fetch(request.url, fetchOptions)
            .then(async (res) => {
                const text = await res.text();
                if (res.ok) {
                    sendResponse({ success: true, data: text, status: res.status });
                } else {
                    sendResponse({ success: false, error: `HTTP ${res.status}: ${text}`, status: res.status });
                }
            })
            .catch(err => {
                sendResponse({ success: false, error: err.message });
            });
            
        // Return true to indicate we will send a response asynchronously
        return true;
    }
    
    if (request.action === "ig_tab_scrape") {
        const username = request.username;
        try {
            chrome.tabs.create({ url: `https://www.instagram.com/${username}/`, active: true }, (tab) => {
                if (chrome.runtime.lastError || !tab) {
                    sendResponse({ success: false, error: "탭 생성 실패: " + (chrome.runtime.lastError ? chrome.runtime.lastError.message : "알 수 없음") });
                    return;
                }
                const tabId = tab.id;
                
                let injected = false;
                let responseSent = false;
                
                const safeSendResponse = (data) => {
                    if (!responseSent) {
                        responseSent = true;
                        try { sendResponse(data); } catch(e) {}
                    }
                };

                const doExtraction = () => {
                    if (injected) return;
                    injected = true;
                    chrome.tabs.onUpdated.removeListener(listener);
                    
                    setTimeout(() => {
                        try {
                            chrome.scripting.executeScript({
                                target: { tabId: tabId },
                                func: () => {
                                    function findEdges(obj) {
                                        if (!obj || typeof obj !== 'object') return null;
                                        if (obj.edge_owner_to_timeline_media && obj.edge_owner_to_timeline_media.edges) {
                                            return obj.edge_owner_to_timeline_media.edges;
                                        }
                                        for (let key in obj) {
                                            let res = findEdges(obj[key]);
                                            if (res) return res;
                                        }
                                        return null;
                                    }
                                    
                                    let scripts = document.querySelectorAll('script');
                                    let edges = [];
                                    for (let s of scripts) {
                                        if (s.textContent.includes('edge_owner_to_timeline_media')) {
                                            try {
                                                if (s.type.includes('json') || s.type === 'application/json') {
                                                    let data = JSON.parse(s.textContent);
                                                    let found = findEdges(data);
                                                    if (found && found.length > 0) { 
                                                        edges = found; 
                                                        break; 
                                                    }
                                                }
                                            } catch(e) {}
                                        }
                                    }
                                    return edges;
                                }
                            }, (results) => {
                                try { chrome.tabs.remove(tabId, () => { let _ = chrome.runtime.lastError; }); } catch(e) {} // safely close tab
                                
                                if (chrome.runtime.lastError) {
                                    safeSendResponse({ success: false, error: chrome.runtime.lastError.message });
                                    return;
                                }

                                if (results && results[0] && results[0].result && results[0].result.length > 0) {
                                    safeSendResponse({ success: true, edges: results[0].result });
                                } else {
                                    safeSendResponse({ success: false, error: "탭에서 데이터를 추출하지 못했습니다. 인스타그램에 로그인되어 있는지 확인하세요." });
                                }
                            });
                        } catch (err) {
                            safeSendResponse({ success: false, error: "스크립트 주입 에러: " + err.message });
                        }
                    }, 3000); // 3 seconds wait
                };

                const listener = function(changedTabId, info) {
                    if (changedTabId === tabId && info.status === 'complete') {
                        doExtraction();
                    }
                };
                
                chrome.tabs.onUpdated.addListener(listener);
                
                // Safety fallback 1: if the page doesn't finish loading in 6 seconds, try extracting anyway
                setTimeout(() => {
                    doExtraction();
                }, 6000);
                
                // Safety fallback 2: Guarantee response within 10 seconds to prevent app.js 15s timeout
                setTimeout(() => {
                    safeSendResponse({ success: false, error: "확장 프로그램 내부 강제 타임아웃 방어막 가동됨." });
                }, 10000);
            });
        } catch (err) {
            sendResponse({ success: false, error: "탭 생성 중 치명적 에러: " + err.message });
        }
        return true;
    }
});
