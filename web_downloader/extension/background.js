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
                    
                    try {
                        chrome.scripting.executeScript({
                            target: { tabId: tabId },
                            func: () => {
                                return new Promise((resolve) => {
                                    let attempts = 0;
                                    let checkInterval = setInterval(() => {
                                        let edgesMap = new Map();
                                        
                                        // 1. JSON Extraction attempt
                                        function extractNodes(obj) {
                                            if (!obj || typeof obj !== 'object') return;
                                            if (typeof obj.shortcode === 'string' && typeof obj.display_url === 'string') {
                                                edgesMap.set(obj.shortcode, { node: obj });
                                            }
                                            for (let key in obj) {
                                                if (obj.hasOwnProperty(key)) {
                                                    extractNodes(obj[key]);
                                                }
                                            }
                                        }
                                        
                                        let scripts = document.querySelectorAll('script');
                                        for (let s of scripts) {
                                            if (s.textContent.includes('shortcode') && s.textContent.includes('display_url')) {
                                                try {
                                                    if (s.type === 'application/json' || s.type.includes('json')) {
                                                        extractNodes(JSON.parse(s.textContent));
                                                    }
                                                } catch(e) {}
                                            }
                                        }
                                        
                                        let edges = Array.from(edgesMap.values());
                                        
                                        // 2. DOM Extraction attempt
                                        if (edges.length === 0) {
                                            let anchors = document.querySelectorAll('a[href^="/p/"], a[href^="/reel/"]');
                                            for (let a of anchors) {
                                                let match = a.href.match(/(?:p|reel)\/([^\/?#&]+)/);
                                                if (match) {
                                                    let shortcode = match[1];
                                                    let img = a.querySelector('img');
                                                    if (img && !edgesMap.has(shortcode)) {
                                                        edgesMap.set(shortcode, {
                                                            node: {
                                                                shortcode: shortcode,
                                                                display_url: img.src,
                                                                is_video: a.href.includes('/reel/') || !!a.querySelector('svg'),
                                                                __from_dom: true
                                                            }
                                                        });
                                                    }
                                                }
                                            }
                                            edges = Array.from(edgesMap.values());
                                        }
                                        
                                        attempts++;
                                        
                                        // If we found posts, or we tried for 8 seconds (16 attempts * 500ms)
                                        if (edges.length > 0 || attempts >= 16) {
                                            clearInterval(checkInterval);
                                            let pageText = document.body ? document.body.innerText.substring(0, 200).replace(/\n/g, ' ') : "No body";
                                            resolve({ edges: edges, pageText: pageText, url: window.location.href });
                                        }
                                    }, 500); // Check every 500ms
                                });
                            }
                        }, (results) => {
                            try { chrome.tabs.remove(tabId, () => { let _ = chrome.runtime.lastError; }); } catch(e) {} // safely close tab
                            
                            if (chrome.runtime.lastError) {
                                safeSendResponse({ success: false, error: chrome.runtime.lastError.message });
                                return;
                            }

                            if (results && results[0] && results[0].result) {
                                let res = results[0].result;
                                if (res.edges && res.edges.length > 0) {
                                    safeSendResponse({ success: true, edges: res.edges });
                                } else {
                                    safeSendResponse({ success: false, error: "데이터 없음. 탭 화면 요약: [" + res.pageText + "] (URL: " + res.url + ")" });
                                }
                            } else {
                                safeSendResponse({ success: false, error: "탭에서 스크립트 실행 결과를 받지 못했습니다." });
                            }
                        });
                    } catch (err) {
                        safeSendResponse({ success: false, error: "스크립트 주입 에러: " + err.message });
                    }
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
