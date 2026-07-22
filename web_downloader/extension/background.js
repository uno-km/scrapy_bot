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
        chrome.tabs.create({ url: `https://www.instagram.com/${username}/`, active: true }, (tab) => {
            const tabId = tab.id;
            
            let injected = false;
            
            const doExtraction = () => {
                if (injected) return;
                injected = true;
                chrome.tabs.onUpdated.removeListener(listener);
                
                // Wait a moment for React to hydrate
                setTimeout(() => {
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
                        if (results && results[0] && results[0].result && results[0].result.length > 0) {
                            sendResponse({ success: true, edges: results[0].result });
                        } else {
                            sendResponse({ success: false, error: "탭에서 데이터를 추출하지 못했습니다. 인스타그램에 로그인되어 있는지 확인하세요." });
                        }
                    });
                }, 3000); // 3 seconds wait
            };

            const listener = function(changedTabId, info) {
                if (changedTabId === tabId && info.status === 'complete') {
                    doExtraction();
                }
            };
            
            chrome.tabs.onUpdated.addListener(listener);
            
            // Safety fallback: if the page doesn't finish loading in 6 seconds, try extracting anyway
            setTimeout(() => {
                doExtraction();
            }, 6000);
        });
        return true;
    }
});
