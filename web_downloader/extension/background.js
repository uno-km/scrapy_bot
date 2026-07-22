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
});
