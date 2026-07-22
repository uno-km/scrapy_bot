// content.js
// Bridges messages from uno-km.github.io to the extension's background worker

window.addEventListener("message", function(event) {
    // Only accept messages from the same frame
    if (event.source !== window) return;

    // Check if it's our specific fetch request
    if (event.data && event.data.type === "AMEVA_EXT_FETCH") {
        
        // Forward to background.js
        chrome.runtime.sendMessage(
            { action: "fetch_proxy", url: event.data.url, headers: event.data.headers }, 
            function(response) {
                // Send response back to the web page
                window.postMessage({ 
                    type: "AMEVA_EXT_FETCH_RESULT", 
                    id: event.data.id, 
                    response: response 
                }, "*");
            }
        );
    }
    
    // Check for IG Tab Scrape request
    if (event.data && event.data.type === "AMEVA_EXT_IG_SCRAPE") {
        chrome.runtime.sendMessage(
            { action: "ig_tab_scrape", username: event.data.username }, 
            function(response) {
                window.postMessage({ 
                    type: "AMEVA_EXT_IG_SCRAPE_RESULT", 
                    id: event.data.id, 
                    response: response 
                }, "*");
            }
        );
    }
    
    // Check extension status
    if (event.data && event.data.type === "AMEVA_EXT_PING") {
        window.postMessage({ 
            type: "AMEVA_EXT_PONG", 
            version: chrome.runtime.getManifest().version 
        }, "*");
    }
});
