// background.js - Extension Service Worker

importScripts('db.js');

// Listen for messages from content.js or popup.js
chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
    if (request.action === 'DOWNLOAD_MEDIA') {
        handleDownload(request.payload)
            .then(res => sendResponse({ success: true, downloadId: res }))
            .catch(err => sendResponse({ success: false, error: err.message }));
        return true; // Keep channel open for async response
    }
    
    if (request.action === 'GET_HISTORY') {
        amevaDB.getAllDownloads()
            .then(data => sendResponse({ success: true, data }))
            .catch(err => sendResponse({ success: false, error: err.message }));
        return true;
    }

    if (request.action === 'CLEAR_HISTORY') {
        amevaDB.clearDownloads()
            .then(() => sendResponse({ success: true }))
            .catch(err => sendResponse({ success: false, error: err.message }));
        return true;
    }
});

// Native Chrome Download Handler
async function handleDownload(payload) {
    const { url, filename, title, platform, mediaType } = payload;
    
    console.log(`[AMEVA Background] Downloading from ${platform}: ${url}`);
    
    return new Promise((resolve, reject) => {
        const downloadOptions = {
            url: url,
            conflictAction: 'uniquify',
            saveAs: false
        };

        if (filename) {
            downloadOptions.filename = `AMEVA_${platform}_${sanitizeFilename(filename)}`;
        }

        chrome.downloads.download(downloadOptions, async (downloadId) => {
            if (chrome.runtime.lastError) {
                const errMsg = chrome.runtime.lastError.message;
                console.error('[AMEVA Background] Download error:', errMsg);
                
                await amevaDB.addDownloadRecord({
                    url,
                    title: title || filename || 'Media Download',
                    platform: platform || 'Web',
                    mediaType: mediaType || 'video',
                    status: `Failed: ${errMsg}`,
                    filename: filename || ''
                });

                reject(new Error(errMsg));
            } else {
                console.log(`[AMEVA Background] Download started! ID: ${downloadId}`);
                
                await amevaDB.addDownloadRecord({
                    url,
                    title: title || filename || 'Media Download',
                    platform: platform || 'Web',
                    mediaType: mediaType || 'video',
                    status: 'Success',
                    filename: filename || `AMEVA_${downloadId}`
                });

                resolve(downloadId);
            }
        });
    });
}

function sanitizeFilename(name) {
    return name.replace(/[^a-zA-Z0-9_\-\.]/g, '_');
}
