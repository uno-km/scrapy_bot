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

// Start URL Extraction
async function startUrlDownload() {
    const urlInput = document.getElementById('url-input').value.trim();
    if (!urlInput) {
        logToTerminal('Please enter a valid URL.', 'error');
        return;
    }

    if (!pyodideReady) {
        logToTerminal('Please wait for the WASM engine to initialize.', 'warn');
        return;
    }

    logToTerminal(`Starting extraction for: ${urlInput}`);
    btnDownloadUrl.disabled = true;
    btnDownloadUrl.innerHTML = '<span class="animate-pulse">Extracting...</span>';

    try {
        // Run Python code to extract info using yt-dlp
        // WARNING: As expected and warned, this will fail in a real browser without a proxy due to CORS.
        // We write the logic assuming it 'could' work if CORS wasn't an issue.
        const pyCode = `
import yt_dlp
import json

ydl_opts = {
    'quiet': True,
    'skip_download': True,
    'extract_flat': 'in_playlist'
}

url = "${urlInput}"
result_str = ""
try:
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info_dict = ydl.extract_info(url, download=False)
        result_str = json.dumps(info_dict)
except Exception as e:
    result_str = json.dumps({"error": str(e)})

result_str
        `;
        
        logToTerminal('Executing yt-dlp in Python environment...', 'info');
        
        // Execute python string
        const resultJsonString = await pyodideInstance.runPythonAsync(pyCode);
        const result = JSON.parse(resultJsonString);

        if (result.error) {
            throw new Error(result.error);
        }

        logToTerminal('Extraction successful (Mocked for UI rendering)!', 'success');
        console.log(result);
        
        // Mocking behavior for TikTok photo galleries or general downloads since actual fetch will fail CORS
        mockGalleryRender(result, 'url');

    } catch (error) {
        logToTerminal(`Extraction Error: ${error.message}`, 'error');
        logToTerminal(`[CRITICAL] Browser CORS Policy Blocked the request. A proxy is required for browser-based fetching.`, 'error');
    } finally {
        btnDownloadUrl.disabled = false;
        btnDownloadUrl.innerHTML = 'Extract & Download';
    }
}

// Start Account Search
async function startAccountSearch() {
    const platform = document.getElementById('platform-select').value;
    const accountInput = document.getElementById('account-input').value.trim();
    
    if (!accountInput) {
        logToTerminal('Please enter a username.', 'error');
        return;
    }

    logToTerminal(`Scanning ${platform} account: @${accountInput}...`);
    btnSearchAccount.disabled = true;
    btnSearchAccount.innerHTML = '<span class="animate-pulse">Scanning Profile...</span>';

    try {
        // Again, this will definitively fail without cookies and a proxy.
        // Mocking the Python call for UI demonstration.
        await new Promise(r => setTimeout(r, 1500)); // Fake delay
        
        logToTerminal(`CRITICAL ERROR: ${platform} blocks unauthenticated scraping. CORS & 403 Forbidden.`, 'error');
        logToTerminal(`Mocking UI rendering for demonstration purposes.`, 'warn');
        
        mockGalleryRender(null, 'account');
    } finally {
        btnSearchAccount.disabled = false;
        btnSearchAccount.innerHTML = 'Scan Account Profile';
    }
}

// Render Gallery (Mock data for UI demonstration)
function mockGalleryRender(realData, type) {
    galleryContainer.classList.remove('hidden');
    mediaGrid.innerHTML = '';
    selectedMediaItems.clear();
    updateDownloadButtonText();

    const mockItems = [
        { id: '1', type: 'video', thumb: 'https://picsum.photos/400/600?random=1', title: 'Video 1' },
        { id: '2', type: 'photo', thumb: 'https://picsum.photos/400/600?random=2', title: 'Photo 1' },
        { id: '3', type: 'photo', thumb: 'https://picsum.photos/400/600?random=3', title: 'Photo 2' },
        { id: '4', type: 'video', thumb: 'https://picsum.photos/400/600?random=4', title: 'Reel 1' },
    ];

    mockItems.forEach(item => {
        const card = document.createElement('div');
        card.className = 'gallery-item relative rounded-xl overflow-hidden bg-gray-800 border border-gray-700 aspect-[9/16] cursor-pointer group';
        card.onclick = (e) => toggleMediaSelection(item.id, card, e);

        card.innerHTML = `
            <img src="${item.thumb}" class="w-full h-full object-cover opacity-80 group-hover:opacity-100 transition-opacity" alt="thumbnail">
            
            <div class="absolute top-2 right-2 z-10">
                <input type="checkbox" class="media-item-checkbox pointer-events-none" id="checkbox-${item.id}">
            </div>
            
            <div class="absolute bottom-0 inset-x-0 bg-gradient-to-t from-gray-900 to-transparent p-4 pt-12">
                <span class="inline-block px-2 py-1 bg-gray-900/80 rounded text-xs font-bold text-${item.type === 'video' ? 'blue' : 'pink'}-400 mb-1 backdrop-blur-sm">
                    ${item.type.toUpperCase()}
                </span>
                <p class="text-white text-sm font-medium truncate">${item.title}</p>
            </div>
        `;
        mediaGrid.appendChild(card);
    });
}

function toggleMediaSelection(id, cardElement, event) {
    // Prevent double triggering if clicked directly on checkbox (though it's pointer-events-none)
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
    btnDownloadSelected.textContent = `Download Selected (${selectedMediaItems.size})`;
    if (selectedMediaItems.size > 0) {
        btnDownloadSelected.classList.remove('opacity-50', 'cursor-not-allowed');
    } else {
        btnDownloadSelected.classList.add('opacity-50', 'cursor-not-allowed');
    }
}

async function downloadSelectedMedia() {
    if (selectedMediaItems.size === 0) return;
    
    logToTerminal(`Initiating batch download for ${selectedMediaItems.size} items...`);
    // Here we would normally use JS fetch() to get the actual bytes, 
    // write them to FFmpeg.wasm if muxing is needed, and trigger a save.
    
    logToTerminal('[SIMULATION] Fetching bytes... FAILED (CORS).', 'error');
}

// Start Initialization on Load
window.addEventListener('DOMContentLoaded', () => {
    initWASM();
});
