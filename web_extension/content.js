// content.js - DOM Inspector & Download Button Injector

(function() {
    console.log('[AMEVA Content Script] Active on:', window.location.hostname);

    const hostname = window.location.hostname;

    // Throttle DOM inspection
    let isScanning = false;

    function scanPage() {
        if (isScanning) return;
        isScanning = true;

        if (hostname.includes('instagram.com')) {
            scanInstagram();
        } else if (hostname.includes('tiktok.com')) {
            scanTikTok();
        }

        setTimeout(() => { isScanning = false; }, 1000);
    }

    // --- INSTAGRAM DETECTOR ---
    function scanInstagram() {
        // Find articles/posts/reels containers
        const mediaElements = document.querySelectorAll('article, div[role="dialog"], div[data-testid="post-container"]');
        
        mediaElements.forEach((container, idx) => {
            if (container.dataset.amevaInjected) return;

            const video = container.querySelector('video');
            const img = container.querySelector('img[srcset], img[src*="instagram"]');

            if (video || img) {
                container.dataset.amevaInjected = 'true';
                container.style.position = 'relative';

                const btn = document.createElement('button');
                btn.className = 'ameva-download-btn';
                btn.innerHTML = '<span>📥 AMEVA 다운로드</span>';
                
                btn.onclick = (e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    
                    const mediaUrl = video ? video.src : (img ? img.src : '');
                    if (!mediaUrl) return alert('미디어 주소를 추출할 수 없습니다.');

                    triggerDownload(btn, {
                        url: mediaUrl,
                        platform: 'Instagram',
                        mediaType: video ? 'video' : 'photo',
                        title: `Instagram_${video ? 'Reel' : 'Post'}_${Date.now()}`
                    });
                };

                container.appendChild(btn);
            }
        });
    }

    // --- TIKTOK DETECTOR ---
    function scanTikTok() {
        const videoContainers = document.querySelectorAll('div[data-e2e="recommend-list-item-container"], div[class*="DivItemContainer"], div[data-e2e="user-post-item"]');

        videoContainers.forEach((container, idx) => {
            if (container.dataset.amevaInjected) return;

            const video = container.querySelector('video');
            if (video) {
                container.dataset.amevaInjected = 'true';
                container.style.position = 'relative';

                const btn = document.createElement('button');
                btn.className = 'ameva-download-btn';
                btn.innerHTML = '<span>📥 AMEVA 다운로드</span>';

                btn.onclick = (e) => {
                    e.preventDefault();
                    e.stopPropagation();

                    const mediaUrl = video.src || (video.querySelector('source') ? video.querySelector('source').src : '');
                    if (!mediaUrl) return alert('틱톡 영상 주소를 추출할 수 없습니다.');

                    triggerDownload(btn, {
                        url: mediaUrl,
                        platform: 'TikTok',
                        mediaType: 'video',
                        title: `TikTok_Video_${Date.now()}`
                    });
                };

                container.appendChild(btn);
            }
        });
    }

    function triggerDownload(btnElement, payload) {
        btnElement.classList.add('downloading');
        btnElement.innerHTML = '<span>⏳ 다운로드 중...</span>';

        chrome.runtime.sendMessage({
            action: 'DOWNLOAD_MEDIA',
            payload: payload
        }, (response) => {
            btnElement.classList.remove('downloading');
            if (response && response.success) {
                btnElement.classList.add('success');
                btnElement.innerHTML = '<span>✅ 완료!</span>';
                setTimeout(() => {
                    btnElement.classList.remove('success');
                    btnElement.innerHTML = '<span>📥 AMEVA 다운로드</span>';
                }, 2500);
            } else {
                btnElement.innerHTML = '<span>❌ 실패</span>';
                setTimeout(() => {
                    btnElement.innerHTML = '<span>📥 AMEVA 다운로드</span>';
                }, 2500);
            }
        });
    }

    // MutationObserver to detect dynamically loaded posts/reels
    const observer = new MutationObserver(() => {
        scanPage();
    });

    observer.observe(document.body, { childList: true, subtree: true });
    
    // Initial Scan
    scanPage();
})();
