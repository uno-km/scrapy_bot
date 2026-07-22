// db.js - IndexedDB wrapper for AMEVA Extension History & Logs

const DB_NAME = 'AMEVADownloaderDB';
const DB_VERSION = 1;
const STORE_DOWNLOADS = 'downloads';
const STORE_LOGS = 'logs';

class AmevaDB {
    constructor() {
        this.db = null;
    }

    async init() {
        if (this.db) return this.db;

        return new Promise((resolve, reject) => {
            const request = indexedDB.open(DB_NAME, DB_VERSION);

            request.onupgradeneeded = (event) => {
                const db = event.target.result;
                
                // Store for Download History
                if (!db.objectStoreNames.contains(STORE_DOWNLOADS)) {
                    const downloadsStore = db.createObjectStore(STORE_DOWNLOADS, { keyPath: 'id', autoIncrement: true });
                    downloadsStore.createIndex('platform', 'platform', { unique: false });
                    downloadsStore.createIndex('timestamp', 'timestamp', { unique: false });
                }

                // Store for Application Logs
                if (!db.objectStoreNames.contains(STORE_LOGS)) {
                    const logsStore = db.createObjectStore(STORE_LOGS, { keyPath: 'id', autoIncrement: true });
                    logsStore.createIndex('timestamp', 'timestamp', { unique: false });
                }
            };

            request.onsuccess = (event) => {
                this.db = event.target.result;
                resolve(this.db);
            };

            request.onerror = (event) => {
                console.error('[AMEVA DB] Error opening IndexedDB:', event.target.error);
                reject(event.target.error);
            };
        });
    }

    // Save a completed or attempted download record
    async addDownloadRecord(record) {
        await this.init();
        return new Promise((resolve, reject) => {
            const tx = this.db.transaction([STORE_DOWNLOADS], 'readwrite');
            const store = tx.objectStore(STORE_DOWNLOADS);
            
            const data = {
                url: record.url,
                title: record.title || 'Untitled Media',
                platform: record.platform || 'General',
                mediaType: record.mediaType || 'video',
                timestamp: new Date().toISOString(),
                status: record.status || 'Success',
                fileUrl: record.fileUrl || '',
                filename: record.filename || ''
            };

            const request = store.add(data);
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
        });
    }

    // Get all download records (latest first)
    async getAllDownloads() {
        await this.init();
        return new Promise((resolve, reject) => {
            const tx = this.db.transaction([STORE_DOWNLOADS], 'readonly');
            const store = tx.objectStore(STORE_DOWNLOADS);
            const request = store.getAll();

            request.onsuccess = () => {
                const results = request.result || [];
                // Sort by timestamp descending
                results.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
                resolve(results);
            };
            request.onerror = () => reject(request.error);
        });
    }

    // Clear all download records
    async clearDownloads() {
        await this.init();
        return new Promise((resolve, reject) => {
            const tx = this.db.transaction([STORE_DOWNLOADS], 'readwrite');
            const store = tx.objectStore(STORE_DOWNLOADS);
            const request = store.clear();
            request.onsuccess = () => resolve(true);
            request.onerror = () => reject(request.error);
        });
    }

    // Log system messages
    async addLog(message, level = 'info') {
        await this.init();
        return new Promise((resolve) => {
            const tx = this.db.transaction([STORE_LOGS], 'readwrite');
            const store = tx.objectStore(STORE_LOGS);
            store.add({
                message,
                level,
                timestamp: new Date().toISOString()
            });
            tx.oncomplete = () => resolve(true);
        });
    }
}

// Global instance
const amevaDB = new AmevaDB();
