// Node Monitor Service Worker
const CACHE_NAME = 'node-monitor-v7';
const STATIC_ASSETS = [
    '/',
    '/index.html',
    '/css/styles.css',
    '/js/lodash.min.js',
    '/grid.js',
    '/status.js',
    '/icon-192.png',
    '/icon-512.png',
    '/manifest.json'
];

// Install event - cache static assets
self.addEventListener('install', (event) => {
    event.waitUntil(
        caches.open(CACHE_NAME)
            .then((cache) => {
                console.log('[SW] Caching static assets');
                return cache.addAll(STATIC_ASSETS);
            })
            .then(() => self.skipWaiting())
    );
});

// Activate event - clean up old caches
self.addEventListener('activate', (event) => {
    event.waitUntil(
        caches.keys().then((cacheNames) => {
            return Promise.all(
                cacheNames
                    .filter((name) => name !== CACHE_NAME)
                    .map((name) => {
                        console.log('[SW] Deleting old cache:', name);
                        return caches.delete(name);
                    })
            );
        }).then(() => self.clients.claim())
    );
});

// Fetch event - network first for API, cache first for static
self.addEventListener('fetch', (event) => {
    const url = new URL(event.request.url);

    // Skip WebSocket connections
    if (url.protocol === 'ws:' || url.protocol === 'wss:') {
        return;
    }

    // For API requests and dynamic data, always go network first
    if (url.pathname.startsWith('/api') || url.pathname === '/websocket') {
        return; // Let the browser handle these normally
    }

    // For static assets, use cache-first strategy
    event.respondWith(
        caches.match(event.request, { ignoreSearch: true })
            .then((cachedResponse) => {
                if (cachedResponse) {
                    // Return cached version but also fetch new version in background
                    fetch(event.request).then((networkResponse) => {
                        if (networkResponse && networkResponse.status === 200) {
                            caches.open(CACHE_NAME).then((cache) => {
                                cache.put(event.request, networkResponse);
                            });
                        }
                    }).catch(() => { });
                    return cachedResponse;
                }

                // Not in cache, fetch from network
                return fetch(event.request).then((networkResponse) => {
                    if (networkResponse && networkResponse.status === 200) {
                        const responseToCache = networkResponse.clone();
                        caches.open(CACHE_NAME).then((cache) => {
                            cache.put(event.request, responseToCache);
                        });
                    }
                    return networkResponse;
                });
            })
    );
});
