/**
 * WDT Distributed File Sharing - Frontend JavaScript
 */

// Format file sizes
function formatSize(bytes) {
    if (bytes === 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes / Math.pow(1024, i)).toFixed(1) + ' ' + units[i];
}

// Format timestamps as relative time
function timeAgo(timestamp) {
    const diff = Date.now() / 1000 - timestamp;
    if (diff < 60) return Math.floor(diff) + 's ago';
    if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
    if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
    return Math.floor(diff / 86400) + 'd ago';
}

// Poll status for dashboard auto-update
function pollStatus(callback, interval) {
    interval = interval || 5000;
    setInterval(function () {
        fetch('/api/status')
            .then(function (r) { return r.json(); })
            .then(callback)
            .catch(function () { /* ignore */ });
    }, interval);
}

// Initialize drag-and-drop zones
document.addEventListener('DOMContentLoaded', function () {
    // Prevent default drag behaviors on the page
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(function (evt) {
        document.body.addEventListener(evt, function (e) {
            e.preventDefault();
            e.stopPropagation();
        }, false);
    });
});
