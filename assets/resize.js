window.addEventListener('load', function () {
    var handle  = document.getElementById('drag-handle');
    var sidebar = document.getElementById('sidebar-panel');
    if (!handle || !sidebar) return;

    var isResizing = false;
    var startX     = 0;
    var startW     = 0;

    handle.addEventListener('mousedown', function (e) {
        isResizing = true;
        startX     = e.clientX;
        startW     = sidebar.getBoundingClientRect().width;
        handle.classList.add('dragging');
        document.body.style.cursor    = 'col-resize';
        document.body.style.userSelect = 'none';
        e.preventDefault();
    });

    document.addEventListener('mousemove', function (e) {
        if (!isResizing) return;
        var newW = Math.max(160, Math.min(520, startW + (e.clientX - startX)));
        sidebar.style.width    = newW + 'px';
        sidebar.style.minWidth = newW + 'px';
    });

    document.addEventListener('mouseup', function () {
        if (!isResizing) return;
        isResizing = false;
        handle.classList.remove('dragging');
        document.body.style.cursor     = '';
        document.body.style.userSelect = '';
    });
});
