
let observer = new ResizeObserver(function (mutations) {
    const observed = mutations.map((mutation) => mutation.target.closest('iframe'))[0];
    isParentCollapsed = observed?.parentNode?.parentNode.classList.contains('content') && observed?.parentNode?.parentNode.previousElementSibling.classList.contains('active');
    if (isParentCollapsed) {
        recursiveResizeToHeight(observed.parentNode.parentNode, 0);
    }
});

var children = document.querySelectorAll('iframe');
for (var i = 0; i < children.length; i++) {
    const child = children[i];
    observer.observe(child, { attributes: true });
}

document.addEventListener('DataPageReady', function (event) {
    $(".content").resizable({
        animate: true, animateEasing: 'swing', animateDuration: 500
    });
});
