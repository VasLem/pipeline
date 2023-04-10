var recursiveResizeToHeight = function (node, height) {
    height += node.scrollHeight;
    node.style.maxHeight = height + "px";
    isParentCollapsible = node.parentElement?.classList.contains('content');
    if (isParentCollapsible) {
        recursiveResizeToHeight(node.parentElement, height)
    }
}
var coll = document.getElementsByClassName("collapsible");
var i;

for (i = 0; i < coll.length; i++) {
    coll[i].addEventListener("click", function () {
        this.classList.toggle("active");
        var content = this.nextElementSibling;
        if (content.style.maxHeight) {
            content.style.maxHeight = null;
        } else {
            recursiveResizeToHeight(content, 0);
        }
    });
}
