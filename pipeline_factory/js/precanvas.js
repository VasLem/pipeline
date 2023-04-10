function setImage(id_) {
    idObjs = document.getElementById(id_);
    canvas = idObjs.getElementsByClassName('canvas')[0];
    description = idObjs.getElementsByClassName('description')[0];
    index = parseInt(canvas.getAttribute('index'));
    src = canvas.getAttribute('src').split(',')[index];
    var xform = null;
    ctx = canvas.getContext('2d');
    xform = trackTransforms(ctx, xform);
    description.innerText = canvas.getAttribute('key').split(',')[index];
    description.setAttribute('href', src);
    description.setAttribute('test-align', 'center');
    description.setAttribute('download', src.split('/')[src.split('/').length - 1])

    var gkhead = new Image();
    gkhead.src = src;
    gkhead.id = canvas.id;
    gkhead.onload = function () {

        canvas = document.getElementById(this.id).getElementsByClassName('canvas')[0];
        canvasStyle = getComputedStyle(canvas);
        canvas.setAttribute("imheight", this.height);
        canvas.setAttribute('imwidth', this.width);
        canvas.width = Math.max(this.width, canvas.width);
        canvas.style.width = canvas.width + 'px';
        var newHeight = parseInt(parseFloat(canvas.width) / this.width * this.height);
        canvas.style.height = newHeight + "px";
        canvas.height = newHeight;
        ctx = canvas.getContext('2d');
        ctx.scale(canvas.width / this.width, canvas.width / this.width);
        draw(canvas);
    };
    draw(canvas);
}
function draw(canvas) {
    ctx = canvas.getContext('2d');
    // Clear the entire canvas
    var p1 = ctx.transformedPoint(0, 0);
    var p2 = ctx.transformedPoint(canvas.width, canvas.height);
    ctx.clearRect(p1.x, p1.y, p2.x - p1.x, p2.y - p1.y);
    var gkhead = new Image();
    gkhead.src = canvas.getAttribute('src').split(',')[canvas.getAttribute('index')];
    gkhead.id = canvas.id;
    ctx.drawImage(gkhead, 0, 0);
    ctx.save();
}
// Adds ctx.getTransform() - returns an SVGMatrix
// Adds ctx.transformedPoint(x,y) - returns an SVGPoint
function trackTransforms(ctx, xform) {
    if (xform == null) {
        var svg = document.createElementNS("http://www.w3.org/2000/svg", 'svg');
        svg.setAttribute('shape-rendering', "crispEdges")
        var xform = svg.createSVGMatrix();
    }
    ctx.getTransform = function () { return xform; };

    var savedTransforms = [];
    var save = ctx.save;
    ctx.save = function () {
        savedTransforms.push(xform.translate(0, 0));
        return save.call(ctx);
    };

    var restore = ctx.restore;
    ctx.restore = function () {
        xform = savedTransforms.pop();
        return restore.call(ctx);
    };

    var scale = ctx.scale;
    ctx.scale = function (sx, sy) {
        xform = xform.scale(sx, sy);
        return scale.call(ctx, sx, sy);
    };

    var rotate = ctx.rotate;
    ctx.rotate = function (radians) {
        xform = xform.rotate(radians * 180 / Math.PI);
        return rotate.call(ctx, radians);
    };

    var translate = ctx.translate;
    ctx.translate = function (dx, dy) {
        xform = xform.translate(dx, dy);
        return translate.call(ctx, dx, dy);
    };

    var transform = ctx.transform;
    ctx.transform = function (a, b, c, d, e, f) {
        var m2 = svg.createSVGMatrix();
        m2.a = a; m2.b = b; m2.c = c; m2.d = d; m2.e = e; m2.f = f;
        xform = xform.multiply(m2);
        return transform.call(ctx, a, b, c, d, e, f);
    };

    var setTransform = ctx.setTransform;
    ctx.setTransform = function (a, b, c, d, e, f) {
        xform.a = a;
        xform.b = b;
        xform.c = c;
        xform.d = d;
        xform.e = e;
        xform.f = f;
        return setTransform.call(ctx, a, b, c, d, e, f);
    };

    var pt = svg.createSVGPoint();
    ctx.transformedPoint = function (x, y) {
        pt.x = x; pt.y = y;
        return pt.matrixTransform(xform.inverse());
    };
    return xform
};



function imshow(id_, key, src) {
    var canvas = document.createElement('canvas');
    canvas.setAttribute('src', src);
    canvas.setAttribute('key', key);
    canvas.setAttribute('index', 0);
    canvas.className = 'canvas';
    canvas.id = id_;
    canvas.width = 800;
    var srcs = src.split(',');
    var src = '';

    var lastX = canvas.width / 2, lastY = canvas.height / 2;

    var dragStart, dragged;
    canvas.addEventListener('mousedown', function (evt) {
        ctx = this.getContext('2d');
        // document.body.style.mozUserSelect = document.body.style.webkitUserSelect = document.body.style.userSelect = 'none';
        lastX = evt.offsetX || (evt.pageX - this.offsetLeft);
        lastY = evt.offsetY || (evt.pageY - this.offsetTop);
        dragStart = ctx.transformedPoint(lastX, lastY);
        dragged = false;
    }, false);
    canvas.addEventListener('mousemove', function (evt) {

        lastX = evt.offsetX || (evt.pageX - this.offsetLeft);
        lastY = evt.offsetY || (evt.pageY - this.offsetTop);
        dragged = true;
        ctx = this.getContext('2d');
        if (dragStart) {
            var pt = ctx.transformedPoint(lastX, lastY);
            ctx.translate(pt.x - dragStart.x, pt.y - dragStart.y);
            draw(this);
        }

    }, false);

    canvas.addEventListener('mouseup', function (evt) {
        dragStart = null;
        if (!dragged) zoom(this, evt.shiftKey ? -1 : 1);
    }, false);

    var scaleFactor = 1.1;

    var zoom = function (canvas, clicks) {

        ctx = canvas.getContext('2d');
        imHeight = canvas.getAttribute('imheight');
        imWidth = canvas.getAttribute('imwidth');
        var pt = ctx.transformedPoint(lastX, lastY);
        ctx.translate(pt.x, pt.y);
        var factor = Math.pow(scaleFactor, clicks);
        xform = ctx.getTransform();
        if (xform.a * factor * imWidth < canvas.width) {
            factor = 1;
        }
        if (xform.d * factor * imHeight < canvas.height) {
            factor = 1;
        }
        ctx.scale(factor, factor);
        xform.d = Math.max(canvas.height / parseFloat(imHeight), xform.d);
        ctx.translate(-pt.x, -pt.y);
        draw(canvas);
    };

    var handleScroll = function (evt) {
        var delta = evt.wheelDelta ? evt.wheelDelta / 40 : evt.detail ? -evt.detail : 0;
        if (delta) zoom(this, delta);
        return evt.preventDefault() && false;
    };

    canvas.addEventListener('DOMMouseScroll', handleScroll, false);
    canvas.addEventListener('mousewheel', handleScroll, false);

    var body = document.getElementById(id_);
    var descriptionDiv = document.createElement('div');
    descriptionDiv.setAttribute('id', id_);
    descriptionDiv.setAttribute('width', '100%');
    descriptionDiv.setAttribute("text-align", "center");
    var description = document.createElement('a');
    description.setAttribute('class', 'description');
    description.setAttribute('id', id_);

    if (srcs.length > 1) {
        var canvasContainer = document.createElement('div');
        canvasContainer.setAttribute('class', 'canvas-container');
        var prevArrow = document.createElement('div');
        prevArrow.setAttribute('class', 'prev arrow');
        prevArrow.setAttribute('id', id_);
        prevArrow.append("\\21E8");
        canvasContainer.append(description);
        canvasContainer.append(prevArrow);

        canvasContainer.append(canvas);

        var nextArrow = document.createElement("div");
        nextArrow.setAttribute('class', 'next arrow');
        nextArrow.setAttribute('id', id_);
        nextArrow.append("•>");
        canvasContainer.append(nextArrow);
        body.append(canvasContainer);
    } else {
        body.append(description);
        body.appendChild(canvas);
    }
    setImage(id_);
}
