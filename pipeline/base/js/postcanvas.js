prevArrows = document.getElementsByClassName('prev arrow');
for (var i = 0; i < prevArrows.length; i++) {
    prevArrows[i].addEventListener("click", function () {
        var idObjs = document.getElementById(this.id);
        canvas = idObjs.getElementsByClassName('canvas')[0];
        description = idObjs.getElementsByClassName('description')[0];
        srcs = canvas.getAttribute('src').split(',');

        var index = parseInt(canvas.getAttribute('index'));

        index--;
        if (index < 0) {
            index = srcs.length - 1;
        }

        var src = srcs[index];

        canvas.setAttribute('index', index);
        setImage(this.id);
    });
}
nextArrows = document.getElementsByClassName('next arrow');
for (var i = 0; i < nextArrows.length; i++) {
    nextArrows[i].addEventListener("click", function () {
        var idObjs = document.getElementById(this.id);
        canvas = idObjs.getElementsByClassName('canvas')[0];
        srcs = canvas.getAttribute('src').split(',');
        var index = parseInt(canvas.getAttribute('index'));
        canvas.setAttribute('index', (index + 1) % srcs.length);
        setImage(this.id);
    });
}
