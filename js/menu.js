document.addEventListener('DOMContentLoaded', function () {
    const menuButton = document.getElementById('menu-buttonId');
    const nav = document.querySelector('.nav');

    menuButton.addEventListener('click', function () {
        nav.classList.toggle('nav-open');
    });
});