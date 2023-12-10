"use strict";

var elementosDuvida = document.querySelectorAll(".mainAbout-aboutUs-box");
elementosDuvida.forEach(function (duvida) {
  duvida.addEventListener("click", function () {
    duvida.classList.toggle("active");
  });
});