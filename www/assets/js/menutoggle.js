// Copyright 2019-2020 Materialize Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

$(document).ready(function () {
    // Grab any element that has the 'js-toggle' class and add an event listener for the toggleClass function
    var toggleBtns = document.getElementsByClassName('js-toggle')
    for (var i = 0; i < toggleBtns.length; i++) {
        toggleBtns[i].addEventListener('click', toggleClass, false)
    }

    function toggleClass() {

        if (this.classList.contains("nav-parent")) {
            this.classList.toggle("nav-parent-open");
            this.classList.toggle("nav-parent-closed");
        }
        // Define the data target via the dataset "target" (e.g. data-target=".docsmenu")
        var content = this.dataset.target.split(' ')
        // Find any menu items that are open
        var mobileCurrentlyOpen = document.querySelector('.mobilemenu:not(.dn)')
        var desktopCurrentlyOpen = document.querySelector('.desktopmenu:not(.dn)')
        var desktopActive = document.querySelector('.desktopmenu:not(.dn)')

        // Loop through the targets' divs
        for (var i = 0; i < content.length; i++) {
            var matches = document.querySelectorAll(content[i]);
            // for each, if the div has the 'dn' class (which is "display:none;"), remove it, otherwise, add that class
            [].forEach.call(matches, function (dom) {
                dom.classList.contains('dn') ?
                    dom.classList.remove('dn') :
                    dom.classList.add('dn');
                return false;
            });
            // Close the currently open menu items
            if (mobileCurrentlyOpen) mobileCurrentlyOpen.classList.add('dn')
            if (desktopCurrentlyOpen) desktopCurrentlyOpen.classList.add('dn')
            if (desktopActive) desktopActive.classList.remove('db')

        }
    }
});
