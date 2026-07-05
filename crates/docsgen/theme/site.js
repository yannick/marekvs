// marekvs docs — theme toggle, mobile nav, copy buttons, TOC scrollspy.
(function () {
  'use strict';

  // ---- theme toggle -------------------------------------------------------
  var root = document.documentElement;
  document.querySelectorAll('.theme-toggle').forEach(function (btn) {
    btn.addEventListener('click', function () {
      var next = root.dataset.theme === 'dark' ? 'light' : 'dark';
      root.dataset.theme = next;
      try { localStorage.setItem('mk-theme', next); } catch (e) {}
    });
  });

  // ---- mobile nav ---------------------------------------------------------
  var toggle = document.querySelector('.nav-toggle');
  var topnav = document.querySelector('.topnav');
  var sidebar = document.getElementById('sidebar');
  if (toggle) {
    toggle.addEventListener('click', function () {
      var open = topnav && topnav.classList.toggle('open');
      if (sidebar) sidebar.classList.toggle('open');
      toggle.setAttribute('aria-expanded', String(!!open));
    });
  }

  // ---- copy buttons -------------------------------------------------------
  document.querySelectorAll('.code-copy').forEach(function (btn) {
    btn.addEventListener('click', function () {
      var code = btn.parentElement.querySelector('code');
      var text = code ? code.innerText : '';
      navigator.clipboard.writeText(text).then(function () {
        btn.textContent = 'copied';
        btn.classList.add('done');
        setTimeout(function () { btn.textContent = 'copy'; btn.classList.remove('done'); }, 1400);
      }).catch(function () {});
    });
  });

  // ---- TOC scrollspy ------------------------------------------------------
  var links = Array.prototype.slice.call(document.querySelectorAll('.toc a'));
  if (links.length) {
    var map = {};
    var targets = links.map(function (a) {
      var id = decodeURIComponent(a.getAttribute('href').slice(1));
      var el = document.getElementById(id);
      if (el) map[id] = a;
      return el;
    }).filter(Boolean);

    var spy = function () {
      var pos = window.scrollY + 96;
      var current = null;
      for (var i = 0; i < targets.length; i++) {
        if (targets[i].offsetTop <= pos) current = targets[i]; else break;
      }
      links.forEach(function (a) { a.classList.remove('active'); });
      if (current && map[current.id]) map[current.id].classList.add('active');
    };
    var ticking = false;
    window.addEventListener('scroll', function () {
      if (!ticking) { window.requestAnimationFrame(function () { spy(); ticking = false; }); ticking = true; }
    }, { passive: true });
    spy();
  }
})();
