/* ==========================================================================
   sqlite-msgpack docs — interactions + lightweight syntax highlighter.
   No external dependencies.
   ========================================================================== */
(function () {
  "use strict";

  /* ----- Theme ----------------------------------------------------------- */
  var root = document.documentElement;
  try {
    var saved = localStorage.getItem("theme");
    if (saved) root.setAttribute("data-theme", saved);
    else if (window.matchMedia && matchMedia("(prefers-color-scheme: dark)").matches)
      root.setAttribute("data-theme", "dark");
  } catch (e) {}

  function toggleTheme() {
    var next = root.getAttribute("data-theme") === "dark" ? "light" : "dark";
    root.setAttribute("data-theme", next);
    try { localStorage.setItem("theme", next); } catch (e) {}
  }

  /* ----- Wiring after DOM ready ------------------------------------------ */
  document.addEventListener("DOMContentLoaded", function () {
    // theme toggles
    Array.prototype.forEach.call(document.querySelectorAll("[data-theme-toggle]"),
      function (b) { b.addEventListener("click", toggleTheme); });

    // mobile nav
    var navToggle = document.querySelector("[data-nav-toggle]");
    if (navToggle)
      navToggle.addEventListener("click", function () {
        document.body.classList.toggle("nav-open");
      });

    // mobile docs sidebar
    var fab = document.querySelector("[data-sidebar-toggle]");
    var backdrop = document.querySelector(".backdrop");
    function closeSidebar() { document.body.classList.remove("sidebar-open"); }
    if (fab) fab.addEventListener("click", function () {
      document.body.classList.toggle("sidebar-open");
    });
    if (backdrop) backdrop.addEventListener("click", closeSidebar);

    highlightAll();
    addCopyButtons();
    setupFilter();
    setupScrollSpy();
    addHeadingAnchors();

    // close mobile sidebar when a link is tapped
    Array.prototype.forEach.call(document.querySelectorAll(".sidenav__list a"),
      function (a) { a.addEventListener("click", closeSidebar); });
  });

  /* ----- Copy buttons ---------------------------------------------------- */
  var COPY_ICON =
    '<svg viewBox="0 0 16 16" fill="currentColor"><path d="M0 6.75C0 5.784.784 5 1.75 5h1.5a.75.75 0 0 1 0 1.5h-1.5a.25.25 0 0 0-.25.25v7.5c0 .138.112.25.25.25h7.5a.25.25 0 0 0 .25-.25v-1.5a.75.75 0 0 1 1.5 0v1.5A1.75 1.75 0 0 1 9.25 16h-7.5A1.75 1.75 0 0 1 0 14.25Z"/><path d="M5 1.75C5 .784 5.784 0 6.75 0h7.5C15.216 0 16 .784 16 1.75v7.5A1.75 1.75 0 0 1 14.25 11h-7.5A1.75 1.75 0 0 1 5 9.25Zm1.75-.25a.25.25 0 0 0-.25.25v7.5c0 .138.112.25.25.25h7.5a.25.25 0 0 0 .25-.25v-7.5a.25.25 0 0 0-.25-.25Z"/></svg>';
  var CHECK_ICON =
    '<svg viewBox="0 0 16 16" fill="currentColor"><path d="M13.78 4.22a.75.75 0 0 1 0 1.06l-7.25 7.25a.75.75 0 0 1-1.06 0L2.22 9.28a.75.75 0 0 1 1.06-1.06L6 10.94l6.72-6.72a.75.75 0 0 1 1.06 0Z"/></svg>';

  function addCopyButtons() {
    Array.prototype.forEach.call(document.querySelectorAll(".code-block"), function (block) {
      var bar = block.querySelector(".code-block__bar");
      var code = block.querySelector("code");
      if (!code) return;
      if (!bar) {
        bar = document.createElement("div");
        bar.className = "code-block__bar";
        var lang = document.createElement("span");
        lang.className = "code-block__lang";
        lang.textContent = langLabel(code);
        bar.appendChild(lang);
        block.insertBefore(bar, block.firstChild);
      }
      var btn = document.createElement("button");
      btn.className = "copy-btn";
      btn.type = "button";
      btn.innerHTML = COPY_ICON + "<span>Copy</span>";
      btn.addEventListener("click", function () {
        var text = code.innerText;
        var done = function () {
          btn.classList.add("copied");
          btn.innerHTML = CHECK_ICON + "<span>Copied</span>";
          setTimeout(function () {
            btn.classList.remove("copied");
            btn.innerHTML = COPY_ICON + "<span>Copy</span>";
          }, 1600);
        };
        if (navigator.clipboard && navigator.clipboard.writeText)
          navigator.clipboard.writeText(text).then(done, function () { fallbackCopy(text); done(); });
        else { fallbackCopy(text); done(); }
      });
      bar.appendChild(btn);
    });
  }

  function fallbackCopy(text) {
    var ta = document.createElement("textarea");
    ta.value = text; ta.style.position = "fixed"; ta.style.opacity = "0";
    document.body.appendChild(ta); ta.select();
    try { document.execCommand("copy"); } catch (e) {}
    document.body.removeChild(ta);
  }

  function langLabel(code) {
    var m = (code.className || "").match(/language-(\w+)/);
    if (!m) return "code";
    var map = { sql: "SQL", bash: "Shell", sh: "Shell", py: "Python", python: "Python",
      js: "JavaScript", ts: "TypeScript", rust: "Rust", go: "Go", json: "JSON",
      yaml: "YAML", yml: "YAML", cpp: "C++", c: "C", text: "Text" };
    return map[m[1]] || m[1];
  }

  /* ----- Sidebar filter (functions page) --------------------------------- */
  function setupFilter() {
    var input = document.querySelector("[data-filter]");
    if (!input) return;
    var lists = document.querySelectorAll(".docs__sidebar .sidenav__group");
    var empty = document.querySelector(".sidenav__empty");
    input.addEventListener("input", function () {
      var q = input.value.trim().toLowerCase();
      var anyVisible = false;
      Array.prototype.forEach.call(lists, function (group) {
        var items = group.querySelectorAll(".sidenav__list li");
        var groupVisible = false;
        Array.prototype.forEach.call(items, function (li) {
          var match = li.textContent.toLowerCase().indexOf(q) !== -1;
          li.style.display = match ? "" : "none";
          if (match) groupVisible = true;
        });
        group.style.display = groupVisible ? "" : "none";
        if (groupVisible) anyVisible = true;
      });
      if (empty) empty.style.display = anyVisible ? "none" : "block";
    });
  }

  /* ----- Scroll spy ------------------------------------------------------ */
  function setupScrollSpy() {
    var links = document.querySelectorAll(".sidenav__list a[href^='#']");
    if (!links.length || !("IntersectionObserver" in window)) return;
    var map = {};
    var targets = [];
    Array.prototype.forEach.call(links, function (a) {
      var id = a.getAttribute("href").slice(1);
      var el = document.getElementById(id);
      if (el) { map[id] = a; targets.push(el); }
    });
    var current = null;
    var obs = new IntersectionObserver(function (entries) {
      entries.forEach(function (e) {
        if (e.isIntersecting) current = e.target.id;
      });
      if (current && map[current]) {
        Array.prototype.forEach.call(links, function (a) { a.classList.remove("active"); });
        map[current].classList.add("active");
      }
    }, { rootMargin: "-80px 0px -70% 0px", threshold: 0 });
    targets.forEach(function (t) { obs.observe(t); });
  }

  /* ----- Heading anchors ------------------------------------------------- */
  function addHeadingAnchors() {
    Array.prototype.forEach.call(
      document.querySelectorAll(".docs__content h2[id], .docs__content h3[id]"),
      function (h) {
        var a = document.createElement("a");
        a.className = "anchor";
        a.href = "#" + h.id;
        a.setAttribute("aria-label", "Permalink");
        a.textContent = "#";
        h.appendChild(a);
      });
  }

  /* ======================================================================
     Lightweight syntax highlighter
     ====================================================================== */
  var KW = {
    sql: ("select from where insert into values create table trigger view index " +
      "begin end check not null and or as on over group by order having limit offset " +
      "distinct count sum avg min max raise abort primary key default constraint foreign " +
      "references update delete set drop if exists union all join left right inner outer " +
      "case when then else integer text blob real numeric boolean before after each row " +
      "with recursive returning hex").split(" "),
    python: ("def class return if elif else for while in not and or is none true false " +
      "import from as with lambda try except finally raise yield global nonlocal del pass " +
      "break continue assert async await").split(" "),
    js: ("const let var function return if else for while do new class extends super this " +
      "import from export default await async typeof instanceof in of try catch finally throw " +
      "switch case break continue true false null undefined void delete yield static get set").split(" "),
    rust: ("let fn use mut pub struct enum impl trait match if else for while loop return " +
      "mod crate self super as ref move where dyn type const static unsafe async await box " +
      "true false in").split(" "),
    go: ("func package import var const type struct interface map chan return if else for " +
      "range switch case default break continue go defer select true false nil make new").split(" "),
    bash: ("if then else elif fi for while do done case esac in function return export local " +
      "cd echo cat set unset source").split(" "),
    json: ["true", "false", "null"]
  };
  KW.ts = KW.js; KW.sh = KW.bash; KW.py = KW.python; KW.yaml = []; KW.yml = [];

  function kwSet(lang) {
    var arr = KW[lang] || [];
    var s = {};
    for (var i = 0; i < arr.length; i++) s[arr[i]] = 1;
    return s;
  }

  var ESC = { "&": "&amp;", "<": "&lt;", ">": "&gt;" };
  function esc(s) { return s.replace(/[&<>]/g, function (c) { return ESC[c]; }); }
  function span(cls, txt) { return '<span class="' + cls + '">' + esc(txt) + "</span>"; }

  function rules(lang) {
    var line = [], block = [];
    if (lang === "sql") line = [/--[^\n]*/y];
    else if (lang === "python" || lang === "py" || lang === "bash" || lang === "sh" ||
             lang === "yaml" || lang === "yml") line = [/#[^\n]*/y];
    else if (lang === "js" || lang === "ts" || lang === "rust" || lang === "go" ||
             lang === "cpp" || lang === "c") { line = [/\/\/[^\n]*/y]; block = [/\/\*[\s\S]*?\*\//y]; }
    return { line: line, block: block };
  }

  function highlight(src, lang) {
    var ci = lang === "sql";
    var kws = kwSet(lang);
    var r = rules(lang);
    var strings = [
      /"""[\s\S]*?"""/y, /'''[\s\S]*?'''/y,        // python triple
      /r#"[\s\S]*?"#/y, /r"[^"]*"/y,               // rust raw
      /"(?:\\.|[^"\\])*"/y, /'(?:\\.|[^'\\])*'/y, /`(?:\\.|[^`\\])*`/y
    ];
    var numRe = /0[xX][0-9a-fA-F]+|(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?/y;
    var identRe = /[A-Za-z_$][\w$]*/y;
    var wsRe = /\s+/y;

    var out = "", i = 0, n = src.length, k, re, m;
    while (i < n) {
      var consumed = false;
      // block comments
      for (k = 0; k < r.block.length; k++) {
        re = r.block[k]; re.lastIndex = i; m = re.exec(src);
        if (m) { out += span("tok-comment", m[0]); i = re.lastIndex; consumed = true; break; }
      }
      if (consumed) continue;
      // line comments
      for (k = 0; k < r.line.length; k++) {
        re = r.line[k]; re.lastIndex = i; m = re.exec(src);
        if (m) { out += span("tok-comment", m[0]); i = re.lastIndex; consumed = true; break; }
      }
      if (consumed) continue;
      // strings
      for (k = 0; k < strings.length; k++) {
        re = strings[k]; re.lastIndex = i; m = re.exec(src);
        if (m) {
          var after = src.slice(i + m[0].length).match(/^\s*:/);
          out += span(lang === "json" && after ? "tok-func" : "tok-string", m[0]);
          i = re.lastIndex; consumed = true; break;
        }
      }
      if (consumed) continue;
      // whitespace
      wsRe.lastIndex = i; m = wsRe.exec(src);
      if (m) { out += m[0]; i = wsRe.lastIndex; continue; }
      // numbers
      numRe.lastIndex = i; m = numRe.exec(src);
      if (m) { out += span("tok-number", m[0]); i = numRe.lastIndex; continue; }
      // identifiers
      identRe.lastIndex = i; m = identRe.exec(src);
      if (m) {
        var word = m[0];
        var key = ci ? word.toLowerCase() : word;
        var rest = src.slice(i + word.length);
        if (kws[key]) out += span("tok-keyword", word);
        else if (/^\s*\(/.test(rest)) out += span("tok-func", word);
        else if (lang === "sql" && /^msgpack/i.test(word)) out += span("tok-builtin", word);
        else out += esc(word);
        i = identRe.lastIndex; continue;
      }
      // default single char
      out += esc(src[i]); i++;
    }
    return out;
  }

  function highlightAll() {
    Array.prototype.forEach.call(document.querySelectorAll("pre code"), function (code) {
      var cls = code.className || "";
      var match = cls.match(/language-(\w+)/);
      var raw = code.textContent;
      if (!match) { code.innerHTML = esc(raw); return; }
      var lang = match[1];
      if (!KW[lang] && !/^(sql|json|cpp|c)$/.test(lang)) { code.innerHTML = esc(raw); return; }
      code.innerHTML = highlight(raw, lang);
    });
  }
})();
