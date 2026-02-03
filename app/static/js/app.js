(function () {
  function qs(sel, root) { return (root || document).querySelector(sel); }
  function qsa(sel, root) { return Array.from((root || document).querySelectorAll(sel)); }

  const wrapper = qs(".search");
  if (!wrapper) return;

  const apiUrl = wrapper.getAttribute("data-api-url");
  const pageUrl = wrapper.getAttribute("data-page-url");

  const input = qs("#globalSearch");
  const dropdown = qs("#globalSearchDropdown");

  if (!apiUrl || !pageUrl || !input || !dropdown) return;

  let timer = null;

  function hide() {
    dropdown.classList.add("hidden");
    dropdown.innerHTML = "";
  }

  function show(html) {
    dropdown.innerHTML = html;
    dropdown.classList.remove("hidden");
  }

  function esc(s) {
    return String(s || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function groupHtml(title, items) {
    if (!items || items.length === 0) return "";
    const rows = items.slice(0, 6).map(r => {
      return `
        <a class="search__row" href="${esc(r.url)}">
          <div class="search__row-title">${esc(r.title)}</div>
          <div class="search__row-sub">${esc(r.subtitle || "")}</div>
        </a>`;
    }).join("");

    return `
      <div class="search__group">
        <div class="search__group-title">${esc(title)}</div>
        ${rows}
      </div>
    `;
  }

  async function fetchAndRender(q) {
    try {
      const res = await fetch(`${apiUrl}?q=${encodeURIComponent(q)}`, { headers: { "Accept": "application/json" }});
      if (!res.ok) { hide(); return; }
      const data = await res.json();
      const results = (data && data.results) || {};
      const html =
        groupHtml("Catalog", results.catalog) +
        groupHtml("Purchases", results.purchases) +
        groupHtml("Sales", results.sales);

      if (!html.trim()) {
        show(`<div class="search__empty">No matches</div>`);
      } else {
        show(html + `<div class="search__footer">Press Enter for full results</div>`);
      }
    } catch (e) {
      hide();
    }
  }

  input.addEventListener("input", function () {
    const q = (input.value || "").trim();
    if (timer) clearTimeout(timer);

    if (q.length < 2) {
      hide();
      return;
    }

    timer = setTimeout(() => fetchAndRender(q), 180);
  });

  input.addEventListener("keydown", function (e) {
    const q = (input.value || "").trim();

    if (e.key === "Escape") {
      hide();
      return;
    }

    if (e.key === "Enter") {
      e.preventDefault();
      if (q.length > 0) {
        window.location.href = `${pageUrl}?q=${encodeURIComponent(q)}`;
      }
    }
  });

  document.addEventListener("click", function (e) {
    const within = wrapper.contains(e.target);
    if (!within) hide();
  });
})();

