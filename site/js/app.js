/**
 * app.js — Application init, event wiring, search orchestration.
 */

let currentPage = 1;
let currentSort = { col: "award_date", dir: "DESC" };
let lastFilters = {};
let totalCount = 0;

// ── Init ──────────────────────────────────────────────────────────────────

async function init() {
    try {
        initLang();
        await initDB(setLoadingStatus);
        populateFilters();
        renderStats();
        bindEvents();
        restoreFromHash();
        hideLoading();
    } catch (err) {
        setLoadingStatus("Error: " + err.message);
        console.error("Init failed:", err);
    }
}

// ── Search ────────────────────────────────────────────────────────────────

function doSearch(page = 1) {
    const filters = getFilterValues();
    lastFilters = filters;
    currentPage = page;

    const { dataSql, countSql, sumSql, params } = buildSearchQuery(
        filters, page, currentSort.col, currentSort.dir
    );

    totalCount = queryScalar(countSql, params) || 0;
    const totalAmount = queryScalar(sumSql, params) || 0;
    const rows = query(dataSql, params);

    const totalPages = Math.ceil(totalCount / PAGE_SIZE);

    if (totalCount > 0) {
        renderResults(rows);
        renderResultsHeader(totalCount, totalAmount);
        renderPagination(page, totalPages, doSearch);
        showResults(true);
    } else {
        showResults(false);
    }

    saveToHash(filters, page);

    // Scroll to results on page change (not first search)
    if (page > 1) {
        document.getElementById("results-section").scrollIntoView({ behavior: "smooth" });
    }
}

// ── Events ────────────────────────────────────────────────────────────────

function bindEvents() {
    document.getElementById("btn-search").addEventListener("click", () => doSearch(1));
    document.getElementById("btn-clear").addEventListener("click", () => {
        clearFilters();
        document.getElementById("results-section").style.display = "none";
        document.getElementById("no-results").style.display = "none";
        document.getElementById("btn-export").style.display = "none";
        window.location.hash = "";
    });
    document.getElementById("btn-export").addEventListener("click", () => {
        exportCSV(lastFilters);
    });

    // Enter key triggers search
    document.getElementById("filters").addEventListener("keydown", (e) => {
        if (e.key === "Enter") {
            e.preventDefault();
            doSearch(1);
        }
    });

    // Debounced text input search
    let debounceTimer;
    for (const id of ["f-contractor", "f-keyword"]) {
        document.getElementById(id).addEventListener("input", () => {
            clearTimeout(debounceTimer);
            debounceTimer = setTimeout(() => doSearch(1), 400);
        });
    }

    // Dropdown change triggers search
    for (const id of ["f-entity", "f-category", "f-fiscal-year"]) {
        document.getElementById(id).addEventListener("change", () => doSearch(1));
    }

    // Language toggle
    document.getElementById("lang-toggle").addEventListener("click", () => {
        setLang(getLang() === "es" ? "en" : "es");
        renderStats();
        if (totalCount > 0) {
            const { countSql, sumSql, params } = buildSearchQuery(
                lastFilters, currentPage, currentSort.col, currentSort.dir
            );
            const totalAmount = queryScalar(sumSql, params) || 0;
            renderResultsHeader(totalCount, totalAmount);
        }
    });

    // Column sort
    document.querySelectorAll("thead th[data-sort]").forEach(th => {
        th.addEventListener("click", () => {
            const col = th.dataset.sort;
            if (currentSort.col === col) {
                currentSort.dir = currentSort.dir === "ASC" ? "DESC" : "ASC";
            } else {
                currentSort.col = col;
                currentSort.dir = col === "amount" ? "DESC" : "ASC";
            }
            // Update header indicators
            document.querySelectorAll("thead th").forEach(h => h.classList.remove("sort-asc", "sort-desc"));
            th.classList.add(currentSort.dir === "ASC" ? "sort-asc" : "sort-desc");
            doSearch(currentPage);
        });
    });
}

// ── URL hash state (shareable searches) ───────────────────────────────────

function saveToHash(filters, page) {
    const params = new URLSearchParams();
    for (const [key, val] of Object.entries(filters)) {
        if (val) params.set(key, val);
    }
    if (page > 1) params.set("page", page);
    const hash = params.toString();
    if (hash) {
        history.replaceState(null, "", "#" + hash);
    }
}

function restoreFromHash() {
    const hash = window.location.hash.slice(1);
    if (!hash) return;

    const params = new URLSearchParams(hash);

    const fieldMap = {
        contractor: "f-contractor",
        entity:     "f-entity",
        amountMin:  "f-amount-min",
        amountMax:  "f-amount-max",
        dateFrom:   "f-date-from",
        dateTo:     "f-date-to",
        category:   "f-category",
        fiscalYear: "f-fiscal-year",
        keyword:    "f-keyword",
    };

    let hasFilter = false;
    for (const [key, elId] of Object.entries(fieldMap)) {
        const val = params.get(key);
        if (val) {
            document.getElementById(elId).value = val;
            hasFilter = true;
        }
    }

    if (hasFilter) {
        const page = parseInt(params.get("page")) || 1;
        doSearch(page);
    }
}

// ── Boot ──────────────────────────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", init);
