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
        populateDownloads();
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
        populateDownloads();
        if (totalCount > 0) {
            doSearch(currentPage);
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

// ── Downloads grid ────────────────────────────────────────────────────────

function populateDownloads() {
    const grid = document.getElementById("downloads-grid");
    if (!grid) return;

    const manifest = getManifest();
    const rawCsvBase = manifest.raw_csv_base_url || "https://github.com/en-he/ocpr-transparency/raw/main/data/raw/";
    const fiscalYears = manifest.fiscal_years && manifest.fiscal_years.length
        ? manifest.fiscal_years
        : getDistinct("fiscal_year").slice().reverse();

    grid.innerHTML = "";

    if (manifest.full_download_db && manifest.full_download_db.url) {
        const dbLink = document.createElement("a");
        dbLink.href = manifest.full_download_db.url;
        dbLink.className = "download-card download-card-featured";
        dbLink.innerHTML = `
            <span class="download-fy">${t("downloads.fullDatabase")}</span>
            <span class="download-label">${t("downloads.sqlite")}</span>
        `;
        grid.appendChild(dbLink);
    }

    for (const fy of fiscalYears) {
        const filename = `contratos_${fy}.csv`;
        const link = document.createElement("a");
        link.href = rawCsvBase + filename;
        link.className = "download-card";
        link.download = filename;
        link.innerHTML = `
            <span class="download-fy">${fy}</span>
            <span class="download-label">${t("downloads.download")} CSV</span>
        `;
        grid.appendChild(link);
    }
}

// ── Boot ──────────────────────────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", init);
