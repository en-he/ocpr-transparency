/**
 * app.js — Application init, event wiring, search orchestration.
 */

let currentPage = 1;
let currentSort = { col: "award_date", dir: "DESC" };
let lastFilters = {};
let totalCount = 0;

function hasActiveFilters(filters) {
    return Object.values(filters || {}).some(val => String(val || "").trim() !== "");
}

function renderDashboardForFilters(filters) {
    const filtered = hasActiveFilters(filters);
    const snapshot = filtered
        ? getDashboardData(filters)
        : getSitewideDashboardData();
    renderDashboard(snapshot, { filtered });
}

// ── Init ──────────────────────────────────────────────────────────────────

async function init() {
    try {
        initLang();
        await initDB(setLoadingStatus);
        populateFilters();
        renderStats();
        initDashboardToggle();
        renderDashboardForFilters(getFilterValues());
        populateDownloads();
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

    renderDashboardForFilters(filters);
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
        lastFilters = {};
        totalCount = 0;
        currentPage = 1;
        document.getElementById("results-section").style.display = "none";
        document.getElementById("no-results").style.display = "none";
        document.getElementById("btn-export").style.display = "none";
        window.location.hash = "";
        renderDashboardForFilters(getFilterValues());
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

    // Language toggle
    document.getElementById("lang-toggle").addEventListener("click", () => {
        setLang(getLang() === "es" ? "en" : "es");
        renderStats();
        populateDownloads();
        setDashboardCollapsed(isDashboardCollapsed());

        if (hasActiveFilters(getFilterValues()) || totalCount > 0) {
            doSearch(currentPage);
        } else {
            renderDashboardForFilters(getFilterValues());
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
    } else {
        history.replaceState(null, "", window.location.pathname);
    }
}

function restoreFromHash() {
    const hash = window.location.hash.slice(1);
    if (!hash) return;

    const params = new URLSearchParams(hash);

    const fieldMap = {
        contractNumber: "f-contract-number",
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
    const featured = document.getElementById("downloads-featured");
    const grid = document.getElementById("downloads-grid");
    if (!grid || !featured) return;

    const manifest = getManifest();
    const rawCsvBase = manifest.raw_csv_base_url || "https://github.com/en-he/ocpr-transparency/raw/main/data/raw/";
    const fiscalYears = manifest.fiscal_years && manifest.fiscal_years.length
        ? manifest.fiscal_years
        : getDistinct("fiscal_year").slice().reverse();

    featured.innerHTML = "";
    grid.innerHTML = "";

    if (manifest.full_download_db && manifest.full_download_db.url) {
        const dbLink = document.createElement("a");
        dbLink.href = manifest.full_download_db.url;
        dbLink.className = "download-feature-card";
        dbLink.innerHTML = `
            <span class="download-feature-kicker">${t("downloads.fullDatabase")}</span>
            <strong class="download-feature-title">${t("downloads.sqlite")}</strong>
            <span class="download-feature-copy">${t("downloads.fullDescription")}</span>
        `;
        featured.appendChild(dbLink);
    }

    for (const fy of fiscalYears) {
        const filename = `contratos_${fy}.csv`;
        const link = document.createElement("a");
        link.href = rawCsvBase + filename;
        link.className = "download-card download-card-archive";
        link.download = filename;
        link.innerHTML = `
            <span class="download-fy">${fy}</span>
            <span class="download-label">${t("downloads.archivedCsv")}</span>
            <span class="download-badge">${t("downloads.archiveBadge")}</span>
        `;
        grid.appendChild(link);
    }
}

// ── Boot ──────────────────────────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", init);
