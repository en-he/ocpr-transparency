/**
 * app.js — Application init, event wiring, search orchestration.
 */

let currentPage = 1;
let currentSort = { col: "award_date", dir: "DESC" };
let lastFilters = {};
let totalCount = 0;
let exportCounts = { summary: 0, detailed: 0 };
let openSortMenuCol = null;
let currentSearchStateRef = "";

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

function ensureSortHeaderUI() {
    document.querySelectorAll("thead th[data-sort]").forEach(th => {
        if (th.querySelector(".sort-header-inner")) return;
        const label = th.dataset.i18n ? t(th.dataset.i18n) : th.textContent.trim();
        th.replaceChildren();

        const inner = document.createElement("span");
        inner.className = "sort-header-inner";

        const labelSpan = document.createElement("span");
        labelSpan.className = "sort-header-label";
        labelSpan.textContent = label;

        const button = document.createElement("button");
        button.type = "button";
        button.className = "sort-indicator-btn";
        button.addEventListener("click", event => {
            event.preventDefault();
            event.stopPropagation();

            if (currentSort.col !== th.dataset.sort) {
                currentSort.col = th.dataset.sort;
                currentSort.dir = th.dataset.sort === "amount" ? "DESC" : "ASC";
            }

            openSortMenuCol = openSortMenuCol === th.dataset.sort ? null : th.dataset.sort;
            syncSortIndicators();
        });

        const icon = document.createElement("span");
        icon.className = "sort-indicator-icon";
        icon.setAttribute("aria-hidden", "true");
        button.appendChild(icon);

        inner.appendChild(labelSpan);
        inner.appendChild(button);

        const menu = document.createElement("div");
        menu.className = "sort-order-menu";

        const arrow = document.createElement("div");
        arrow.className = "sort-order-arrow";
        menu.appendChild(arrow);

        const title = document.createElement("div");
        title.className = "sort-order-title";
        title.textContent = t("sort.order");
        menu.appendChild(title);

        ["ASC", "DESC"].forEach(dir => {
            const item = document.createElement("button");
            item.type = "button";
            item.className = "sort-order-item";
            item.dataset.sortDirection = dir;
            item.addEventListener("click", event => {
                event.preventDefault();
                event.stopPropagation();
                currentSort.col = th.dataset.sort;
                currentSort.dir = dir;
                openSortMenuCol = null;
                syncSortIndicators();
                doSearch(currentPage);
            });

            const check = document.createElement("span");
            check.className = "sort-order-check";
            check.textContent = "\u2713";

            const text = document.createElement("span");
            text.textContent = dir === "ASC" ? t("sort.ascending") : t("sort.descending");

            item.appendChild(check);
            item.appendChild(text);
            menu.appendChild(item);
        });

        inner.appendChild(menu);
        th.appendChild(inner);
    });
}

function updateSortMenuLabels() {
    document.querySelectorAll("thead th[data-sort]").forEach(th => {
        const title = th.querySelector(".sort-order-title");
        if (title) title.textContent = t("sort.order");
        th.querySelectorAll(".sort-order-item").forEach(item => {
            const textSpan = item.querySelector("span:last-child");
            if (textSpan) {
                textSpan.textContent = item.dataset.sortDirection === "ASC"
                    ? t("sort.ascending")
                    : t("sort.descending");
            }
        });
        const btn = th.querySelector(".sort-indicator-btn");
        if (btn) btn.setAttribute("aria-label", t("sort.changeOrder"));
    });
}

function syncSortIndicators() {
    if (!document.querySelector("thead th .sort-header-inner")) {
        ensureSortHeaderUI();
    }

    document.querySelectorAll("thead th[data-sort]").forEach(th => {
        const active = th.dataset.sort === currentSort.col;
        const menuOpen = active && openSortMenuCol === th.dataset.sort;
        const button = th.querySelector(".sort-indicator-btn");
        const icon = th.querySelector(".sort-indicator-icon");
        const menu = th.querySelector(".sort-order-menu");

        th.classList.remove("sort-asc", "sort-desc");
        th.classList.remove("sort-active", "sort-menu-open");
        th.setAttribute("aria-sort", "none");

        if (icon) {
            icon.textContent = active
                ? (currentSort.dir === "ASC" ? "\u25B2" : "\u25BC")
                : "\u2195";
        }

        if (active) {
            const direction = currentSort.dir === "ASC" ? "ascending" : "descending";
            th.classList.add(currentSort.dir === "ASC" ? "sort-asc" : "sort-desc");
            th.classList.add("sort-active");
            th.setAttribute("aria-sort", direction);
        }

        if (button) {
            button.hidden = false;
            button.setAttribute("aria-label", t("sort.changeOrder"));
            button.setAttribute("aria-expanded", String(menuOpen));
        }

        if (menu) {
            menu.hidden = !menuOpen;
            th.classList.toggle("sort-menu-open", menuOpen);
            menu.querySelectorAll(".sort-order-item").forEach(item => {
                const selected = active && item.dataset.sortDirection === currentSort.dir;
                item.classList.toggle("is-selected", selected);
                item.setAttribute("aria-pressed", String(selected));
            });
        }
    });

    const wrapper = document.querySelector(".table-wrapper");
    if (wrapper) {
        wrapper.style.overflowX = openSortMenuCol ? "visible" : "";
    }

    // Position open menu and arrow relative to the button
    document.querySelectorAll(".sort-order-menu").forEach(menu => {
        menu.style.right = "";
        menu.style.left = "";
        const arrowEl = menu.querySelector(".sort-order-arrow");
        if (arrowEl) arrowEl.style.left = "";
        if (!menu.hidden) {
            const menuRect = menu.getBoundingClientRect();
            const btn = menu.closest(".sort-header-inner")?.querySelector(".sort-indicator-btn");
            // Clamp if overflowing left viewport edge
            if (menuRect.left < 4) {
                menu.style.right = "auto";
                menu.style.left = "0";
            }
            // Point arrow at the button center
            if (btn && arrowEl) {
                const btnRect = btn.getBoundingClientRect();
                const updatedMenuRect = menu.getBoundingClientRect();
                const btnCenter = btnRect.left + btnRect.width / 2;
                const arrowLeft = btnCenter - updatedMenuRect.left - 6;
                arrowEl.style.left = arrowLeft + "px";
            }
        }
    });
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
        ensureSortHeaderUI();
        bindEvents();
        syncSortIndicators();
        setExportResultsState({ visible: false });
        restoreFromHash();
        hideLoading();
    } catch (err) {
        setLoadingStatus("Error: " + err.message);
        console.error("Init failed:", err);
    }
}

// ── Search ────────────────────────────────────────────────────────────────

function doSearch(page = 1) {
    try {
        const filters = getFilterValues();
        lastFilters = filters;
        currentPage = page;
        syncSortIndicators();
        setSearchStateRef(buildHashState(filters, page));

        let rows = [];
        let totalAmount = 0;
        let detailedCount = 0;

        if (filters.contractNumber) {
            const mergedResults = searchContractFamilies(filters, page, currentSort.col, currentSort.dir);
            rows = mergedResults.rows;
            totalCount = mergedResults.totalCount;
            totalAmount = mergedResults.totalAmount;
            detailedCount = mergedResults.detailedCount;
        } else {
            const { dataSql, countSql, sumSql, params } = buildSearchQuery(
                filters, page, currentSort.col, currentSort.dir
            );

            totalCount = queryScalar(countSql, params) || 0;
            totalAmount = queryScalar(sumSql, params) || 0;
            rows = query(dataSql, params);
            const detailedQuery = totalCount > 0
                ? buildDetailedQuery(filters, 1, currentSort.col, currentSort.dir, { limit: 1, offset: 0 })
                : null;
            detailedCount = detailedQuery
                ? (queryScalar(detailedQuery.countSql, detailedQuery.params) || 0)
                : 0;
        }

        exportCounts = {
            summary: Number(totalCount || 0),
            detailed: Number(detailedCount || 0),
        };

        const totalPages = Math.ceil(totalCount / PAGE_SIZE);

        if (totalCount > 0) {
            renderResults(rows);
            renderResultsHeader(totalCount, totalAmount);
            renderPagination(page, totalPages, doSearch);
            showResults(true);
            setExportResultsState({ visible: true, counts: exportCounts });
        } else {
            showResults(false);
            setExportResultsState({ visible: false });
        }

        renderDashboardForFilters(filters);
        saveToHash(filters, page);

        // Scroll to results on page change (not first search)
        if (page > 1) {
            document.getElementById("results-section").scrollIntoView({ behavior: "smooth" });
        }
    } catch (err) {
        console.error("Search failed:", err);
        showResults(false);
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
        exportCounts = { summary: 0, detailed: 0 };
        setSearchStateRef("");
        document.getElementById("results-section").style.display = "none";
        document.getElementById("no-results").style.display = "none";
        setExportResultsState({ visible: false });
        window.location.hash = "";
        renderDashboardForFilters(getFilterValues());
    });
    document.getElementById("btn-export").addEventListener("click", () => {
        exportResults(lastFilters);
    });
    document.getElementById("export-mode").addEventListener("change", updateExportHelper);
    document.getElementById("export-format").addEventListener("change", updateExportHelper);

    // Enter key triggers search
    document.getElementById("filters").addEventListener("keydown", (e) => {
        if (e.target && e.target.closest("#export-panel")) {
            return;
        }
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
        updateSortMenuLabels();
        syncSortIndicators();

        if (hasActiveFilters(getFilterValues()) || totalCount > 0) {
            doSearch(currentPage);
        } else {
            renderDashboardForFilters(getFilterValues());
        }
    });

    // Column sort
    document.querySelectorAll("thead th[data-sort]").forEach(th => {
        th.tabIndex = 0;
        th.addEventListener("click", event => {
            if (event.target.closest(".sort-indicator-btn") || event.target.closest(".sort-order-menu")) {
                return;
            }
            const col = th.dataset.sort;
            if (currentSort.col === col) {
                currentSort.dir = currentSort.dir === "ASC" ? "DESC" : "ASC";
            } else {
                currentSort.col = col;
                currentSort.dir = col === "amount" ? "DESC" : "ASC";
            }
            openSortMenuCol = null;
            syncSortIndicators();
            doSearch(currentPage);
        });
        th.addEventListener("keydown", event => {
            if (event.target.closest(".sort-indicator-btn") || event.target.closest(".sort-order-menu")) {
                return;
            }
            if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                th.click();
            }
        });
    });

    document.addEventListener("click", event => {
        if (!event.target.closest("thead th[data-sort]")) {
            if (openSortMenuCol) {
                openSortMenuCol = null;
                syncSortIndicators();
            }
        }
    });
}

// ── URL hash state (shareable searches) ───────────────────────────────────

function setSearchStateRef(hash) {
    currentSearchStateRef = String(hash || "").replace(/^#/, "");
    window.__ocprCurrentSearchRef = currentSearchStateRef;
}

function buildHashState(filters, page) {
    const params = new URLSearchParams();
    for (const [key, val] of Object.entries(filters)) {
        if (val) params.set(key, val);
    }
    if (page > 1) params.set("page", page);
    if (currentSort.col !== "award_date" || currentSort.dir !== "DESC") {
        params.set("sortCol", currentSort.col);
        params.set("sortDir", currentSort.dir);
    }
    return params.toString();
}

function saveToHash(filters, page) {
    const hash = buildHashState(filters, page);
    setSearchStateRef(hash);
    if (hash) {
        history.replaceState(null, "", "#" + hash);
    } else {
        history.replaceState(null, "", window.location.pathname);
    }
}

function restoreFromHash() {
    const hash = window.location.hash.slice(1);
    if (!hash) return;

    setSearchStateRef(hash);
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
        serviceType: "f-service-type",
        validFrom:  "f-valid-from",
        validTo:    "f-valid-to",
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

    const sortCol = params.get("sortCol");
    const sortDir = params.get("sortDir");
    if (sortCol) {
        const validCols = ["contract_number", "contractor", "entity", "amount", "award_date", "service_category"];
        if (validCols.includes(sortCol)) {
            currentSort.col = sortCol;
            currentSort.dir = sortDir === "ASC" ? "ASC" : "DESC";
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
    const fiscalYears = manifest.archived_csv_fiscal_years && manifest.archived_csv_fiscal_years.length
        ? manifest.archived_csv_fiscal_years
        : manifest.fiscal_years && manifest.fiscal_years.length
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
