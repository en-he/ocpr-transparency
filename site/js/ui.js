/**
 * ui.js — DOM rendering: filters, results table, pagination, export.
 */

const DASHBOARD_PREF_KEY = "ocpr-dashboard-collapsed";
const DASHBOARD_MOBILE_MEDIA = "(max-width: 640px)";
const EXPORT_LIMITS = {
    csv: 100000,
    xlsx: 100000,
    pdf: {
        summary: 250,
        detailed: 100,
    },
};

let exportState = {
    visible: false,
    busy: false,
    counts: {
        summary: 0,
        detailed: 0,
    },
};

// ── Format helpers ────────────────────────────────────────────────────────

function formatAmount(val) {
    if (val == null || val === "") return "-";
    return "$" + Number(val).toLocaleString("en-US", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    });
}

function formatDate(val) {
    if (!val) return "-";
    const raw = String(val).trim();
    if (!raw || raw === "0.00") return "-";

    let year;
    let month;
    let day;

    if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
        [year, month, day] = raw.split("-").map(Number);
    } else if (/^\d{2}[-/]\d{2}[-/]\d{4}$/.test(raw)) {
        const [first, second, y] = raw.split(/[-/]/).map(Number);
        year = y;
        if (first > 12 && second <= 12) {
            day = first;
            month = second;
        } else {
            month = first;
            day = second;
        }
    } else {
        return raw;
    }

    if (!year || !month || !day || month < 1 || month > 12 || day < 1 || day > 31) {
        return raw;
    }

    const monthLabel = new Date(Date.UTC(year, month - 1, day)).toLocaleString("en-US", {
        month: "long",
        timeZone: "UTC",
    });
    return `${monthLabel} ${day}, ${year}`;
}

function truncate(str, max = 40) {
    if (!str) return "-";
    return str.length > max ? str.slice(0, max) + "..." : str;
}

function formatCompactAmount(val) {
    if (val == null || val === "") return "-";
    return "$" + Number(val).toLocaleString("en-US", {
        notation: "compact",
        maximumFractionDigits: 1,
    });
}

function formatCount(val) {
    return Number(val || 0).toLocaleString("en-US");
}

// ── Dropdown population ───────────────────────────────────────────────────

function populateDropdown(selectId, values) {
    const el = document.getElementById(selectId);
    const firstOption = el.options[0]; // "Todas" / "Todos"
    el.innerHTML = "";
    el.appendChild(firstOption);
    for (const val of values) {
        const opt = document.createElement("option");
        opt.value = val;
        opt.textContent = val;
        el.appendChild(opt);
    }
}

function populateDatalist(listId, values) {
    const el = document.getElementById(listId);
    if (!el) return;

    el.innerHTML = "";
    for (const val of values) {
        const opt = document.createElement("option");
        opt.value = val;
        el.appendChild(opt);
    }
}

function populateFilters() {
    populateDatalist("f-entity-options", getDistinct("entity"));
    populateDropdown("f-category", getDistinct("service_category"));
    populateDatalist("f-service-type-options", getDistinct("service_type"));
    populateDropdown("f-fiscal-year", getDistinct("fiscal_year"));
}

// ── Stats bar ─────────────────────────────────────────────────────────────

function renderStats() {
    const stats = getStats();
    document.getElementById("stat-total").textContent =
        `${stats.total.toLocaleString("es")} ${t("stats.contracts")}`;
    document.getElementById("stat-amount").textContent =
        `${formatAmount(stats.amount)} ${t("stats.total")}`;
    document.getElementById("stat-updated").textContent =
        `${stats.minYear} - ${stats.maxYear}`;
    document.getElementById("stats-bar").style.display = "";
}

// ── Dashboard ─────────────────────────────────────────────────────────────

function getDefaultDashboardCollapsed() {
    return Boolean(window.matchMedia && window.matchMedia(DASHBOARD_MOBILE_MEDIA).matches);
}

function isDashboardCollapsed() {
    const section = document.getElementById("dashboard-section");
    return section ? section.classList.contains("is-collapsed") : false;
}

function getDashboardCollapsedPreference() {
    const stored = localStorage.getItem(DASHBOARD_PREF_KEY);
    if (stored === "true") return true;
    if (stored === "false") return false;
    return getDefaultDashboardCollapsed();
}

function setDashboardCollapsed(collapsed, persist = false) {
    const section = document.getElementById("dashboard-section");
    const toggle = document.getElementById("dashboard-toggle");
    const label = document.getElementById("dashboard-toggle-label");
    if (!section || !toggle || !label) return;

    section.classList.toggle("is-collapsed", collapsed);
    toggle.setAttribute("aria-expanded", String(!collapsed));
    label.textContent = collapsed ? t("dashboard.show") : t("dashboard.hide");

    if (persist) {
        localStorage.setItem(DASHBOARD_PREF_KEY, String(collapsed));
    }
}

function initDashboardToggle() {
    const toggle = document.getElementById("dashboard-toggle");
    if (!toggle) return;

    if (toggle.dataset.bound !== "true") {
        toggle.addEventListener("click", () => {
            setDashboardCollapsed(!isDashboardCollapsed(), true);
        });
        toggle.dataset.bound = "true";
    }

    setDashboardCollapsed(getDashboardCollapsedPreference());
}

function renderDashboardList(listId, items) {
    const list = document.getElementById(listId);
    if (!list) return;

    list.innerHTML = "";
    if (!items || items.length === 0) {
        const empty = document.createElement("li");
        empty.className = "dashboard-empty";
        empty.textContent = t("dashboard.empty");
        list.appendChild(empty);
        return;
    }

    const maxAmount = Math.max(...items.map(item => Number(item.total_amount || 0)), 0);

    items.forEach((item, index) => {
        const amount = Number(item.total_amount || 0);
        const li = document.createElement("li");
        li.className = "leaderboard-item";

        const rank = document.createElement("span");
        rank.className = "leaderboard-rank";
        rank.textContent = String(index + 1);

        const main = document.createElement("div");
        main.className = "leaderboard-main";

        const name = document.createElement("div");
        name.className = "leaderboard-name";
        name.textContent = item.name || "-";

        const meta = document.createElement("div");
        meta.className = "leaderboard-meta";
        meta.textContent = `${formatCount(item.family_count)} ${t("dashboard.families")}`;

        const track = document.createElement("div");
        track.className = "leaderboard-track";

        const bar = document.createElement("span");
        bar.className = "leaderboard-bar";
        bar.style.width = `${maxAmount > 0 ? (amount / maxAmount) * 100 : 0}%`;
        track.appendChild(bar);

        main.appendChild(name);
        main.appendChild(meta);
        main.appendChild(track);

        const amountEl = document.createElement("div");
        amountEl.className = "leaderboard-amount";
        amountEl.textContent = formatCompactAmount(amount);
        amountEl.title = formatAmount(amount);

        li.appendChild(rank);
        li.appendChild(main);
        li.appendChild(amountEl);
        list.appendChild(li);
    });
}

function renderDashboardTrend(items) {
    const container = document.getElementById("dashboard-trend");
    if (!container) return;

    container.innerHTML = "";
    if (!items || items.length === 0) {
        const empty = document.createElement("p");
        empty.className = "dashboard-empty";
        empty.textContent = t("dashboard.empty");
        container.appendChild(empty);
        return;
    }

    const maxAmount = Math.max(...items.map(item => Number(item.total_amount || 0)), 0);

    items.forEach(item => {
        const amount = Number(item.total_amount || 0);
        const row = document.createElement("div");
        row.className = "trend-row";

        const label = document.createElement("div");
        label.className = "trend-label";
        label.textContent = item.fiscal_year || "-";

        const track = document.createElement("div");
        track.className = "trend-track";

        const bar = document.createElement("span");
        bar.className = "trend-bar";
        bar.style.width = `${maxAmount > 0 ? (amount / maxAmount) * 100 : 0}%`;
        track.appendChild(bar);

        const values = document.createElement("div");
        values.className = "trend-values";

        const amountEl = document.createElement("span");
        amountEl.className = "trend-amount";
        amountEl.textContent = formatCompactAmount(amount);
        amountEl.title = formatAmount(amount);

        const families = document.createElement("span");
        families.className = "trend-families";
        families.textContent = `${formatCount(item.family_count)} ${t("dashboard.families")}`;

        values.appendChild(amountEl);
        values.appendChild(families);

        row.appendChild(label);
        row.appendChild(track);
        row.appendChild(values);
        container.appendChild(row);
    });
}

function renderDashboard(snapshot, options = {}) {
    const modeBadge = document.getElementById("dashboard-mode-badge");
    if (!modeBadge) return;

    const data = snapshot || {
        top_contractors: [],
        top_entities: [],
        yearly_spending: [],
    };

    modeBadge.textContent = options.filtered
        ? t("dashboard.modeFiltered")
        : t("dashboard.modeSitewide");
    modeBadge.classList.toggle("is-filtered", Boolean(options.filtered));

    renderDashboardList("dashboard-contractors", data.top_contractors);
    renderDashboardList("dashboard-entities", data.top_entities);
    renderDashboardTrend(data.yearly_spending);
    document.getElementById("dashboard-section").style.display = "";
}

// ── Collect filter values ─────────────────────────────────────────────────

function getFilterValues() {
    return {
        contractNumber: document.getElementById("f-contract-number").value.trim(),
        contractor: document.getElementById("f-contractor").value.trim(),
        entity:     document.getElementById("f-entity").value,
        amountMin:  document.getElementById("f-amount-min").value,
        amountMax:  document.getElementById("f-amount-max").value,
        dateFrom:   document.getElementById("f-date-from").value,
        dateTo:     document.getElementById("f-date-to").value,
        category:   document.getElementById("f-category").value,
        serviceType: document.getElementById("f-service-type").value.trim(),
        validFrom:  document.getElementById("f-valid-from").value,
        validTo:    document.getElementById("f-valid-to").value,
        fiscalYear: document.getElementById("f-fiscal-year").value,
        keyword:    document.getElementById("f-keyword").value.trim(),
    };
}

function clearFilters() {
    document.getElementById("f-contract-number").value = "";
    document.getElementById("f-contractor").value = "";
    document.getElementById("f-entity").value = "";
    document.getElementById("f-amount-min").value = "";
    document.getElementById("f-amount-max").value = "";
    document.getElementById("f-date-from").value = "";
    document.getElementById("f-date-to").value = "";
    document.getElementById("f-category").value = "";
    document.getElementById("f-service-type").value = "";
    document.getElementById("f-valid-from").value = "";
    document.getElementById("f-valid-to").value = "";
    document.getElementById("f-fiscal-year").value = "";
    document.getElementById("f-keyword").value = "";
}

// ── Results table ─────────────────────────────────────────────────────────

function renderResults(rows) {
    const tbody = document.getElementById("results-body");
    tbody.innerHTML = "";

    for (const r of rows) {
        const tr = document.createElement("tr");
        const amendCount =
            Number(r.family_size || 0) ||
            getAmendmentCount(r.contract_number, r.entity, r.contractor, r.id);
        const hasAmendments = amendCount > 1;

        tr.innerHTML = `
            <td>
                <div class="contract-cell">
                    ${hasAmendments ? '<button class="btn-expand" title="' + t("amendments.show") + '">+</button>' : ''}
                    <a href="contract.html?id=${r.id}" class="contract-link">${r.contract_number || "-"}</a>
                    ${r.amendment ? '<span class="amendment-badge">' + r.amendment + '</span>' : ''}
                </div>
            </td>
            <td title="${(r.contractor || "").replace(/"/g, "&quot;")}">${truncate(r.contractor, 35)}</td>
            <td title="${(r.entity || "").replace(/"/g, "&quot;")}">${truncate(r.entity, 30)}</td>
            <td class="amount">${formatAmount(r.amount)}</td>
            <td class="date">${formatDate(r.award_date)}</td>
            <td>${truncate(r.service_category, 25)}</td>
        `;

        tbody.appendChild(tr);

        // Bind expand/collapse for amendments
        if (hasAmendments) {
            const btn = tr.querySelector(".btn-expand");
            let expanded = false;
            let amendmentRows = [];

            btn.addEventListener("click", () => {
                if (expanded) {
                    // Collapse
                    amendmentRows.forEach(ar => ar.remove());
                    amendmentRows = [];
                    btn.textContent = "+";
                    btn.title = t("amendments.show");
                    expanded = false;
                } else {
                    // Expand
                    const amendments = getAmendments(
                        r.contract_number,
                        r.entity,
                        r.contractor,
                        Number(r.family_has_original) ? r.id : null
                    );
                    let insertionPoint = tr;
                    for (const a of amendments) {
                        const atr = document.createElement("tr");
                        atr.className = "amendment-row";
                        atr.innerHTML = `
                            <td class="amendment-indent">
                                <div class="contract-cell">
                                    <a href="contract.html?id=${a.id}" class="contract-link">${r.contract_number}</a>
                                    <span class="amendment-badge">${a.amendment || t("detail.original")}</span>
                                </div>
                            </td>
                            <td colspan="2"></td>
                            <td class="amount">${formatAmount(a.amount)}</td>
                            <td class="date">${formatDate(a.award_date)}</td>
                            <td>${truncate(a.service_type, 25)}</td>
                        `;
                        insertionPoint.after(atr);
                        insertionPoint = atr;
                        amendmentRows.push(atr);
                    }
                    btn.textContent = "\u2212";
                    btn.title = t("amendments.hide");
                    expanded = true;
                }
            });
        }
    }
}

function renderResultsHeader(count, totalAmount) {
    document.getElementById("results-count").textContent =
        `${count.toLocaleString("es")} ${t("results.found")}`;
    document.getElementById("results-amount").textContent =
        `${t("results.total")} ${formatAmount(totalAmount)}`;
}

// ── Pagination ────────────────────────────────────────────────────────────

function renderPagination(currentPage, totalPages, onPageChange) {
    const container = document.getElementById("pagination");
    container.innerHTML = "";

    if (totalPages <= 1) return;

    const addBtn = (label, page, disabled = false, active = false) => {
        const btn = document.createElement("button");
        btn.textContent = label;
        btn.disabled = disabled;
        if (active) btn.classList.add("active");
        if (!disabled && !active) {
            btn.addEventListener("click", () => onPageChange(page));
        }
        container.appendChild(btn);
    };

    addBtn("\u00ab", 1, currentPage === 1);
    addBtn("\u2039", currentPage - 1, currentPage === 1);

    // Show up to 7 page buttons around current
    let start = Math.max(1, currentPage - 3);
    let end = Math.min(totalPages, start + 6);
    if (end - start < 6) start = Math.max(1, end - 6);

    if (start > 1) {
        addBtn("1", 1);
        if (start > 2) {
            const dots = document.createElement("span");
            dots.textContent = "...";
            dots.style.padding = "0 0.3rem";
            container.appendChild(dots);
        }
    }

    for (let i = start; i <= end; i++) {
        addBtn(String(i), i, false, i === currentPage);
    }

    if (end < totalPages) {
        if (end < totalPages - 1) {
            const dots = document.createElement("span");
            dots.textContent = "...";
            dots.style.padding = "0 0.3rem";
            container.appendChild(dots);
        }
        addBtn(String(totalPages), totalPages);
    }

    addBtn("\u203a", currentPage + 1, currentPage === totalPages);
    addBtn("\u00bb", totalPages, currentPage === totalPages);
}

// ── Export panel ───────────────────────────────────────────────────────────

const EXPORT_SCRIPT_LOADERS = new Map();

function setExportResultsState({ visible = false, counts = {} } = {}) {
    exportState.visible = Boolean(visible);
    exportState.counts = {
        summary: Number(counts.summary || 0),
        detailed: Number(counts.detailed || 0),
    };
    updateExportHelper();
}

function setExportBusy(busy) {
    exportState.busy = Boolean(busy);
    updateExportHelper();
}

function getExportSelection() {
    return {
        mode: document.getElementById("export-mode")?.value || "summary",
        format: document.getElementById("export-format")?.value || "csv",
    };
}

function getExportRowLabel(mode) {
    return t(mode === "detailed" ? "export.rows.detailed" : "export.rows.summary");
}

function getExportLimit(mode, format) {
    if (format === "pdf") {
        return EXPORT_LIMITS.pdf[mode] || EXPORT_LIMITS.pdf.summary;
    }
    return EXPORT_LIMITS[format] || EXPORT_LIMITS.csv;
}

function getExportEligibility() {
    const { mode, format } = getExportSelection();
    const count = Number(exportState.counts[mode] || 0);
    const limit = getExportLimit(mode, format);
    return {
        mode,
        format,
        count,
        limit,
        overLimit: count > limit,
        canExport: exportState.visible && count > 0 && count <= limit && !exportState.busy,
    };
}

function updateExportHelper() {
    const panel = document.getElementById("export-panel");
    const helper = document.getElementById("export-helper");
    const button = document.getElementById("btn-export");
    if (!panel || !helper || !button) return;

    panel.style.display = exportState.visible ? "" : "none";
    if (!exportState.visible) {
        helper.textContent = "";
        helper.classList.remove("is-warning");
        button.disabled = true;
        button.textContent = t("btn.export");
        return;
    }

    const { mode, format, count, limit, overLimit } = getExportEligibility();
    const helperPrefix = `${t("export.limitLabel")}: ${formatCount(limit)} ${getExportRowLabel(mode)}.`;
    const helperCount = `${t("export.currentLabel")}: ${formatCount(count)}.`;

    let helperText;
    if (exportState.busy) {
        helperText = `${helperPrefix} ${helperCount} ${t("btn.exporting")}`;
        helper.classList.remove("is-warning");
    } else if (overLimit) {
        helperText = `${helperPrefix} ${helperCount} ${t("export.overLimit")} ${t("export.chooseAnother")}`;
        helper.classList.add("is-warning");
    } else {
        const guidance = format === "pdf"
            ? t("export.pdfFallback")
            : t("export.tableFallback");
        helperText = `${helperPrefix} ${helperCount} ${guidance}`;
        helper.classList.remove("is-warning");
    }

    helper.textContent = helperText;
    button.disabled = !getExportEligibility().canExport;
    button.textContent = exportState.busy ? t("btn.exporting") : t("btn.export");
}

function getSummaryExportColumns(format) {
    if (format === "pdf") {
        return [
            { key: "contract_number", label: t("th.contract") },
            { key: "contractor", label: t("th.contractor") },
            { key: "entity", label: t("th.entity") },
            { key: "amount", label: t("th.amount"), formatter: formatAmount },
            { key: "award_date", label: t("th.date"), formatter: formatDate },
            { key: "valid_from", label: t("detail.validFrom"), formatter: formatDate },
            { key: "valid_to", label: t("detail.validTo"), formatter: formatDate },
            { key: "service_category", label: t("th.category") },
        ];
    }

    return [
        { key: "contract_number", label: t("th.contract") },
        { key: "contractor", label: t("th.contractor") },
        { key: "entity", label: t("th.entity") },
        { key: "amount", label: t("th.amount") },
        { key: "award_date", label: t("th.date") },
        { key: "valid_from", label: t("detail.validFrom") },
        { key: "valid_to", label: t("detail.validTo") },
        { key: "service_category", label: t("th.category") },
        { key: "service_type", label: t("detail.serviceType") },
        { key: "fiscal_year", label: t("detail.fiscalYear") },
        { key: "family_size", label: t("export.familySize") },
        { key: "family_total_amount", label: t("export.familyTotalAmount") },
        { key: "procurement_method", label: t("detail.procurementMethod") },
        { key: "fund_type", label: t("detail.fundType") },
    ];
}

function getDetailedExportColumns(format) {
    if (format === "pdf") {
        return [
            { key: "contract_number", label: t("th.contract") },
            { key: "amendment", label: t("detail.amendment") },
            { key: "contractor", label: t("th.contractor") },
            { key: "entity", label: t("th.entity") },
            { key: "amount", label: t("th.amount"), formatter: formatAmount },
            { key: "award_date", label: t("th.date"), formatter: formatDate },
            { key: "valid_from", label: t("detail.validFrom"), formatter: formatDate },
            { key: "valid_to", label: t("detail.validTo"), formatter: formatDate },
            { key: "service_type", label: t("detail.serviceType") },
        ];
    }

    return [
        { key: "contract_number", label: t("th.contract") },
        { key: "amendment", label: t("detail.amendment") },
        { key: "contractor", label: t("th.contractor") },
        { key: "entity", label: t("th.entity") },
        { key: "entity_number", label: t("detail.entityNumber") },
        { key: "amount", label: t("th.amount") },
        { key: "amount_receivable", label: t("detail.amountReceivable") },
        { key: "award_date", label: t("th.date") },
        { key: "valid_from", label: t("detail.validFrom") },
        { key: "valid_to", label: t("detail.validTo") },
        { key: "service_category", label: t("th.category") },
        { key: "service_type", label: t("detail.serviceType") },
        { key: "fiscal_year", label: t("detail.fiscalYear") },
        { key: "procurement_method", label: t("detail.procurementMethod") },
        { key: "fund_type", label: t("detail.fundType") },
        { key: "pco_number", label: t("detail.pcoNumber") },
        { key: "cancelled", label: t("detail.cancelled") },
        { key: "document_url", label: "document_url" },
    ];
}

function getExportColumns(mode, format) {
    return mode === "detailed"
        ? getDetailedExportColumns(format)
        : getSummaryExportColumns(format);
}

function getExportCellValue(row, column, format) {
    const value = row[column.key];
    if (format === "pdf" && typeof column.formatter === "function") {
        return column.formatter(value);
    }
    return value == null ? "" : value;
}

function buildExportQuery(filters, mode, limit) {
    if (mode === "detailed") {
        return buildDetailedQuery(filters, 1, "award_date", "DESC", { limit, offset: 0 });
    }
    return buildSearchQuery(filters, 1, "award_date", "DESC", { limit, offset: 0 });
}

function buildExportFilename(mode, format) {
    const datePart = new Date().toISOString().slice(0, 10);
    return `ocpr_contracts_${mode}_${datePart}.${format}`;
}

function downloadBlob(blob, filename) {
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = filename;
    link.click();
    URL.revokeObjectURL(url);
}

function escapeCsvValue(value) {
    let formatted = value == null ? "" : String(value);
    if (formatted.includes(",") || formatted.includes('"') || formatted.includes("\n")) {
        formatted = '"' + formatted.replace(/"/g, '""') + '"';
    }
    return formatted;
}

function exportCsvFile(filename, columns, rows) {
    const csvLines = [columns.map(column => escapeCsvValue(column.label)).join(",")];
    for (const row of rows) {
        csvLines.push(
            columns.map(column => escapeCsvValue(getExportCellValue(row, column, "csv"))).join(",")
        );
    }

    downloadBlob(
        new Blob([csvLines.join("\n")], { type: "text/csv;charset=utf-8;" }),
        filename
    );
}

function loadScriptOnce(src, globalCheck) {
    if (globalCheck()) return Promise.resolve();
    if (EXPORT_SCRIPT_LOADERS.has(src)) return EXPORT_SCRIPT_LOADERS.get(src);

    const promise = new Promise((resolve, reject) => {
        const script = document.createElement("script");
        script.src = src;
        script.async = true;
        script.onload = () => {
            if (globalCheck()) {
                resolve();
            } else {
                reject(new Error(`Failed to initialize ${src}`));
            }
        };
        script.onerror = () => reject(new Error(`Failed to load ${src}`));
        document.head.appendChild(script);
    }).catch(err => {
        EXPORT_SCRIPT_LOADERS.delete(src);
        throw err;
    });

    EXPORT_SCRIPT_LOADERS.set(src, promise);
    return promise;
}

async function ensureXlsx() {
    await loadScriptOnce(
        "https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js",
        () => Boolean(window.XLSX)
    );
    return window.XLSX;
}

async function ensureJsPdf() {
    await loadScriptOnce(
        "https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js",
        () => Boolean(window.jspdf && window.jspdf.jsPDF)
    );
    await loadScriptOnce(
        "https://cdnjs.cloudflare.com/ajax/libs/jspdf-autotable/3.8.2/jspdf.plugin.autotable.min.js",
        () => Boolean(window.jspdf?.jsPDF?.API?.autoTable)
    );
    return window.jspdf.jsPDF;
}

async function exportXlsxFile(filename, columns, rows) {
    const XLSX = await ensureXlsx();
    const aoa = [
        columns.map(column => column.label),
        ...rows.map(row => columns.map(column => getExportCellValue(row, column, "xlsx"))),
    ];
    const worksheet = XLSX.utils.aoa_to_sheet(aoa);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, "Contracts");

    const buffer = XLSX.write(workbook, { bookType: "xlsx", type: "array" });
    downloadBlob(
        new Blob(
            [buffer],
            { type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" }
        ),
        filename
    );
}

async function exportPdfFile(filename, title, columns, rows) {
    const JsPdf = await ensureJsPdf();
    const doc = new JsPdf({
        orientation: "landscape",
        unit: "pt",
        format: "letter",
    });

    if (typeof doc.autoTable !== "function") {
        throw new Error("jsPDF autoTable plugin is not available");
    }

    doc.setFontSize(14);
    doc.text(title, 32, 34);
    doc.setFontSize(9);
    doc.text(new Date().toISOString().slice(0, 10), 32, 50);
    doc.autoTable({
        startY: 62,
        head: [columns.map(column => column.label)],
        body: rows.map(row => columns.map(column => {
            const value = getExportCellValue(row, column, "pdf");
            return value == null || value === "" ? "-" : String(value);
        })),
        margin: { top: 24, right: 24, bottom: 24, left: 24 },
        styles: { fontSize: 7, cellPadding: 4, overflow: "linebreak" },
        headStyles: { fillColor: [13, 110, 253] },
    });
    doc.save(filename);
}

async function exportResults(filters) {
    const eligibility = getExportEligibility();
    if (!eligibility.canExport) {
        updateExportHelper();
        return;
    }

    setExportBusy(true);
    try {
        const queryDef = buildExportQuery(filters, eligibility.mode, eligibility.limit);
        const rows = query(queryDef.dataSql, queryDef.params);
        if (rows.length === 0) {
            return;
        }

        const columns = getExportColumns(eligibility.mode, eligibility.format);
        const filename = buildExportFilename(eligibility.mode, eligibility.format);

        if (eligibility.format === "csv") {
            exportCsvFile(filename, columns, rows);
        } else if (eligibility.format === "xlsx") {
            await exportXlsxFile(filename, columns, rows);
        } else {
            const title = `${t("title")} - ${t(
                eligibility.mode === "detailed"
                    ? "export.mode.detailed"
                    : "export.mode.summary"
            )}`;
            await exportPdfFile(filename, title, columns, rows);
        }
    } catch (err) {
        console.error("Export failed:", err);
        window.alert(t("export.error"));
    } finally {
        setExportBusy(false);
    }
}

// ── Show/hide sections ────────────────────────────────────────────────────

function showResults(hasResults) {
    document.getElementById("results-section").style.display = hasResults ? "" : "none";
    document.getElementById("no-results").style.display = hasResults ? "none" : "";
}

function hideLoading() {
    document.getElementById("loading").style.display = "none";
}

function setLoadingStatus(msg) {
    document.getElementById("loading-status").textContent = msg;
}
