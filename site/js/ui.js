/**
 * ui.js — DOM rendering: filters, results table, pagination, export.
 */

const DASHBOARD_PREF_KEY = "ocpr-dashboard-collapsed";
const DASHBOARD_MOBILE_MEDIA = "(max-width: 640px)";

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

function populateFilters() {
    populateDropdown("f-entity", getDistinct("entity"));
    populateDropdown("f-category", getDistinct("service_category"));
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

// ── CSV Export ─────────────────────────────────────────────────────────────

function exportCSV(filters) {
    const { dataSql, params } = buildSearchQuery(filters, 1, "award_date", "DESC");
    // Remove LIMIT/OFFSET for full export
    const fullSql = dataSql.replace(/LIMIT \d+ OFFSET \d+/, "LIMIT 100000");
    const rows = query(fullSql, params);

    if (rows.length === 0) return;

    const headers = ["contract_number", "contractor", "entity", "amount",
                     "award_date", "valid_from", "valid_to", "service_category",
                     "service_type", "fiscal_year", "procurement_method", "fund_type"];

    const csvLines = [headers.join(",")];
    for (const r of rows) {
        const line = headers.map(h => {
            let val = r[h] == null ? "" : String(r[h]);
            if (val.includes(",") || val.includes('"') || val.includes("\n")) {
                val = '"' + val.replace(/"/g, '""') + '"';
            }
            return val;
        });
        csvLines.push(line.join(","));
    }

    const blob = new Blob([csvLines.join("\n")], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "contratos_ocpr.csv";
    a.click();
    URL.revokeObjectURL(url);
}

// ── Show/hide sections ────────────────────────────────────────────────────

function showResults(hasResults) {
    document.getElementById("results-section").style.display = hasResults ? "" : "none";
    document.getElementById("no-results").style.display = hasResults ? "none" : "";
    document.getElementById("btn-export").style.display = hasResults ? "" : "none";
}

function hideLoading() {
    document.getElementById("loading").style.display = "none";
}

function setLoadingStatus(msg) {
    document.getElementById("loading-status").textContent = msg;
}
