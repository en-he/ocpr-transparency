/**
 * contract.js — Contract detail page logic.
 * Reads ?id= parameter, loads DB, renders full contract details + amendments.
 */

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

function setField(id, val) {
    document.getElementById(id).textContent = val || "-";
}

function setLinkField(id, url) {
    const link = document.getElementById(id);
    if (!link) return;
    if (url) {
        link.href = url;
        link.textContent = url;
        link.style.pointerEvents = "";
    } else {
        link.removeAttribute("href");
        link.textContent = "-";
        link.style.pointerEvents = "none";
    }
}

function formatSourceType(value) {
    if (!value) return "-";
    const key = `detail.sourceType.${value}`;
    const translated = t(key);
    return translated === key ? value : translated;
}

function formatRecoveryStatus(value) {
    if (!value) return "-";
    const key = `detail.recoveryStatus.${value}`;
    const translated = t(key);
    return translated === key ? value : translated;
}

async function initDetail() {
    try {
        initLang();

        const params = new URLSearchParams(window.location.search);
        const contractId = parseInt(params.get("id"), 10);
        const familyContractNumber = (params.get("contract_number") || "").trim();
        const familyEntity = (params.get("entity") || "").trim();
        const familyContractor = (params.get("contractor") || "").trim();
        const backLink = document.getElementById("back-to-search");
        if (backLink) {
            backLink.href = buildSearchUrl();
        }

        if (!contractId && (!familyContractNumber || !familyEntity || !familyContractor)) {
            document.getElementById("loading").style.display = "none";
            document.getElementById("contract-not-found").style.display = "";
            return;
        }

        await initDB(msg => {
            document.getElementById("loading-status").textContent = msg;
        });

        let contract = null;
        let recoveryTarget = null;

        if (contractId) {
            contract = getContractById(contractId);
            if (contract) {
                recoveryTarget = getRecoveryTarget(
                    contract.contract_number,
                    contract.entity,
                    contract.contractor
                );
            }
        } else {
            const resolved = resolveContractFamilyDetail(
                familyContractNumber,
                familyEntity,
                familyContractor
            );
            contract = resolved.contract;
            recoveryTarget = resolved.recoveryTarget;
        }

        document.getElementById("loading").style.display = "none";

        if (!contract) {
            document.getElementById("contract-not-found").style.display = "";
            return;
        }

        if (recoveryTarget) {
            contract = {
                ...contract,
                recovery_status: contract.recovery_status || recoveryTarget.status || null,
                recovery_notes: contract.recovery_notes || recoveryTarget.notes || null,
                recovery_lookup_mode: contract.recovery_lookup_mode || recoveryTarget.lookup_mode || null,
            };
        }

        renderContract(contract);
        renderAmendments(contract);

        // Bind language toggle
        document.getElementById("lang-toggle").addEventListener("click", () => {
            setLang(getLang() === "es" ? "en" : "es");
            renderContract(contract);
            renderAmendments(contract);
        });

    } catch (err) {
        document.getElementById("loading-status").textContent = "Error: " + err.message;
        console.error("Detail init failed:", err);
    }
}

function renderContract(c) {
    document.getElementById("contract-detail").style.display = "";

    // Title
    const titleParts = [c.contract_number || t("detail.untitled")];
    if (c.amendment) titleParts.push(`(${t("detail.amendment")}: ${c.amendment})`);
    document.getElementById("detail-title").textContent = titleParts.join(" ");

    // Cancelled badge
    const badge = document.getElementById("detail-status");
    if (c.cancelled) {
        badge.textContent = t("detail.cancelled");
        badge.className = "badge badge-danger";
        badge.style.display = "";
    } else {
        badge.style.display = "none";
    }

    // Contract info
    setField("d-contract-number", c.contract_number);
    setField("d-amendment", c.amendment);
    setField("d-entity", c.entity);
    setField("d-entity-number", c.entity_number);
    setField("d-contractor", c.contractor);
    setField("d-fiscal-year", c.fiscal_year);

    // Financial
    document.getElementById("d-amount").textContent = formatAmount(c.amount);
    document.getElementById("d-amount-receivable").textContent = formatAmount(c.amount_receivable);
    setField("d-fund-type", c.fund_type);

    // Dates
    setField("d-award-date", formatDate(c.award_date));
    setField("d-valid-from", formatDate(c.valid_from));
    setField("d-valid-to", formatDate(c.valid_to));

    // Service
    setField("d-service-category", c.service_category);
    setField("d-service-type", c.service_type);
    setField("d-procurement-method", c.procurement_method);
    setField("d-pco-number", c.pco_number);
    setField("d-source-type", formatSourceType(c.source_type));
    setField("d-source-contract-id", c.source_contract_id);
    setLinkField("d-source-url", c.source_url);
    setField("d-recovery-status", formatRecoveryStatus(c.recovery_status));
    setField("d-recovery-notes", c.recovery_notes);

    // Page title
    document.title = `${c.contract_number || "Contrato"} — ${c.entity || "OCPR"}`;
}

function renderAmendments(c) {
    if (!c.contract_number || !c.entity || !c.contractor) return;

    const amendments = getAmendments(c.contract_number, c.entity, c.contractor, c.id);

    const section = document.getElementById("amendments-section");
    if (amendments.length === 0) {
        section.style.display = "none";
        return;
    }

    section.style.display = "";
    const tbody = document.getElementById("amendments-body");
    tbody.innerHTML = "";

    for (const a of amendments) {
        const amendmentUrl = buildContractUrl(a.id);
        const tr = document.createElement("tr");
        tr.style.cursor = "pointer";
        tr.addEventListener("click", () => {
            window.location.href = amendmentUrl;
        });
        tr.innerHTML = `
            <td><a href="${amendmentUrl}">${a.amendment || t("detail.original")}</a></td>
            <td class="amount">${formatAmount(a.amount)}</td>
            <td class="date">${formatDate(a.award_date)}</td>
            <td class="date">${formatDate(a.valid_from)}</td>
            <td class="date">${formatDate(a.valid_to)}</td>
            <td>${a.service_type || "-"}</td>
        `;
        tbody.appendChild(tr);
    }
}

document.addEventListener("DOMContentLoaded", initDetail);
