/**
 * i18n.js — Lightweight bilingual support (ES/EN).
 * Loaded before other JS files. Uses data-i18n attributes on DOM elements.
 */

const STRINGS = {
    es: {
        // Header
        "title":            "Contratos del Gobierno de Puerto Rico",
        "subtitle":         "Datos abiertos de la Oficina del Contralor",

        // Loading
        "loading":          "Cargando base de datos...",
        "loading.download": "Descargando contratos",

        // Stats
        "stats.contracts":  "contratos",
        "stats.total":      "valor total",

        // Filter labels
        "filter.contractor":    "Contratista",
        "filter.entity":        "Entidad Gubernamental",
        "filter.amountMin":     "Cuant\u00eda m\u00ednima ($)",
        "filter.amountMax":     "Cuant\u00eda m\u00e1xima ($)",
        "filter.dateFrom":      "Fecha desde",
        "filter.dateTo":        "Fecha hasta",
        "filter.category":      "Categor\u00eda de Servicio",
        "filter.fiscalYear":    "A\u00f1o Fiscal",
        "filter.keyword":       "B\u00fasqueda libre",

        // Filter placeholders
        "ph.contractor":    "Nombre del contratista... (use * como comod\u00edn)",
        "ph.amountMin":     "0",
        "ph.amountMax":     "Sin l\u00edmite",
        "ph.keyword":       "Buscar en todos los campos...",

        // Filter defaults
        "all.f":            "Todas",
        "all.m":            "Todos",

        // Buttons
        "btn.search":       "Buscar",
        "btn.clear":        "Limpiar filtros",
        "btn.export":       "Exportar CSV",

        // Results
        "results.found":    "contrato(s) encontrado(s)",
        "results.total":    "Valor total:",

        // Table headers
        "th.contract":      "Contrato",
        "th.contractor":    "Contratista",
        "th.entity":        "Entidad",
        "th.amount":        "Cuant\u00eda",
        "th.date":          "Fecha",
        "th.category":      "Categor\u00eda",

        // No results
        "noResults":        "No se encontraron contratos con los filtros seleccionados.",

        // Footer
        "footer.source":    "Fuente:",
        "footer.ocpr":      "Oficina del Contralor de Puerto Rico",
        "footer.openData":  "Datos abiertos",
    },
    en: {
        // Header
        "title":            "Puerto Rico Government Contracts",
        "subtitle":         "Open data from the Office of the Comptroller",

        // Loading
        "loading":          "Loading database...",
        "loading.download": "Downloading contracts",

        // Stats
        "stats.contracts":  "contracts",
        "stats.total":      "total value",

        // Filter labels
        "filter.contractor":    "Contractor",
        "filter.entity":        "Government Entity",
        "filter.amountMin":     "Minimum amount ($)",
        "filter.amountMax":     "Maximum amount ($)",
        "filter.dateFrom":      "Date from",
        "filter.dateTo":        "Date to",
        "filter.category":      "Service Category",
        "filter.fiscalYear":    "Fiscal Year",
        "filter.keyword":       "Free search",

        // Filter placeholders
        "ph.contractor":    "Contractor name... (use * as wildcard)",
        "ph.amountMin":     "0",
        "ph.amountMax":     "No limit",
        "ph.keyword":       "Search all fields...",

        // Filter defaults
        "all.f":            "All",
        "all.m":            "All",

        // Buttons
        "btn.search":       "Search",
        "btn.clear":        "Clear filters",
        "btn.export":       "Export CSV",

        // Results
        "results.found":    "contract(s) found",
        "results.total":    "Total value:",

        // Table headers
        "th.contract":      "Contract",
        "th.contractor":    "Contractor",
        "th.entity":        "Entity",
        "th.amount":        "Amount",
        "th.date":          "Date",
        "th.category":      "Category",

        // No results
        "noResults":        "No contracts found with the selected filters.",

        // Footer
        "footer.source":    "Source:",
        "footer.ocpr":      "Office of the Comptroller of Puerto Rico",
        "footer.openData":  "Open data",
    }
};

let _lang = "es";

function getLang() {
    return _lang;
}

function t(key) {
    return (STRINGS[_lang] && STRINGS[_lang][key]) || (STRINGS.es[key]) || key;
}

function setLang(lang) {
    _lang = lang;
    localStorage.setItem("ocpr-lang", lang);
    applyLang();
}

function applyLang() {
    // Update all elements with data-i18n attribute (text content)
    document.querySelectorAll("[data-i18n]").forEach(el => {
        el.textContent = t(el.dataset.i18n);
    });

    // Update all elements with data-i18n-ph attribute (placeholders)
    document.querySelectorAll("[data-i18n-ph]").forEach(el => {
        el.placeholder = t(el.dataset.i18nPh);
    });

    // Update html lang attribute
    document.documentElement.lang = _lang;

    // Update toggle button text
    const toggle = document.getElementById("lang-toggle");
    if (toggle) {
        toggle.textContent = _lang === "es" ? "EN" : "ES";
    }

    // Update page title
    document.title = _lang === "es"
        ? "OCPR Contratos - Transparencia"
        : "OCPR Contracts - Transparency";
}

function initLang() {
    _lang = localStorage.getItem("ocpr-lang") || "es";
    applyLang();
}
