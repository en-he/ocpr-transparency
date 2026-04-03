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

        // Detail page
        "detail.back":              "\u2190 Volver a la b\u00fasqueda",
        "detail.print":             "Imprimir",
        "detail.notFound":          "Contrato no encontrado.",
        "detail.contractInfo":      "Informaci\u00f3n del Contrato",
        "detail.contractNumber":    "N\u00famero de Contrato",
        "detail.amendment":         "Enmienda",
        "detail.entity":            "Entidad",
        "detail.entityNumber":      "N\u00famero de Entidad",
        "detail.contractor":        "Contratista",
        "detail.fiscalYear":        "A\u00f1o Fiscal",
        "detail.financial":         "Informaci\u00f3n Financiera",
        "detail.amount":            "Cuant\u00eda a Pagar",
        "detail.amountReceivable":  "Cuant\u00eda a Recibir",
        "detail.fundType":          "Tipo de Fondos",
        "detail.dates":             "Fechas",
        "detail.awardDate":         "Fecha Otorgado",
        "detail.validFrom":         "Vigencia Desde",
        "detail.validTo":           "Vigencia Hasta",
        "detail.service":           "Servicio",
        "detail.serviceCategory":   "Categor\u00eda de Servicio",
        "detail.serviceType":       "Tipo de Servicio",
        "detail.procurementMethod": "Forma de Contrataci\u00f3n",
        "detail.pcoNumber":         "N\u00famero PCo",
        "detail.amendments":        "Enmiendas de este Contrato",
        "detail.original":          "Original",
        "detail.cancelled":         "Cancelado",
        "detail.untitled":          "Sin n\u00famero",
        "detail.viewDetail":        "Ver detalle",

        // Amendments in results
        "amendments.show":          "Ver enmiendas",
        "amendments.hide":          "Ocultar enmiendas",
        "amendments.count":         "enmienda(s)",

        // Downloads section
        "downloads.title":          "Descargas por A\u00f1o Fiscal",
        "downloads.description":    "Archivos CSV archivados de la Oficina del Contralor. Algunos a\u00f1os fiscales pueden ya no estar disponibles en el sitio oficial.",
        "downloads.file":           "Archivo",
        "downloads.download":       "Descargar",
        "downloads.fullDatabase":   "Base de Datos Completa",
        "downloads.sqlite":         "Descargar SQLite",
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

        // Detail page
        "detail.back":              "\u2190 Back to search",
        "detail.print":             "Print",
        "detail.notFound":          "Contract not found.",
        "detail.contractInfo":      "Contract Information",
        "detail.contractNumber":    "Contract Number",
        "detail.amendment":         "Amendment",
        "detail.entity":            "Entity",
        "detail.entityNumber":      "Entity Number",
        "detail.contractor":        "Contractor",
        "detail.fiscalYear":        "Fiscal Year",
        "detail.financial":         "Financial Information",
        "detail.amount":            "Amount Payable",
        "detail.amountReceivable":  "Amount Receivable",
        "detail.fundType":          "Fund Type",
        "detail.dates":             "Dates",
        "detail.awardDate":         "Award Date",
        "detail.validFrom":         "Valid From",
        "detail.validTo":           "Valid To",
        "detail.service":           "Service",
        "detail.serviceCategory":   "Service Category",
        "detail.serviceType":       "Service Type",
        "detail.procurementMethod": "Procurement Method",
        "detail.pcoNumber":         "PCo Number",
        "detail.amendments":        "Amendments for this Contract",
        "detail.original":          "Original",
        "detail.cancelled":         "Cancelled",
        "detail.untitled":          "No number",
        "detail.viewDetail":        "View detail",

        // Amendments in results
        "amendments.show":          "Show amendments",
        "amendments.hide":          "Hide amendments",
        "amendments.count":         "amendment(s)",

        // Downloads section
        "downloads.title":          "Downloads by Fiscal Year",
        "downloads.description":    "Archived CSV files from the Office of the Comptroller. Some fiscal years may no longer be available on the official site.",
        "downloads.file":           "File",
        "downloads.download":       "Download",
        "downloads.fullDatabase":   "Full Database",
        "downloads.sqlite":         "Download SQLite",
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
