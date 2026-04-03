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

        // Dashboard
        "dashboard.kicker":       "Instantánea de fiscalización",
        "dashboard.title":        "Quién recibe y quién gasta",
        "dashboard.summary":      "Vea los principales contratistas, las entidades con más gasto y la tendencia anual para orientar la búsqueda.",
        "dashboard.hide":         "Ocultar",
        "dashboard.show":         "Mostrar",
        "dashboard.modeSitewide": "Vista general",
        "dashboard.modeFiltered": "Vista filtrada",
        "dashboard.topContractors": "Principales contratistas",
        "dashboard.topEntities":  "Principales entidades",
        "dashboard.yearlyTrend":  "Tendencia anual del gasto",
        "dashboard.families":     "familias de contratos",
        "dashboard.empty":        "No hay datos para esta vista.",

        // Filter labels
        "filter.contractNumber":  "Número de Contrato",
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
        "ph.contractNumber":  "2022-000019",
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
        "downloads.kicker":         "Descargas y archivos",
        "downloads.title":          "Datos abiertos preservados",
        "downloads.description":    "Descargue la base de datos completa o use las copias archivadas por a\u00f1o fiscal preservadas en este repositorio.",
        "downloads.fullDescription": "La descarga principal para an\u00e1lisis completos, sin el l\u00edmite del navegador.",
        "downloads.archiveTitle":   "CSVs archivados por a\u00f1o fiscal",
        "downloads.archiveDescription": "Estas copias preservadas mantienen accesibles los a\u00f1os m\u00e1s viejos aunque el portal oficial ya no ofrezca esa descarga masiva.",
        "downloads.file":           "Archivo",
        "downloads.download":       "Descargar",
        "downloads.fullDatabase":   "Base de Datos Completa",
        "downloads.sqlite":         "Descargar SQLite",
        "downloads.archivedCsv":    "Descargar CSV archivado",
        "downloads.archiveBadge":   "Copia preservada",
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

        // Dashboard
        "dashboard.kicker":       "Accountability snapshot",
        "dashboard.title":        "Who gets paid and who spends",
        "dashboard.summary":      "See the top contractors, the biggest public entities, and the yearly spending trend before diving into search.",
        "dashboard.hide":         "Hide",
        "dashboard.show":         "Show",
        "dashboard.modeSitewide": "Sitewide snapshot",
        "dashboard.modeFiltered": "Filtered snapshot",
        "dashboard.topContractors": "Top contractors",
        "dashboard.topEntities":  "Top entities",
        "dashboard.yearlyTrend":  "Yearly spending trend",
        "dashboard.families":     "contract families",
        "dashboard.empty":        "No data for this view.",

        // Filter labels
        "filter.contractNumber":  "Contract Number",
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
        "ph.contractNumber":  "2022-000019",
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
        "downloads.kicker":         "Downloads and archives",
        "downloads.title":          "Preserved open data",
        "downloads.description":    "Download the full database or use the fiscal-year archive copies preserved in this repository.",
        "downloads.fullDescription": "The main download for full analysis without browser-size constraints.",
        "downloads.archiveTitle":   "Archived fiscal-year CSVs",
        "downloads.archiveDescription": "These preserved copies keep older years accessible even when the official portal no longer offers that bulk download.",
        "downloads.file":           "File",
        "downloads.download":       "Download",
        "downloads.fullDatabase":   "Full Database",
        "downloads.sqlite":         "Download SQLite",
        "downloads.archivedCsv":    "Download archived CSV",
        "downloads.archiveBadge":   "Preserved copy",
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
