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
        "filter.serviceType":   "Tipo de Servicio",
        "filter.validFrom":     "Vigencia desde",
        "filter.validTo":       "Vigencia hasta",
        "filter.keyword":       "B\u00fasqueda libre",

        // Filter placeholders
        "ph.contractNumber":  "2022-000019",
        "ph.contractor":    "Nombre del contratista... (use * como comod\u00edn)",
        "ph.entity":        "Escriba o seleccione una entidad",
        "ph.amountMin":     "0",
        "ph.amountMax":     "Sin l\u00edmite",
        "ph.serviceType":   "Escriba o seleccione un tipo de servicio",
        "ph.keyword":       "Buscar en todos los campos...",

        // Filter defaults
        "all.f":            "Todas",
        "all.m":            "Todos",

        // Buttons
        "btn.search":       "Buscar",
        "btn.clear":        "Limpiar filtros",
        "btn.export":       "Exportar",
        "btn.exporting":    "Preparando...",

        // Export panel
        "export.mode":              "Modo de exportaci\u00f3n",
        "export.format":            "Formato",
        "export.mode.summary":      "Resumen",
        "export.mode.detailed":     "Detallado",
        "export.format.csv":        "CSV",
        "export.format.xlsx":       "Excel (.xlsx)",
        "export.format.pdf":        "PDF",
        "export.limitLabel":        "L\u00edmite",
        "export.currentLabel":      "Selecci\u00f3n actual",
        "export.rows.summary":      "filas agrupadas",
        "export.rows.detailed":     "filas de contratos",
        "export.overLimit":         "La selecci\u00f3n supera este l\u00edmite.",
        "export.chooseAnother":     "Aplique m\u00e1s filtros o cambie de formato.",
        "export.pdfFallback":       "PDF funciona mejor para reportes compactos; use Excel o CSV para conjuntos grandes.",
        "export.tableFallback":     "Excel y CSV permiten exportaciones m\u00e1s grandes, hasta 100,000 filas.",
        "export.error":             "No se pudo generar la exportaci\u00f3n.",
        "export.familySize":        "Filas en la familia",
        "export.familyTotalAmount": "Valor total de la familia",

        // Results
        "results.found":    "contrato(s) encontrado(s)",
        "results.total":    "Valor total:",

        // Sorting
        "sort.order":       "Orden",
        "sort.ascending":   "Ascendente",
        "sort.descending":  "Descendente",
        "sort.changeOrder": "Cambiar orden",

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
        "filter.serviceType":   "Service Type",
        "filter.validFrom":     "Valid from",
        "filter.validTo":       "Valid to",
        "filter.keyword":       "Free search",

        // Filter placeholders
        "ph.contractNumber":  "2022-000019",
        "ph.contractor":    "Contractor name... (use * as wildcard)",
        "ph.entity":        "Type or select an entity",
        "ph.amountMin":     "0",
        "ph.amountMax":     "No limit",
        "ph.serviceType":   "Type or select a service type",
        "ph.keyword":       "Search all fields...",

        // Filter defaults
        "all.f":            "All",
        "all.m":            "All",

        // Buttons
        "btn.search":       "Search",
        "btn.clear":        "Clear filters",
        "btn.export":       "Export",
        "btn.exporting":    "Preparing...",

        // Export panel
        "export.mode":              "Export mode",
        "export.format":            "Format",
        "export.mode.summary":      "Summary",
        "export.mode.detailed":     "Detailed",
        "export.format.csv":        "CSV",
        "export.format.xlsx":       "Excel (.xlsx)",
        "export.format.pdf":        "PDF",
        "export.limitLabel":        "Limit",
        "export.currentLabel":      "Current selection",
        "export.rows.summary":      "grouped rows",
        "export.rows.detailed":     "contract rows",
        "export.overLimit":         "This selection is over the limit.",
        "export.chooseAnother":     "Narrow the filters or switch formats.",
        "export.pdfFallback":       "PDF works best for compact reports; use Excel or CSV for larger result sets.",
        "export.tableFallback":     "Excel and CSV support larger exports, up to 100,000 rows.",
        "export.error":             "The export could not be generated.",
        "export.familySize":        "Rows in family",
        "export.familyTotalAmount": "Family total amount",

        // Results
        "results.found":    "contract(s) found",
        "results.total":    "Total value:",

        // Sorting
        "sort.order":       "Order",
        "sort.ascending":   "Ascending",
        "sort.descending":  "Descending",
        "sort.changeOrder": "Change sort order",

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
        const sortLabel = el.querySelector(".sort-header-label");
        if (sortLabel) {
            sortLabel.textContent = t(el.dataset.i18n);
        } else {
            el.textContent = t(el.dataset.i18n);
        }
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
