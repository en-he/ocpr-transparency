"""
Shared configuration for the OCPR contract data pipeline.
"""
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────
RAW_DIR = Path("data/raw")
DB_PATH = Path("data/db/contratos.db")
STATE_FILE = Path("data/db/.monitor_state.json")

# ── OCPR endpoints ─────────────────────────────────────────────────────────
BASE_URL = "https://consultacontratos.ocpr.gov.pr"
DOWNLOAD_PATH = "/contract/downloadfrequentsearchfiscalyeardocument"
SEARCH_URL = f"{BASE_URL}/contract/search"

# ── Fiscal years ───────────────────────────────────────────────────────────
# All years listed on the site.  Known 404s: 2023-2024, 2011-2012, 2010-2011
ALL_FISCAL_YEARS = [
    "2023-2024", "2022-2023", "2021-2022", "2020-2021",
    "2019-2020", "2018-2019", "2017-2018", "2016-2017",
    "2015-2016", "2014-2015", "2013-2014", "2012-2013",
    "2011-2012", "2010-2011",
]

# ── HTTP ───────────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "ocpr-transparency/1.0 "
        "(public accountability project; github.com/ocpr-transparency)"
    ),
    "Accept": "text/csv, application/octet-stream, */*",
    "Referer": f"{BASE_URL}/",
}

# ── Column mapping ─────────────────────────────────────────────────────────
# Keys are canonical internal names; values are known CSV header variants.
# Validated against actual 2012-2023 exports.
COLUMN_MAP = {
    "contract_number":    ["Núm. Contrato", "Num. Contrato", "Numero Contrato", "NumContrato"],
    "entity":             ["Entidad", "Entidad Gubernamental"],
    "entity_number":      ["Número de Entidad", "Numero de Entidad"],
    "contractor":         ["Contratista"],
    "amendment":          ["Enmienda"],
    "service_category":   ["Categoría de Servicio", "Categoria de Servicio", "Categoria"],
    "service_type":       ["Tipo de Servicio"],
    "amount":             ["Cuantía", "Cuantia", "Monto"],
    "amount_receivable":  ["Cuantía a Recibir", "Cuantia a Recibir"],
    "award_date":         ["Otorgado en", "Fecha de Otorgamiento", "Fecha Otorgamiento"],
    "valid_from":         ["Vigencia Desde", "Fecha Vigencia Desde", "Inicio Vigencia"],
    "valid_to":           ["Vigencia Hasta", "Fecha Vigencia Hasta", "Fin Vigencia"],
    "procurement_method": ["Forma de Contratación", "Forma de Contratacion"],
    "fund_type":          ["Fondo"],
    "pco_number":         ["Número PCo", "Numero PCo", "PCo"],
    "cancelled":          ["Cancelado"],
    "document_url":       ["Documento", "URL Documento", "Enlace Documento"],
}
