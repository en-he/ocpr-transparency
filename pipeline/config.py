"""
Shared configuration for the OCPR contract data pipeline.
"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from contract_utils import PR_TIMEZONE

# ── Paths ──────────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parent.parent

RAW_DIR    = REPO_ROOT / "data" / "raw"
DB_PATH    = REPO_ROOT / "data" / "db" / "contratos.db"
STATE_FILE = REPO_ROOT / "data" / "db" / "monitor_state.json"

# ── OCPR endpoints ─────────────────────────────────────────────────────────
BASE_URL = "https://consultacontratos.ocpr.gov.pr"
DOWNLOAD_PATH = "/contract/downloadfrequentsearchfiscalyeardocument"
SEARCH_URL = f"{BASE_URL}/contract/search"

# ── Fiscal years ───────────────────────────────────────────────────────────
BULK_CSV_START_YEAR = 2010

# These older exports were recovered from archive.org and are now preserved in-repo.
ARCHIVED_ONLY_FISCAL_YEARS = {
    "2011-2012",
    "2010-2011",
}

# As of April 4, 2026, the live portal still exposes this year in the UI but returns 404.
KNOWN_LIVE_404_YEARS = {
    "2023-2024",
}


def format_fiscal_year(start_year: int) -> str:
    return f"{start_year:04d}-{start_year + 1:04d}"


def parse_fiscal_year(value: str) -> tuple[int, int]:
    start, end = value.split("-", 1)
    return int(start), int(end)


def current_fiscal_year(today: date | None = None) -> str:
    if today is None:
        today = datetime.now(PR_TIMEZONE).date()
    start_year = today.year if today.month >= 7 else today.year - 1
    return format_fiscal_year(start_year)


def bulk_csv_years_through_current(today: date | None = None) -> list[str]:
    current_start, _ = parse_fiscal_year(current_fiscal_year(today))
    return [
        format_fiscal_year(start_year)
        for start_year in range(current_start, BULK_CSV_START_YEAR - 1, -1)
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
# Validated against archived 2010-2023 exports.
COLUMN_MAP = {
    "contract_number":    [
        "Núm. Contrato", "Num. Contrato",
        "Número de Contrato", "Numero de Contrato",
        "Numero Contrato", "NumContrato",
    ],
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
