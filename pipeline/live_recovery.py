"""
Shared client, parser, and persistence helpers for live contract recovery.
"""
from __future__ import annotations

import csv
import html
import json
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

from config import BASE_URL, HEADERS
from contract_utils import (
    CANONICAL_RECORD_COLUMNS,
    LIVE_MONITOR_SOURCE_TYPE,
    LIVE_RECOVERY_SOURCE_TYPE,
    RAW_SOURCE_TYPE,
    fiscal_year_from_date,
    is_original_amendment,
    normalize_contract_record,
    normalize_contractor_family,
    normalize_entity_name,
    normalize_lookup_value,
    parse_ms_ajax_date,
    records_equivalent,
    row_hash,
)


TARGET_COLUMNS = [
    "contract_number",
    "entity",
    "contractor",
    "recovery_batch",
    "lookup_mode",
    "source_url",
    "status",
    "notes",
    "last_checked_at",
]

RECOVERED_COLUMNS = CANONICAL_RECORD_COLUMNS

SEARCH_PAGE_URL = f"{BASE_URL}/contract"
SEARCH_API_URL = f"{BASE_URL}/contract/search"
DETAIL_PATH = "/contract/details"
ENTITY_LOOKUP_URL = f"{BASE_URL}/entity/findby"
DOCUMENT_DOWNLOAD_URL = f"{BASE_URL}/contract/downloaddocument"
ENRICHMENT_DEFER_NOTE = "existing original already present; defer live detail supplementation to enrichment track"


@dataclass
class RecoveryTarget:
    contract_number: str
    entity: str
    contractor: str
    recovery_batch: str | None
    lookup_mode: str
    source_url: str | None
    status: str
    notes: str | None
    last_checked_at: str | None


@dataclass
class ParsedContractDetail:
    record: dict
    contractor_names: list[str]
    entity_display: str | None


def recovery_target_identity(contract_number: str, entity: str, contractor: str) -> tuple[str, str, str]:
    return (
        normalize_lookup_value(contract_number),
        normalize_entity_name(entity),
        normalize_contractor_family(contractor),
    )


def recovery_target_batch_sort_key(recovery_batch: str | None) -> tuple[int, str]:
    value = (recovery_batch or "").strip()
    match = re.match(r"^(\d{4})-(\d{4})$", value)
    if not match:
        return (10**9, value)
    start_year = int(match.group(1))
    end_year = int(match.group(2))
    return (-start_year, f"{start_year:04d}-{end_year:04d}")


def sort_recovery_targets(targets: list[RecoveryTarget]) -> list[RecoveryTarget]:
    return sorted(
        targets,
        key=lambda target: (
            recovery_target_batch_sort_key(target.recovery_batch),
            target.contract_number or "",
            normalize_entity_name(target.entity),
            normalize_contractor_family(target.contractor),
            target.lookup_mode or "",
        ),
    )


def build_detail_url(contract_id: str | int) -> str:
    return f"{BASE_URL}{DETAIL_PATH}?contractid={contract_id}"


def build_document_url(document_code: str | None) -> str | None:
    if not document_code:
        return None
    return f"{DOCUMENT_DOWNLOAD_URL}?{urlencode({'code': document_code})}"


def extract_contract_id(source_url: str | None) -> str | None:
    if not source_url:
        return None
    parsed = urlparse(source_url)
    values = parse_qs(parsed.query).get("contractid") or []
    return values[0] if values else None


def split_entity_display(value: str | None) -> tuple[str | None, str | None]:
    if not value:
        return None, None
    if "|" not in value:
        return None, value.strip()
    entity_number, entity_name = value.split("|", 1)
    entity_number = entity_number.strip() or None
    entity_name = entity_name.strip() or None
    return entity_number, entity_name


def _find_input_value(soup: BeautifulSoup, element_id: str) -> str | None:
    node = soup.find(id=element_id)
    if not node:
        return None
    if node.has_attr("value"):
        return node.get("value")
    return node.get_text(" ", strip=True) or None


def _extract_table_rows_by_heading(soup: BeautifulSoup, heading_text: str) -> list[list[str]]:
    for heading in soup.find_all(["h4", "h5", "h6"]):
        text = normalize_lookup_value(heading.get_text(" ", strip=True))
        if text != normalize_lookup_value(heading_text):
            continue
        table = heading.find_next("table")
        if not table or not table.tbody:
            return []
        rows: list[list[str]] = []
        for tr in table.tbody.find_all("tr"):
            cells = [cell.get_text(" ", strip=True) for cell in tr.find_all(["td", "th"])]
            if cells:
                rows.append(cells)
        return rows
    return []


def parse_contract_detail_html(
    html_text: str,
    *,
    source_url: str | None = None,
    captured_at: str | None = None,
) -> ParsedContractDetail:
    soup = BeautifulSoup(html_text, "html.parser")
    entity_display = _find_input_value(soup, "EntityName")
    entity_number, entity_name = split_entity_display(entity_display)

    contractor_rows = _extract_table_rows_by_heading(soup, "Contratista")
    contractor_names = [html.unescape(row[0]) for row in contractor_rows if row]
    primary_contractor = contractor_names[0] if contractor_names else None
    source_contract_id = extract_contract_id(source_url)
    inserted_at = captured_at or datetime.now(timezone.utc).isoformat()

    record = normalize_contract_record(
        {
            "contract_number": _find_input_value(soup, "ContractNumber"),
            "entity": entity_name,
            "entity_number": entity_number,
            "contractor": primary_contractor,
            "amendment": _find_input_value(soup, "Amendment"),
            "service_category": _find_input_value(soup, "ServiceGroup"),
            "service_type": _find_input_value(soup, "Service"),
            "amount": _find_input_value(soup, "Amount"),
            "amount_receivable": _find_input_value(soup, "AmountToReceive"),
            "award_date": _find_input_value(soup, "DateOfGrant"),
            "valid_from": _find_input_value(soup, "EffectiveDateFrom"),
            "valid_to": _find_input_value(soup, "EffectiveDateTo"),
            "procurement_method": _find_input_value(soup, "ContractingForm"),
            "fund_type": _find_input_value(soup, "Fund"),
            "pco_number": _find_input_value(soup, "PcONumber"),
            "cancelled": 0,
            "document_url": None,
            "fiscal_year": fiscal_year_from_date(_find_input_value(soup, "DateOfGrant")),
            "source_type": LIVE_RECOVERY_SOURCE_TYPE,
            "source_url": source_url,
            "source_contract_id": source_contract_id,
            "inserted_at": inserted_at,
        },
        default_source_type=LIVE_RECOVERY_SOURCE_TYPE,
        inserted_at=inserted_at,
    )

    return ParsedContractDetail(
        record=record,
        contractor_names=contractor_names,
        entity_display=entity_display,
    )


def normalize_search_result_row(search_row: dict, *, inserted_at: str, source_type: str) -> dict:
    contractors = [
        html.unescape((contractor or {}).get("Name") or "")
        for contractor in (search_row.get("Contractors") or [])
        if (contractor or {}).get("Name")
    ]
    contract_id = str(search_row.get("ContractId")) if search_row.get("ContractId") is not None else None
    detail_url = build_detail_url(contract_id) if contract_id else None
    return normalize_contract_record(
        {
            "contract_number": search_row.get("ContractNumber"),
            "entity": search_row.get("EntityName"),
            "entity_number": search_row.get("EntityId"),
            "contractor": contractors[0] if contractors else None,
            "amendment": search_row.get("Amendment"),
            "service_category": search_row.get("ServiceGroup"),
            "service_type": search_row.get("Service"),
            "amount": search_row.get("AmountToPay"),
            "amount_receivable": search_row.get("AmountToReceive"),
            "award_date": parse_ms_ajax_date(search_row.get("DateOfGrant")),
            "valid_from": parse_ms_ajax_date(search_row.get("EffectiveDateFrom")),
            "valid_to": parse_ms_ajax_date(search_row.get("EffectiveDateTo")),
            "procurement_method": None,
            "fund_type": None,
            "pco_number": None,
            "cancelled": 1 if search_row.get("CancellationDate") else 0,
            "document_url": build_document_url(search_row.get("DocumentWithoutSocialSecurityId")),
            "fiscal_year": fiscal_year_from_date(parse_ms_ajax_date(search_row.get("DateOfGrant"))),
            "source_type": source_type,
            "source_url": detail_url,
            "source_contract_id": contract_id,
            "inserted_at": inserted_at,
        },
        default_source_type=source_type,
        inserted_at=inserted_at,
    )


class OCPRContractRegistryClient:
    def __init__(self, session: requests.Session | None = None):
        self.session = session or requests.Session()
        self._anti_forgery_token: str | None = None

    def _browser_headers(self) -> dict:
        return {
            "User-Agent": HEADERS["User-Agent"],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": f"{BASE_URL}/",
        }

    def _ajax_headers(self) -> dict:
        self.bootstrap_search()
        return {
            "User-Agent": HEADERS["User-Agent"],
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/json; charset=utf-8",
            "Referer": SEARCH_PAGE_URL,
            "X-Requested-With": "XMLHttpRequest",
            "__RequestVerificationToken": self._anti_forgery_token or "",
        }

    def bootstrap_search(self) -> str:
        if self._anti_forgery_token:
            return self._anti_forgery_token

        response = self.session.get(SEARCH_PAGE_URL, headers=self._browser_headers(), timeout=60)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        token_node = soup.find("input", {"name": "__RequestVerificationToken"})
        if not token_node or not token_node.get("value"):
            raise RuntimeError("Could not locate the OCPR anti-forgery token on the search page.")
        self._anti_forgery_token = str(token_node["value"])
        return self._anti_forgery_token

    def resolve_entity_id(self, entity_name: str) -> str | None:
        response = self.session.get(
            ENTITY_LOOKUP_URL,
            params={"name": entity_name, "pageIndex": 1, "pageSize": 80},
            headers={
                "User-Agent": HEADERS["User-Agent"],
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Referer": SEARCH_PAGE_URL,
                "X-Requested-With": "XMLHttpRequest",
            },
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()
        results = data.get("Results") or []
        exact = [
            result
            for result in results
            if normalize_entity_name(result.get("Name")) == normalize_entity_name(entity_name)
        ]
        if len(exact) == 1:
            return str(exact[0]["Code"])
        return None

    def search_contract_rows(
        self,
        *,
        contract_number: str | None = None,
        entity_id: str | None = None,
        contractor_name: str | None = None,
        award_date_from: str | None = None,
        start: int = 0,
        length: int = 100,
    ) -> dict:
        payload = {
            "draw": 1,
            "start": start,
            "length": length,
            "order": [{"column": 1, "dir": "desc"}],
            "search": {"value": "", "regex": False},
            "EntityId": entity_id,
            "ContractNumber": contract_number,
            "ContractorName": contractor_name,
            "DateOfGrantFrom": award_date_from,
            "DateOfGrantTo": None,
            "EffectiveDateFrom": None,
            "EffectiveDateTo": None,
            "AmountFrom": None,
            "AmountTo": None,
            "ServiceGroupId": None,
            "ServiceId": None,
            "FundId": None,
            "ContractingFormId": None,
            "PCONumber": None,
        }
        response = self.session.post(
            SEARCH_API_URL,
            headers=self._ajax_headers(),
            json=payload,
            timeout=60,
        )
        response.raise_for_status()
        return response.json()

    def iter_recent_contract_rows(self, *, award_date_from: str, page_size: int = 100) -> Iterable[list[dict]]:
        start = 0
        while True:
            payload = self.search_contract_rows(
                award_date_from=award_date_from,
                start=start,
                length=page_size,
            )
            rows = payload.get("data") or []
            if not rows:
                break
            yield rows
            start += page_size
            if start >= int(payload.get("recordsFiltered") or 0):
                break

    def fetch_detail_html(self, source_url: str) -> str:
        response = self.session.get(
            source_url,
            headers=self._browser_headers(),
            timeout=60,
        )
        response.raise_for_status()
        return response.text


def load_recovery_targets(path: Path) -> list[RecoveryTarget]:
    if not path.exists():
        return []
    with open(path, encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        return [
            RecoveryTarget(
                contract_number=(row.get("contract_number") or "").strip(),
                entity=(row.get("entity") or "").strip(),
                contractor=(row.get("contractor") or "").strip(),
                recovery_batch=(row.get("recovery_batch") or "").strip() or None,
                lookup_mode=(row.get("lookup_mode") or "").strip(),
                source_url=(row.get("source_url") or "").strip() or None,
                status=(row.get("status") or "").strip() or "pending",
                notes=(row.get("notes") or "").strip() or None,
                last_checked_at=(row.get("last_checked_at") or "").strip() or None,
            )
            for row in reader
        ]


def write_recovery_targets(path: Path, targets: list[RecoveryTarget]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=TARGET_COLUMNS)
        writer.writeheader()
        for target in sort_recovery_targets(targets):
            writer.writerow(asdict(target))


def load_recovered_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        rows = []
        for row in reader:
            normalized = normalize_contract_record(
                row,
                default_source_type=row.get("source_type") or RAW_SOURCE_TYPE,
                inserted_at=row.get("inserted_at") or datetime.now(timezone.utc).isoformat(),
            )
            rows.append(normalized)
        return rows


def write_recovered_rows(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    sorted_rows = sorted(
        rows,
        key=lambda row: (
            row.get("contract_number") or "",
            row.get("entity") or "",
            row.get("award_date") or "",
            row.get("source_contract_id") or "",
            row.get("contractor") or "",
        ),
    )
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=RECOVERED_COLUMNS)
        writer.writeheader()
        for row in sorted_rows:
            writer.writerow({column: row.get(column) for column in RECOVERED_COLUMNS})


def contractor_target_matches(target_contractor: str, contractor_names: Iterable[str]) -> bool:
    target_family = normalize_contractor_family(target_contractor)
    if not target_family:
        return True
    candidate_families = {
        normalize_contractor_family(name)
        for name in contractor_names
        if normalize_contractor_family(name)
    }
    return target_family in candidate_families


def validate_detail_match(target: RecoveryTarget, parsed: ParsedContractDetail) -> tuple[bool, str]:
    record = parsed.record
    if not is_original_amendment(record.get("amendment")):
        return False, "detail page is not the original contract row"
    if (record.get("contract_number") or "") != target.contract_number:
        return False, "detail page contract number does not match the recovery target"
    if normalize_entity_name(record.get("entity")) != normalize_entity_name(target.entity):
        return False, "detail page entity does not match the recovery target"
    if not contractor_target_matches(target.contractor, parsed.contractor_names or [record.get("contractor") or ""]):
        return False, "detail page contractor family does not match the recovery target"
    if not record.get("award_date"):
        return False, "detail page is missing an award date"
    return True, "validated"


def filter_search_candidates(search_rows: list[dict], target: RecoveryTarget) -> list[dict]:
    candidates = []
    for row in search_rows:
        if (row.get("ContractNumber") or "").strip() != target.contract_number:
            continue
        if normalize_entity_name(row.get("EntityName")) != normalize_entity_name(target.entity):
            continue
        if not is_original_amendment(row.get("Amendment")):
            continue
        contractors = [
            html.unescape((contractor or {}).get("Name") or "")
            for contractor in (row.get("Contractors") or [])
            if (contractor or {}).get("Name")
        ]
        if contractors and not contractor_target_matches(target.contractor, contractors):
            continue
        candidates.append(row)
    return candidates


def fetch_existing_rows_for_hash(conn: sqlite3.Connection, row_hash_value: str) -> list[dict]:
    if conn is None:
        return []
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT contract_number, entity, entity_number, contractor, amendment,
               service_category, service_type, amount, amount_receivable,
               award_date, valid_from, valid_to, procurement_method, fund_type,
               pco_number, cancelled, document_url, fiscal_year,
               source_type, source_url, source_contract_id
        FROM contracts
        WHERE row_hash = ?
        """,
        (row_hash_value,),
    ).fetchall()
    return [dict(row) for row in rows]


def fetch_existing_original_family_rows(conn: sqlite3.Connection, contract_number: str, entity: str) -> list[dict]:
    if conn is None:
        return []
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT contract_number, entity, entity_number, contractor, amendment,
               service_category, service_type, amount, amount_receivable,
               award_date, valid_from, valid_to, procurement_method, fund_type,
               pco_number, cancelled, document_url, fiscal_year,
               source_type, source_url, source_contract_id
        FROM contracts
        WHERE contract_number = ? AND entity = ?
        """,
        (contract_number, entity),
    ).fetchall()
    return [dict(row) for row in rows if is_original_amendment(row["amendment"])]


def _matches_when_present(left: object, right: object) -> bool:
    return left in {None, ""} or right in {None, ""} or left == right


def _matches_zero_or_missing(left: object, right: object) -> bool:
    left_value = 0 if left in {None, ""} else left
    right_value = 0 if right in {None, ""} else right
    return left_value == right_value


def _is_enrichment_track_match(existing: dict, record: dict) -> bool:
    if existing.get("contract_number") != record.get("contract_number"):
        return False
    if existing.get("entity") != record.get("entity"):
        return False
    if not is_original_amendment(existing.get("amendment")) or not is_original_amendment(record.get("amendment")):
        return False

    for key in ("amount", "award_date", "valid_from", "valid_to"):
        if existing.get(key) != record.get(key):
            return False

    if not _matches_when_present(existing.get("entity_number"), record.get("entity_number")):
        return False

    if not _matches_zero_or_missing(existing.get("amount_receivable"), record.get("amount_receivable")):
        return False

    existing_family = normalize_contractor_family(existing.get("contractor"))
    record_family = normalize_contractor_family(record.get("contractor"))
    if existing_family and record_family and existing_family != record_family:
        return False

    return True


def detect_recovery_conflict(
    conn: sqlite3.Connection,
    recovered_rows: list[dict],
    record: dict,
) -> tuple[str, str]:
    matching_hash_rows = fetch_existing_rows_for_hash(conn, record["row_hash"]) + [
        row for row in recovered_rows if row.get("row_hash") == record["row_hash"]
    ]
    for existing in matching_hash_rows:
        if records_equivalent(existing, record):
            return "identical", "matching recovered row already exists"
        if _is_enrichment_track_match(existing, record):
            if existing.get("source_type") == LIVE_RECOVERY_SOURCE_TYPE:
                return "identical", "matching recovered row already exists"
            return "needs_enrichment", ENRICHMENT_DEFER_NOTE
        return "hash_conflict", "row hash collision with a different existing row"

    same_family_rows = fetch_existing_original_family_rows(conn, record["contract_number"], record["entity"]) + [
        row
        for row in recovered_rows
        if row.get("contract_number") == record["contract_number"]
        and row.get("entity") == record["entity"]
        and is_original_amendment(row.get("amendment"))
    ]
    for existing in same_family_rows:
        if records_equivalent(existing, record):
            return "identical", "matching original row already exists for this family"
        if _is_enrichment_track_match(existing, record):
            if existing.get("source_type") == LIVE_RECOVERY_SOURCE_TYPE:
                return "identical", "matching recovered row already exists"
            return "needs_enrichment", ENRICHMENT_DEFER_NOTE
        if normalize_contractor_family(existing.get("contractor")) != normalize_contractor_family(record.get("contractor")):
            return "family_conflict", "existing original row in this family has a conflicting contractor family"
        return "family_conflict", "existing original row in this family conflicts with the recovered row"

    return "ok", "ready to write"


def enrich_from_search_row(record: dict, search_row: dict, *, source_type: str) -> dict:
    merged = dict(record)
    search_record = normalize_search_result_row(search_row, inserted_at=record["inserted_at"], source_type=source_type)
    for key, value in search_record.items():
        if key == "row_hash":
            continue
        if merged.get(key) in {None, "", 0} and value not in {None, ""}:
            merged[key] = value
    merged["source_type"] = source_type
    merged["source_url"] = search_record.get("source_url") or merged.get("source_url")
    merged["source_contract_id"] = search_record.get("source_contract_id") or merged.get("source_contract_id")
    merged["document_url"] = search_record.get("document_url") or merged.get("document_url")
    merged["row_hash"] = row_hash(merged)
    return merged


def format_note(*parts: str | None) -> str:
    return "; ".join(part for part in parts if part)


def upsert_recovered_row(recovered_rows: list[dict], record: dict):
    for index, existing in enumerate(recovered_rows):
        if records_equivalent(existing, record):
            recovered_rows[index] = record
            return
    recovered_rows.append(record)
