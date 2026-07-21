import hashlib
import importlib
import json
import re
import sys
import tempfile
import unittest
from pathlib import Path
from urllib.parse import urlparse


PIPELINE_DIR = Path(__file__).resolve().parents[1] / "pipeline"
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "source-discovery"
BULK_FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "bulk"

OFFICIAL_HOST = "consultacontratos.ocpr.gov.pr"
REGISTRY_URL = f"https://{OFFICIAL_HOST}/contract/frequent-search"
BULK_URL_TEMPLATE = (
    f"https://{OFFICIAL_HOST}"
    "/contract/downloadfrequentsearchfiscalyeardocument?q={fiscal_year}"
)
CAPTURE_TIME = "2026-07-19T00:00:00+00:00"


def _require_pipeline_module(name):
    """Import an intentionally absent Task 8 module as a normal RED failure."""
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError as exc:
        if exc.name == name or exc.name == f"pipeline.{name}":
            raise AssertionError(
                f"pipeline/{name}.py is not implemented yet: {exc}"
            ) from exc
        raise


def _fixture_bytes(filename):
    return (FIXTURES_DIR / filename).read_bytes()


def _valid_csv_bytes():
    # Reuse the existing certified Latin-1 fixture instead of inventing a
    # second schema or source shape for this discovery contract.
    return (BULK_FIXTURES_DIR / "ocpr-bulk-v1.csv").read_bytes()


def _year_from_url(url):
    match = re.search(r"(\d{4}-\d{4})", url)
    if not match:
        raise AssertionError(f"test transport received a URL without a fiscal year: {url}")
    return match.group(1)


class FakeResponse:
    """Small requests-like response object for entirely offline tests."""

    def __init__(
        self,
        *,
        status_code=200,
        headers=None,
        content=b"",
        url=None,
        history=(),
    ):
        self.status_code = status_code
        self.headers = dict(headers or {})
        self.content = content
        self.url = url
        self.history = tuple(history)


class StreamingResponse:
    """Response whose body is available only through bounded chunks."""

    def __init__(
        self,
        *,
        status_code=200,
        headers=None,
        chunks=(),
        url=None,
    ):
        self.status_code = status_code
        self.headers = dict(headers or {})
        self._chunks = tuple(chunks)
        self.url = url
        self.history = ()
        self.iteration_count = 0
        self.content_accessed = False

    @property
    def content(self):
        self.content_accessed = True
        raise AssertionError("streaming response.content must not be accessed")

    def iter_content(self, chunk_size=None):
        del chunk_size
        for chunk in self._chunks:
            self.iteration_count += 1
            yield chunk


def _csv_response(url, fiscal_year, *, content=None):
    return FakeResponse(
        status_code=200,
        headers={
            "Content-Type": "text/csv; charset=latin-1",
            "Content-Disposition": (
                f'attachment; filename="contratos_{fiscal_year}.csv"'
            ),
        },
        content=_valid_csv_bytes() if content is None else content,
        url=url,
    )


def _redirected_csv_response(requested_url, fiscal_year, *, final_url=None):
    if final_url is None:
        final_url = (
            f"https://{OFFICIAL_HOST}/records/bulk/"
            f"contratos_{fiscal_year}.csv"
        )
    hop = FakeResponse(
        status_code=302,
        headers={"Location": final_url},
        url=requested_url,
    )
    response = _csv_response(final_url, fiscal_year)
    response.history = (hop,)
    return response


class PlannedGet:
    """Injected GET-only transport with deterministic response/exception plans."""

    def __init__(self, plans, *, registry_html=None):
        self.plans = {year: list(values) for year, values in plans.items()}
        self.registry_html = (
            _fixture_bytes("registry-page.html")
            if registry_html is None
            else registry_html
        )
        self.calls = []
        self.attempts = {}

    def __call__(self, url, **kwargs):
        requested_method = kwargs.pop("method", "GET")
        self.calls.append(
            {"method": requested_method, "url": url, "kwargs": dict(kwargs)}
        )
        if str(requested_method).upper() != "GET":
            raise AssertionError(f"Task 8 transport must use GET, got {requested_method}")

        if url == REGISTRY_URL:
            return FakeResponse(
                status_code=200,
                headers={"Content-Type": "text/html; charset=utf-8"},
                content=self.registry_html,
                url=REGISTRY_URL,
            )

        fiscal_year = _year_from_url(url)
        self.attempts[fiscal_year] = self.attempts.get(fiscal_year, 0) + 1
        if fiscal_year not in self.plans:
            raise AssertionError(f"unexpected bulk probe for {fiscal_year}: {url}")
        choices = self.plans[fiscal_year]
        choice = choices[0] if len(choices) == 1 else choices.pop(0)
        if isinstance(choice, BaseException):
            raise choice
        if callable(choice):
            return choice(url, fiscal_year)
        return choice


class FixtureFileContractTests(unittest.TestCase):
    """Mechanical checks stay green even while production APIs are absent."""

    def test_all_offline_fixtures_exist_and_are_local(self):
        expected = {
            "registry-page.html",
            "registry-page-moved.html",
            "invalid-payload.html",
        }
        self.assertEqual(
            {path.name for path in FIXTURES_DIR.iterdir() if path.is_file()},
            expected,
        )
        for filename in expected:
            self.assertTrue((FIXTURES_DIR / filename).is_file())

    def test_registry_fixture_contains_only_the_three_advertised_bulk_years(self):
        html = (FIXTURES_DIR / "registry-page.html").read_text(encoding="utf-8")
        self.assertEqual(
            set(re.findall(r"q=(\d{4}-\d{4})", html)),
            {"2021-2022", "2022-2023", "2023-2024"},
        )
        self.assertIn("/contract/search?", html)
        self.assertIn("/contract/document/", html)
        self.assertIn("/contract/export?format=xlsx", html)
        self.assertIn("mailto:", html)

    def test_moved_fixture_contains_official_path_movement_and_decoys(self):
        html = (FIXTURES_DIR / "registry-page-moved.html").read_text(
            encoding="utf-8"
        )
        self.assertIn("/records/bulk/contratos_2023-2024.csv", html)
        self.assertIn(
            f"https://{OFFICIAL_HOST}/records/bulk/contratos_2024-2025.csv",
            html,
        )
        self.assertIn("/contract/search?", html)
        self.assertIn("/contract/document/", html)
        self.assertIn("mailto:", html)

    def test_invalid_payload_fixture_is_an_html_error_body(self):
        body = _fixture_bytes("invalid-payload.html")
        self.assertIn(b"<!doctype html>", body.lower())
        self.assertIn(b"Access denied", body)


class RegistryParserContractTests(unittest.TestCase):
    def test_extracts_current_registry_fiscal_year_links_and_resolves_https_urls(self):
        module = _require_pipeline_module("discover_bulk_sources")
        links = module.extract_fiscal_year_links(
            (FIXTURES_DIR / "registry-page.html").read_text(encoding="utf-8"),
            page_url=REGISTRY_URL,
        )
        by_year = {link.fiscal_year: link.url for link in links}
        self.assertEqual(
            set(by_year), {"2021-2022", "2022-2023", "2023-2024"}
        )
        for fiscal_year, url in by_year.items():
            with self.subTest(fiscal_year=fiscal_year):
                parsed = urlparse(url)
                self.assertEqual(parsed.scheme, "https")
                self.assertEqual(parsed.hostname, OFFICIAL_HOST)
                self.assertIn("downloadfrequentsearchfiscalyeardocument", parsed.path)

    def test_extracts_moved_official_path_without_silently_rewriting_it(self):
        module = _require_pipeline_module("discover_bulk_sources")
        links = module.extract_fiscal_year_links(
            (FIXTURES_DIR / "registry-page-moved.html").read_text(
                encoding="utf-8"
            ),
            page_url=REGISTRY_URL,
        )
        by_year = {link.fiscal_year: link.url for link in links}
        self.assertEqual(
            by_year["2023-2024"],
            f"https://{OFFICIAL_HOST}/records/bulk/contratos_2023-2024.csv",
        )
        self.assertEqual(
            by_year["2024-2025"],
            f"https://{OFFICIAL_HOST}/records/bulk/contratos_2024-2025.csv",
        )
        self.assertNotIn("/contract/search", " ".join(by_year.values()))
        self.assertNotIn("/contract/document", " ".join(by_year.values()))

    def test_parser_does_not_return_search_document_or_export_endpoints(self):
        module = _require_pipeline_module("discover_bulk_sources")
        links = module.extract_fiscal_year_links(
            (FIXTURES_DIR / "registry-page.html").read_text(encoding="utf-8"),
            page_url=REGISTRY_URL,
        )
        returned_urls = [link.url for link in links]
        self.assertTrue(returned_urls)
        for url in returned_urls:
            with self.subTest(url=url):
                self.assertNotIn("/contract/search", url)
                self.assertNotIn("/contract/document/", url)
                self.assertNotIn("/contract/export", url)
                self.assertNotIn("mailto:", url)

    def test_candidate_window_is_bounded_to_anchors_and_adjacent_newer_years(self):
        module = _require_pipeline_module("discover_bulk_sources")
        years = module.candidate_fiscal_years(
            newest_certified_year="2021-2022",
            advertised_years=("2022-2023", "2023-2024"),
            current_fiscal_year="2024-2025",
            adjacent_newer_years=1,
        )
        self.assertEqual(
            set(years),
            {"2021-2022", "2022-2023", "2023-2024", "2024-2025", "2025-2026"},
        )
        self.assertEqual(len(years), 5)
        self.assertNotIn("2010-2011", years)
        self.assertNotIn("2026-2027", years)

    def test_candidate_window_includes_unadvertised_intervening_years(self):
        module = _require_pipeline_module("discover_bulk_sources")
        years = module.candidate_fiscal_years(
            newest_certified_year="2022-2023",
            advertised_years=(),
            current_fiscal_year="2026-2027",
            adjacent_newer_years=1,
        )
        self.assertEqual(
            years,
            (
                "2022-2023",
                "2023-2024",
                "2024-2025",
                "2025-2026",
                "2026-2027",
                "2027-2028",
            ),
        )


class DiscoveryStatusContractTests(unittest.TestCase):
    def _make_planned_get(self, *, external_redirect=False):
        redirect_url = (
            f"https://{OFFICIAL_HOST}/records/bulk/contratos_2023-2024.csv"
            if not external_redirect
            else "https://unapproved.example.invalid/records/contratos_2023-2024.csv"
        )
        return PlannedGet(
            {
                "2021-2022": [lambda url, year: _csv_response(url, year)],
                "2022-2023": [
                    FakeResponse(status_code=404, headers={}, content=b"", url=None)
                ],
                "2023-2024": [
                    lambda url, year: _redirected_csv_response(
                        url, year, final_url=redirect_url
                    )
                ],
                "2024-2025": [lambda url, year: _csv_response(url, year)],
                "2025-2026": [
                    lambda url, year: FakeResponse(
                        status_code=200,
                        headers={
                            "Content-Type": "text/csv",
                            "Content-Disposition": (
                                f'attachment; filename="contratos_{year}.csv"'
                            ),
                        },
                        content=_fixture_bytes("invalid-payload.html"),
                        url=url,
                    )
                ],
                "2026-2027": [
                    FakeResponse(status_code=404, headers={}, content=b"", url=None)
                ],
                "2027-2028": [
                    TimeoutError("offline transient timeout"),
                    TimeoutError("offline transient timeout"),
                    TimeoutError("offline transient timeout"),
                ],
            }
        )

    def test_all_discovery_statuses_are_reported_with_advertised_semantics(self):
        module = _require_pipeline_module("discover_bulk_sources")
        http_get = self._make_planned_get()
        sleeps = []
        report = module.discover_bulk_sources(
            registry_url=REGISTRY_URL,
            newest_certified_year="2021-2022",
            current_fiscal_year="2024-2025",
            http_get=http_get,
            allowed_hosts={OFFICIAL_HOST},
            bulk_url_template=BULK_URL_TEMPLATE,
            adjacent_newer_years=3,
            max_retries=2,
            backoff_seconds=0.25,
            sleep=sleeps.append,
            max_bytes=50_000,
        )
        observations = {item.fiscal_year: item for item in report.observations}
        self.assertEqual(
            {item.status for item in observations.values()},
            {
                "listed_available",
                "listed_but_404",
                "unlisted_available",
                "relocated_redirect",
                "unavailable",
                "transient_error",
                "invalid_payload",
            },
        )
        self.assertEqual(observations["2021-2022"].status, "listed_available")
        self.assertTrue(observations["2021-2022"].advertised)
        self.assertTrue(observations["2021-2022"].eligible)
        self.assertEqual(observations["2022-2023"].status, "listed_but_404")
        self.assertEqual(observations["2023-2024"].status, "relocated_redirect")
        self.assertTrue(observations["2023-2024"].review_required)
        self.assertFalse(observations["2023-2024"].eligible)
        self.assertEqual(observations["2024-2025"].status, "unlisted_available")
        self.assertFalse(observations["2024-2025"].advertised)
        self.assertTrue(observations["2024-2025"].eligible)
        self.assertEqual(observations["2025-2026"].status, "invalid_payload")
        self.assertEqual(observations["2026-2027"].status, "unavailable")
        self.assertEqual(observations["2027-2028"].status, "transient_error")

        requested = observations["2023-2024"].requested_url
        moved = observations["2023-2024"].final_url
        self.assertEqual(
            observations["2023-2024"].redirect_chain,
            (requested, moved),
        )
        self.assertEqual(observations["2023-2024"].reason, "path_moved_pending_review")

        self.assertEqual(http_get.attempts["2022-2023"], 1)
        self.assertEqual(http_get.attempts["2027-2028"], 3)
        self.assertEqual(sleeps, [0.25, 0.5])

    def test_probe_is_get_only_and_never_touches_decoy_lanes(self):
        module = _require_pipeline_module("discover_bulk_sources")
        http_get = self._make_planned_get()
        module.discover_bulk_sources(
            registry_url=REGISTRY_URL,
            newest_certified_year="2021-2022",
            current_fiscal_year="2024-2025",
            http_get=http_get,
            allowed_hosts={OFFICIAL_HOST},
            bulk_url_template=BULK_URL_TEMPLATE,
            adjacent_newer_years=0,
            max_retries=0,
            sleep=lambda _delay: self.fail("no retry expected in this probe"),
        )
        self.assertTrue(http_get.calls)
        self.assertTrue(all(call["method"] == "GET" for call in http_get.calls))
        probe_urls = [
            call["url"] for call in http_get.calls if call["url"] != REGISTRY_URL
        ]
        self.assertTrue(probe_urls)
        for url in probe_urls:
            with self.subTest(url=url):
                self.assertNotIn("/contract/search", url)
                self.assertNotIn("/contract/document/", url)
                self.assertNotIn("/contract/export", url)
                self.assertNotIn("mailto:", url)
                self.assertNotIn("xlsx", url.lower())

    def test_redirect_to_unapproved_or_non_https_host_is_retained_but_rejected(self):
        module = _require_pipeline_module("discover_bulk_sources")
        http_get = self._make_planned_get(external_redirect=True)
        report = module.discover_bulk_sources(
            registry_url=REGISTRY_URL,
            newest_certified_year="2021-2022",
            current_fiscal_year="2024-2025",
            http_get=http_get,
            allowed_hosts={OFFICIAL_HOST},
            bulk_url_template=BULK_URL_TEMPLATE,
            adjacent_newer_years=0,
            max_retries=0,
        )
        moved = {
            item.fiscal_year: item
            for item in report.observations
            if item.fiscal_year == "2023-2024"
        }["2023-2024"]
        self.assertEqual(moved.status, "relocated_redirect")
        self.assertEqual(moved.reason, "redirect_host_not_allowlisted")
        self.assertFalse(moved.eligible)
        self.assertTrue(moved.review_required)
        self.assertEqual(
            moved.redirect_chain,
            (moved.requested_url, moved.final_url),
        )
        self.assertEqual(urlparse(moved.final_url).hostname, "unapproved.example.invalid")

    def test_manual_redirect_validation_never_calls_off_allowlist_target(self):
        module = _require_pipeline_module("discover_bulk_sources")
        requested = BULK_URL_TEMPLATE.format(fiscal_year="2024-2025")
        external = "https://unapproved.example.invalid/steal.csv"
        calls = []

        def http_get(url, **kwargs):
            calls.append((url, dict(kwargs)))
            if url == REGISTRY_URL:
                return FakeResponse(
                    status_code=200,
                    headers={"Content-Type": "text/html"},
                    content=b"<html></html>",
                    url=url,
                )
            if url == requested:
                return FakeResponse(
                    status_code=302,
                    headers={"Location": external},
                    content=b"",
                    url=url,
                )
            self.fail(f"off-allowlist redirect was contacted: {url}")

        report = module.discover_bulk_sources(
            registry_url=REGISTRY_URL,
            newest_certified_year=None,
            current_fiscal_year="2024-2025",
            http_get=http_get,
            allowed_hosts={OFFICIAL_HOST},
            bulk_url_template=BULK_URL_TEMPLATE,
            adjacent_newer_years=0,
            max_retries=0,
        )
        observation = report.observations[0]
        self.assertEqual(observation.status, "relocated_redirect")
        self.assertEqual(observation.reason, "redirect_host_not_allowlisted")
        self.assertEqual(observation.redirect_chain, (requested, external))
        self.assertEqual([url for url, _kwargs in calls], [REGISTRY_URL, requested])
        for _url, kwargs in calls:
            self.assertFalse(kwargs["allow_redirects"])
            self.assertTrue(kwargs["stream"])

    def test_registry_body_is_streamed_with_the_same_bound(self):
        module = _require_pipeline_module("discover_bulk_sources")
        registry = StreamingResponse(
            status_code=200,
            headers={"Content-Type": "text/html"},
            chunks=(b"<html>", b"ignored-over-limit"),
            url=REGISTRY_URL,
        )

        def http_get(url, **_kwargs):
            if url == REGISTRY_URL:
                return registry
            return FakeResponse(status_code=404, content=b"missing", url=url)

        report = module.discover_bulk_sources(
            registry_url=REGISTRY_URL,
            newest_certified_year=None,
            current_fiscal_year="2024-2025",
            http_get=http_get,
            allowed_hosts={OFFICIAL_HOST},
            bulk_url_template=BULK_URL_TEMPLATE,
            adjacent_newer_years=0,
            max_retries=0,
            max_bytes=8,
        )
        self.assertEqual(len(report.observations), 1)
        self.assertFalse(registry.content_accessed)
        self.assertEqual(registry.iteration_count, 2)


class PayloadValidationContractTests(unittest.TestCase):
    def _response(self, fiscal_year="2024-2025", **overrides):
        values = {
            "status_code": 200,
            "headers": {
                "Content-Type": "text/csv; charset=latin-1",
                "Content-Disposition": (
                    f'attachment; filename="contratos_{fiscal_year}.csv"'
                ),
            },
            "content": _valid_csv_bytes(),
            "url": BULK_URL_TEMPLATE.format(fiscal_year=fiscal_year),
        }
        values.update(overrides)
        return FakeResponse(**values)

    def _assert_invalid(self, response, expected_reason, *, max_bytes=50_000):
        module = _require_pipeline_module("capture_bulk_snapshot")
        validation = module.validate_bulk_response(
            response,
            fiscal_year="2024-2025",
            max_bytes=max_bytes,
        )
        self.assertFalse(validation.valid)
        self.assertEqual(validation.reason, expected_reason)

    def test_non_200_is_invalid_payload(self):
        self._assert_invalid(
            self._response(status_code=404, content=b"not found"),
            "http_status_not_200",
        )

    def test_html_body_is_invalid_even_when_headers_claim_csv(self):
        self._assert_invalid(
            self._response(content=_fixture_bytes("invalid-payload.html")),
            "html_body",
        )

    def test_empty_body_is_invalid(self):
        self._assert_invalid(self._response(content=b""), "empty_body")

    def test_over_byte_limit_is_invalid_before_schema_validation(self):
        self._assert_invalid(
            self._response(content=b"x" * 17),
            "byte_limit_exceeded",
            max_bytes=16,
        )

    def test_bad_content_disposition_is_invalid(self):
        self._assert_invalid(
            self._response(
                headers={
                    "Content-Type": "text/csv",
                    "Content-Disposition": "inline; filename=records.csv",
                }
            ),
            "invalid_content_disposition",
        )

    def test_bad_media_type_is_invalid(self):
        self._assert_invalid(
            self._response(
                headers={
                    "Content-Type": "application/pdf",
                    "Content-Disposition": (
                        'attachment; filename="contratos_2024-2025.csv"'
                    ),
                }
            ),
            "invalid_media_type",
        )

    def test_non_latin_encoding_marker_is_invalid(self):
        self._assert_invalid(
            self._response(content=b"\xff\xfeN\x00o\x00t\x00,\x00C\x00S\x00V\x00"),
            "unrecognized_encoding",
        )

    def test_unknown_schema_is_invalid(self):
        self._assert_invalid(
            self._response(
                content=(
                    b"Not A Known Header,Another Unknown Header\r\n"
                    b"value,still-not-a-contract-record\r\n"
                )
            ),
            "unknown_schema",
        )

    def test_valid_payload_hash_and_length_are_exact_source_bytes(self):
        module = _require_pipeline_module("capture_bulk_snapshot")
        body = _valid_csv_bytes()
        validation = module.validate_bulk_response(
            self._response(content=body),
            fiscal_year="2024-2025",
        )
        self.assertTrue(validation.valid)
        self.assertIsNone(validation.reason)
        self.assertEqual(validation.encoding, "latin-1")
        self.assertEqual(validation.byte_length, len(body))
        self.assertEqual(validation.sha256, hashlib.sha256(body).hexdigest())

    def test_streaming_validation_never_reads_content_and_stops_at_bounded_prefix(self):
        module = _require_pipeline_module("capture_bulk_snapshot")
        response = StreamingResponse(
            headers={
                "Content-Type": "text/csv",
                "Content-Disposition": (
                    'attachment; filename="contratos_2024-2025.csv"'
                ),
            },
            chunks=(b"1234", b"x" * 1000000, b"must-not-be-consumed"),
            url=BULK_URL_TEMPLATE.format(fiscal_year="2024-2025"),
        )
        validation = module.validate_bulk_response(
            response,
            fiscal_year="2024-2025",
            max_bytes=8,
        )
        self.assertFalse(validation.valid)
        self.assertEqual(validation.reason, "byte_limit_exceeded")
        self.assertEqual(validation.byte_length, 9)
        self.assertTrue(validation.truncated)
        self.assertFalse(response.content_accessed)
        self.assertEqual(response.iteration_count, 2)


class ImmutableCaptureContractTests(unittest.TestCase):
    def _capture_kwargs(self, root, response, *, validator=None):
        kwargs = {
            "response": response,
            "fiscal_year": "2010-2011",
            "source_url": BULK_URL_TEMPLATE.format(fiscal_year="2010-2011"),
            "quarantine_dir": root / "quarantine",
            "evidence_dir": root / "evidence",
            "active_view": root / "raw" / "contratos_2010-2011.csv",
            "allowed_hosts": {OFFICIAL_HOST},
            "captured_at": CAPTURE_TIME,
        }
        if validator is not None:
            kwargs["validator"] = validator
        return kwargs

    def test_invalid_response_is_quarantined_before_injected_validation(self):
        module = _require_pipeline_module("capture_bulk_snapshot")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            active_view = root / "raw" / "contratos_2010-2011.csv"
            active_view.parent.mkdir(parents=True)
            active_view.write_bytes(b"existing-active-view")
            body = _valid_csv_bytes()
            response = _csv_response(
                BULK_URL_TEMPLATE.format(fiscal_year="2010-2011"),
                "2010-2011",
                content=body,
            )
            validation_calls = []

            def validator(quarantine_path, fiscal_year):
                validation_calls.append((Path(quarantine_path), fiscal_year))
                self.assertTrue(Path(quarantine_path).is_file())
                self.assertEqual(Path(quarantine_path).read_bytes(), body)
                return "synthetic_schema_review_rejection"

            result = module.capture_bulk_snapshot(
                **self._capture_kwargs(root, response, validator=validator)
            )
            self.assertEqual(result.status, "rejected")
            self.assertEqual(result.reason, "synthetic_schema_review_rejection")
            self.assertEqual(len(validation_calls), 1)
            self.assertTrue(result.quarantine_path.is_file())
            self.assertEqual(result.quarantine_path.read_bytes(), body)
            self.assertIsNone(result.evidence_path)
            self.assertIsNone(result.metadata_path)
            self.assertFalse((root / "evidence").exists())
            self.assertEqual(active_view.read_bytes(), b"existing-active-view")

    def test_capture_validates_quarantined_bytes_without_rereading_network_stream(self):
        module = _require_pipeline_module("capture_bulk_snapshot")
        body = _valid_csv_bytes()

        class OneShotResponse(StreamingResponse):
            def iter_content(self, chunk_size=None):
                if self.iteration_count:
                    raise AssertionError("network stream was consumed more than once")
                yield from super().iter_content(chunk_size=chunk_size)

        response = OneShotResponse(
            status_code=200,
            headers={
                "Content-Type": "text/csv",
                "Content-Disposition": 'attachment; filename="contratos_2010-2011.csv"',
            },
            chunks=(body,),
            url=BULK_URL_TEMPLATE.format(fiscal_year="2010-2011"),
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = module.capture_bulk_snapshot(**self._capture_kwargs(root, response))
            self.assertEqual(result.status, "captured")
            self.assertEqual(response.iteration_count, 1)
            self.assertEqual(result.evidence_path.read_bytes(), body)

    def test_capture_runs_full_certifier_before_accepting_evidence(self):
        module = _require_pipeline_module("capture_bulk_snapshot")
        body = _valid_csv_bytes().replace(
            b'"CONTRATISTA EJEMPLO UNO, S.E."',
            b'"CONTRATISTA\r\nEJEMPLO\r\nUNO"',
            1,
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            active_view = root / "raw" / "contratos_2010-2011.csv"
            active_view.parent.mkdir(parents=True)
            active_view.write_bytes(b"existing-active-view")
            response = _csv_response(
                BULK_URL_TEMPLATE.format(fiscal_year="2010-2011"),
                "2010-2011",
                content=body,
            )
            result = module.capture_bulk_snapshot(**self._capture_kwargs(root, response))
            self.assertEqual(result.status, "invalid_payload")
            self.assertEqual(
                result.reason,
                "certification_failed:UnsupportedMultilineRecordError",
            )
            self.assertTrue(result.quarantine_path.is_file())
            self.assertFalse((root / "evidence").exists())
            self.assertEqual(active_view.read_bytes(), b"existing-active-view")

    def test_capture_is_immutable_noop_on_same_hash_and_never_overwrites_active_view(self):
        module = _require_pipeline_module("capture_bulk_snapshot")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            active_view = root / "raw" / "contratos_2010-2011.csv"
            active_view.parent.mkdir(parents=True)
            active_view.write_bytes(b"old-active-bytes")
            body = _valid_csv_bytes()
            response = _csv_response(
                BULK_URL_TEMPLATE.format(fiscal_year="2010-2011"),
                "2010-2011",
                content=body,
            )
            first = module.capture_bulk_snapshot(
                **self._capture_kwargs(root, response)
            )
            self.assertEqual(first.status, "captured")
            self.assertIsNotNone(first.evidence_path)
            self.assertIsNotNone(first.metadata_path)
            self.assertTrue(first.evidence_path.is_file())
            self.assertEqual(first.evidence_path.read_bytes(), body)
            self.assertEqual(active_view.read_bytes(), b"old-active-bytes")

            metadata_before = first.metadata_path.read_bytes()
            evidence_files_before = sorted(
                path.relative_to(root / "evidence")
                for path in (root / "evidence").rglob("*")
                if path.is_file()
            )
            repeat_kwargs = self._capture_kwargs(root, response)
            repeat_kwargs["captured_at"] = "2026-07-20T00:00:00+00:00"
            second = module.capture_bulk_snapshot(**repeat_kwargs)
            self.assertEqual(second.status, "unchanged")
            self.assertEqual(second.sha256, first.sha256)
            self.assertEqual(second.evidence_path, first.evidence_path)
            self.assertEqual(second.metadata_path, first.metadata_path)
            self.assertEqual(first.metadata_path.read_bytes(), metadata_before)
            self.assertEqual(
                sorted(
                    path.relative_to(root / "evidence")
                    for path in (root / "evidence").rglob("*")
                    if path.is_file()
                ),
                evidence_files_before,
            )
            self.assertEqual(active_view.read_bytes(), b"old-active-bytes")

            metadata = json.loads(first.metadata_path.read_text(encoding="utf-8"))
            self.assertEqual(metadata["fiscal_year"], "2010-2011")
            self.assertEqual(metadata["source_url"], response.url)
            self.assertEqual(metadata["requested_url"], response.url)
            self.assertEqual(metadata["final_url"], response.url)
            self.assertEqual(metadata["redirect_chain"], [response.url])
            self.assertEqual(metadata["captured_at"], CAPTURE_TIME)
            self.assertEqual(metadata["sha256"], hashlib.sha256(body).hexdigest())
            self.assertEqual(metadata["byte_length"], len(body))
            self.assertEqual(metadata["http_status"], 200)
            self.assertEqual(metadata["content_type"], "text/csv; charset=latin-1")
            self.assertEqual(
                metadata["content_disposition"],
                'attachment; filename="contratos_2010-2011.csv"',
            )
            self.assertEqual(metadata["encoding"], "latin-1")

    def test_same_bytes_with_conflicting_source_metadata_fail_closed(self):
        module = _require_pipeline_module("capture_bulk_snapshot")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            body = _valid_csv_bytes()
            original_url = BULK_URL_TEMPLATE.format(fiscal_year="2010-2011")
            original = module.capture_bulk_snapshot(
                **self._capture_kwargs(
                    root,
                    _csv_response(original_url, "2010-2011", content=body),
                )
            )
            self.assertEqual(original.status, "captured")
            metadata_before = original.metadata_path.read_bytes()

            moved_url = (
                f"https://{OFFICIAL_HOST}/records/bulk/"
                "contratos_2010-2011.csv"
            )
            conflict_kwargs = self._capture_kwargs(
                root,
                _csv_response(moved_url, "2010-2011", content=body),
            )
            conflict_kwargs["source_url"] = moved_url
            conflict_kwargs["captured_at"] = "2026-07-20T00:00:00+00:00"
            conflict = module.capture_bulk_snapshot(**conflict_kwargs)

            self.assertEqual(conflict.status, "rejected")
            self.assertEqual(conflict.reason, "immutable metadata conflict")
            self.assertEqual(conflict.evidence_path, original.evidence_path)
            self.assertEqual(conflict.metadata_path, original.metadata_path)
            self.assertEqual(original.evidence_path.read_bytes(), body)
            self.assertEqual(original.metadata_path.read_bytes(), metadata_before)

    def test_changed_same_year_keeps_prior_evidence_and_explicit_promotion_selects_bytes(self):
        module = _require_pipeline_module("capture_bulk_snapshot")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            active_view = root / "raw" / "contratos_2010-2011.csv"
            active_view.parent.mkdir(parents=True)
            active_view.write_bytes(b"active-must-not-change-during-capture")
            original_body = _valid_csv_bytes()
            original_response = _csv_response(
                BULK_URL_TEMPLATE.format(fiscal_year="2010-2011"),
                "2010-2011",
                content=original_body,
            )
            original = module.capture_bulk_snapshot(
                **self._capture_kwargs(root, original_response)
            )
            changed_body = original_body.replace(b"1500.00", b"1501.00", 1)
            changed_response = _csv_response(
                BULK_URL_TEMPLATE.format(fiscal_year="2010-2011"),
                "2010-2011",
                content=changed_body,
            )
            changed = module.capture_bulk_snapshot(
                **self._capture_kwargs(root, changed_response)
            )
            self.assertEqual(changed.status, "captured")
            self.assertNotEqual(changed.sha256, original.sha256)
            self.assertNotEqual(changed.evidence_path, original.evidence_path)
            self.assertEqual(original.evidence_path.read_bytes(), original_body)
            self.assertEqual(changed.evidence_path.read_bytes(), changed_body)
            self.assertEqual(
                active_view.read_bytes(),
                b"active-must-not-change-during-capture",
            )

            promoted = module.promote_bulk_snapshot(
                evidence_path=changed.evidence_path,
                active_view=active_view,
            )
            self.assertEqual(Path(promoted), active_view)
            self.assertEqual(active_view.read_bytes(), changed.evidence_path.read_bytes())
            self.assertEqual(active_view.read_bytes(), changed_body)
            self.assertEqual(original.evidence_path.read_bytes(), original_body)

    def test_oversized_stream_is_quarantined_as_bounded_truncated_prefix(self):
        module = _require_pipeline_module("capture_bulk_snapshot")
        fiscal_year = "2010-2011"
        url = BULK_URL_TEMPLATE.format(fiscal_year=fiscal_year)
        response = StreamingResponse(
            headers={
                "Content-Type": "text/csv",
                "Content-Disposition": (
                    f'attachment; filename="contratos_{fiscal_year}.csv"'
                ),
            },
            chunks=(b"1234", b"x" * 1000000, b"must-not-be-consumed"),
            url=url,
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = module.capture_bulk_snapshot(
                **self._capture_kwargs(root, response),
                max_bytes=8,
            )
            self.assertEqual(result.status, "invalid_payload")
            self.assertEqual(result.reason, "byte_limit_exceeded")
            self.assertTrue(result.quarantine_truncated)
            self.assertEqual(result.quarantine_path.read_bytes(), b"1234" + b"x" * 5)
            self.assertLessEqual(result.quarantine_path.stat().st_size, 9)
            self.assertIsNone(result.evidence_path)
            self.assertFalse(response.content_accessed)
            self.assertEqual(response.iteration_count, 2)

    def test_nonzero_capture_offset_is_rejected_after_quarantine(self):
        module = _require_pipeline_module("capture_bulk_snapshot")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            response = _csv_response(
                BULK_URL_TEMPLATE.format(fiscal_year="2010-2011"),
                "2010-2011",
            )
            kwargs = self._capture_kwargs(root, response)
            kwargs["captured_at"] = "2026-07-19T05:00:00+05:00"
            result = module.capture_bulk_snapshot(**kwargs)
            self.assertEqual(result.status, "rejected")
            self.assertEqual(result.reason, "captured_at_must_be_utc")
            self.assertTrue(result.quarantine_path.is_file())
            self.assertEqual(result.quarantine_path.read_bytes(), response.content)
            self.assertFalse((root / "evidence").exists())

    def test_capture_rejects_symlinked_ancestor_without_writing_through_it(self):
        module = _require_pipeline_module("capture_bulk_snapshot")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            real_quarantine = root / "real-quarantine"
            real_quarantine.mkdir()
            linked_parent = root / "quarantine-link"
            linked_parent.symlink_to(real_quarantine, target_is_directory=True)
            response = _csv_response(
                BULK_URL_TEMPLATE.format(fiscal_year="2010-2011"),
                "2010-2011",
            )
            kwargs = self._capture_kwargs(root, response)
            kwargs["quarantine_dir"] = linked_parent / "nested"
            with self.assertRaises(ValueError):
                module.capture_bulk_snapshot(**kwargs)
            self.assertEqual(list(real_quarantine.iterdir()), [])

    def test_promotion_rejects_symlinked_evidence_ancestor(self):
        module = _require_pipeline_module("capture_bulk_snapshot")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            real_evidence = root / "real-evidence"
            real_evidence.mkdir()
            evidence = real_evidence / "2010-2011" / "evidence.csv"
            evidence.parent.mkdir()
            evidence.write_bytes(b"evidence")
            linked_parent = root / "evidence-link"
            linked_parent.symlink_to(real_evidence, target_is_directory=True)
            active_view = root / "raw" / "active.csv"
            with self.assertRaises(ValueError):
                module.promote_bulk_snapshot(
                    evidence_path=linked_parent / "2010-2011" / "evidence.csv",
                    active_view=active_view,
                )
            self.assertFalse(active_view.exists())


if __name__ == "__main__":
    unittest.main()
