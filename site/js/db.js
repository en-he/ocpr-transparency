/**
 * db.js — sql.js wrapper with manifest-driven loading and IndexedDB caching.
 *
 * The site serves a smaller browser-focused SQLite database while keeping the
 * full database downloadable as open data.
 */

const MANIFEST_URL = "data-manifest.json";
const IDB_NAME = "ocpr-transparency";
const IDB_STORE = "db-cache";
const IDB_VERSION = 2;
const CACHE_KEY = "browser-db";
const PAGE_SIZE = 50;

const DEFAULT_MANIFEST = {
    generated_at: null,
    row_count: null,
    total_amount: null,
    fiscal_years: [],
    archived_csv_fiscal_years: [],
    dashboard: {
        top_contractors: [],
        top_entities: [],
        yearly_spending: [],
    },
    raw_csv_base_url: "https://github.com/en-he/ocpr-transparency/raw/main/data/raw/",
    recovery_targets: [],
    browser_db: {
        url: "contratos.db.gz",
        parts: null,
        sha256: "legacy-browser-db",
    },
    full_download_db: {
        url: "https://github.com/en-he/ocpr-transparency/releases/download/data-latest/contratos-full.db.gz",
        sha256: null,
    },
};

let _db = null;
let _manifest = DEFAULT_MANIFEST;
const _distinctCache = new Map();

async function loadManifest() {
    try {
        const response = await fetch(MANIFEST_URL, { cache: "no-store" });
        if (!response.ok) {
            throw new Error(`Failed to fetch ${MANIFEST_URL}: ${response.status}`);
        }
        const loaded = await response.json();
        return {
            ...DEFAULT_MANIFEST,
            ...loaded,
            dashboard: {
                ...DEFAULT_MANIFEST.dashboard,
                ...(loaded.dashboard || {}),
                top_contractors: Array.isArray(loaded.dashboard?.top_contractors)
                    ? loaded.dashboard.top_contractors
                    : DEFAULT_MANIFEST.dashboard.top_contractors,
                top_entities: Array.isArray(loaded.dashboard?.top_entities)
                    ? loaded.dashboard.top_entities
                    : DEFAULT_MANIFEST.dashboard.top_entities,
                yearly_spending: Array.isArray(loaded.dashboard?.yearly_spending)
                    ? loaded.dashboard.yearly_spending
                    : DEFAULT_MANIFEST.dashboard.yearly_spending,
            },
            archived_csv_fiscal_years: Array.isArray(loaded.archived_csv_fiscal_years)
                ? loaded.archived_csv_fiscal_years
                : DEFAULT_MANIFEST.archived_csv_fiscal_years,
            recovery_targets: Array.isArray(loaded.recovery_targets)
                ? loaded.recovery_targets
                : DEFAULT_MANIFEST.recovery_targets,
            browser_db: {
                ...DEFAULT_MANIFEST.browser_db,
                ...(loaded.browser_db || {}),
                parts: Array.isArray(loaded.browser_db?.parts)
                    ? loaded.browser_db.parts
                    : DEFAULT_MANIFEST.browser_db.parts,
            },
            full_download_db: {
                ...DEFAULT_MANIFEST.full_download_db,
                ...(loaded.full_download_db || {}),
            },
        };
    } catch (err) {
        console.warn("Falling back to default manifest:", err);
        return { ...DEFAULT_MANIFEST };
    }
}

function getManifest() {
    return _manifest;
}

async function initDB(onStatus) {
    onStatus("Initializing database...");
    _manifest = await loadManifest();
    _distinctCache.clear();
    _ftsKeywordSupport = null;

    const SQL = await initSqlJs({
        locateFile: file => `https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.10.3/${file}`,
    });

    const browserDb = _manifest.browser_db || DEFAULT_MANIFEST.browser_db;
    const browserHash = browserDb.sha256 || "legacy-browser-db";

    onStatus("Checking local cache...");
    let dbBytes = await loadFromCache(browserHash);

    if (!dbBytes) {
        const compressed = await fetchBrowserDb(browserDb, onStatus);
        onStatus("Decompressing...");
        dbBytes = await decompress(compressed);
        await saveToCache(browserHash, dbBytes);
    } else {
        onStatus("Loaded from cache");
    }

    onStatus("Opening database...");
    _db = new SQL.Database(dbBytes);
    registerSqlHelpers(_db);
    _db.run("PRAGMA cache_size = -32000");
}

async function decompress(compressed) {
    if (typeof DecompressionStream !== "undefined") {
        const ds = new DecompressionStream("gzip");
        const blob = new Blob([compressed]);
        const stream = blob.stream().pipeThrough(ds);
        const result = await new Response(stream).arrayBuffer();
        return new Uint8Array(result);
    }
    throw new Error("DecompressionStream not supported. Please use a modern browser.");
}

async function fetchBrowserDb(browserDb, onStatus) {
    if (Array.isArray(browserDb.parts) && browserDb.parts.length > 0) {
        const chunks = [];
        let totalBytes = 0;

        for (let index = 0; index < browserDb.parts.length; index += 1) {
            const partUrl = browserDb.parts[index];
            onStatus(`Downloading contracts (${index + 1}/${browserDb.parts.length})...`);
            const response = await fetch(partUrl, { cache: "no-store" });
            if (!response.ok) {
                throw new Error(`Failed to fetch ${partUrl}: ${response.status}`);
            }

            const chunk = new Uint8Array(await response.arrayBuffer());
            chunks.push(chunk);
            totalBytes += chunk.byteLength;
        }

        return concatUint8Arrays(chunks, totalBytes);
    }

    if (!browserDb.url) {
        throw new Error("Browser DB download URL missing from manifest.");
    }

    onStatus("Downloading contracts...");
    const response = await fetch(browserDb.url, { cache: "no-store" });
    if (!response.ok) {
        throw new Error(`Failed to fetch ${browserDb.url}: ${response.status}`);
    }
    return new Uint8Array(await response.arrayBuffer());
}

function concatUint8Arrays(chunks, totalBytes = null) {
    const size = totalBytes == null
        ? chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0)
        : totalBytes;
    const merged = new Uint8Array(size);
    let offset = 0;

    for (const chunk of chunks) {
        merged.set(chunk, offset);
        offset += chunk.byteLength;
    }

    return merged;
}

function openIDB() {
    return new Promise((resolve, reject) => {
        const req = indexedDB.open(IDB_NAME, IDB_VERSION);
        req.onupgradeneeded = () => {
            if (!req.result.objectStoreNames.contains(IDB_STORE)) {
                req.result.createObjectStore(IDB_STORE);
            }
        };
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
    });
}

async function loadFromCache(expectedHash) {
    try {
        const idb = await openIDB();
        return await new Promise(resolve => {
            const tx = idb.transaction(IDB_STORE, "readonly");
            const req = tx.objectStore(IDB_STORE).get(CACHE_KEY);
            req.onsuccess = () => {
                const cached = req.result;
                if (cached && cached.hash === expectedHash && cached.bytes) {
                    resolve(cached.bytes);
                } else {
                    resolve(null);
                }
            };
            req.onerror = () => resolve(null);
        });
    } catch {
        return null;
    }
}

async function saveToCache(hash, bytes) {
    try {
        const idb = await openIDB();
        const tx = idb.transaction(IDB_STORE, "readwrite");
        tx.objectStore(IDB_STORE).put({ hash, bytes }, CACHE_KEY);
    } catch {
        // Cache failure is non-fatal.
    }
}

async function clearCache() {
    try {
        const idb = await openIDB();
        const tx = idb.transaction(IDB_STORE, "readwrite");
        tx.objectStore(IDB_STORE).clear();
    } catch {
        // ignore
    }
}

function query(sql, params = []) {
    const stmt = _db.prepare(sql);
    stmt.bind(params);
    const rows = [];
    while (stmt.step()) {
        const row = stmt.getAsObject();
        if (Object.prototype.hasOwnProperty.call(row, "amendment")) {
            row.amendment = normalizeAmendmentValue(row.amendment);
        }
        rows.push(row);
    }
    stmt.free();
    return rows;
}

function queryScalar(sql, params = []) {
    const result = _db.exec(sql, params);
    if (result.length > 0 && result[0].values.length > 0) {
        return result[0].values[0][0];
    }
    return null;
}

const CONTRACTOR_ALIAS_PATTERNS = [
    /\bA\s+DIVISION\s+OF\b.*$/u,
    /\bDIVISION\s+OF\b.*$/u,
    /\bD\s*B\s*A\b.*$/u,
    /\bA\s*K\s*A\b.*$/u,
    /\bH\s*N\s*C\b.*$/u,
];

const CONTRACTOR_STOPWORDS = new Set([
    "INC",
    "INCORPORATED",
    "LLC",
    "LLLP",
    "LLP",
    "LP",
    "LTD",
    "LIMITED",
    "CORP",
    "CORPORATION",
    "CO",
    "COMPANY",
    "PSC",
    "CSP",
    "PC",
    "SE",
    "SC",
    "US",
    "USA",
    "THE",
    "OF",
    "FOR",
    "DE",
    "DEL",
    "LA",
    "LAS",
    "LOS",
    "EL",
    "PARA",
    "Y",
    "AND",
    "ING",
    "INGENIERO",
]);

const CONTRACTOR_COMPACT_SUFFIXES = [
    "INCORPORATED",
    "CORPORATION",
    "COMPANY",
    "LIMITED",
    "LLLP",
    "LLC",
    "LLP",
    "CORP",
    "LTD",
    "PSC",
    "CSP",
    "INC",
];

const CONTRACTOR_SPACED_SUFFIX_PATTERNS = [
    [/\bL\s+L\s+L\s+P\b/gu, "LLLP"],
    [/\bL\s+L\s+C\b/gu, "LLC"],
    [/\bL\s+L\s+P\b/gu, "LLP"],
    [/\bP\s+S\s+C\b/gu, "PSC"],
    [/\bC\s+S\s+P\b/gu, "CSP"],
    [/\bP\s+C\b/gu, "PC"],
    [/\bS\s+C\b/gu, "SC"],
    [/\bS\s+E\b/gu, "SE"],
];

const LEADING_CONTRACTOR_TITLE_PATTERN = /^(?:ING|INGENIERO)\b\s*/u;

const CONTRACTOR_FAMILY_OVERRIDES = new Map([
    ["AUTORIDADF FINANCIAMIENTO INFRAESTRU", "AUTORIDAD FINANCIAMIENTO INFRAESTRUCTURA PUERTO RICO"],
    ["MAGLEZ ENGINEERINGS CONTRACTORS", "MAGLEZ ENGINEERING CONTRACTORS"],
    ["CONSTRUCCIONES VIVI AGREDADO", "CONSTRUCCIONES VIVI AGREGADOS"],
    ["CONSTRUCCIONES VIVI AGREGADO", "CONSTRUCCIONES VIVI AGREGADOS"],
    ["CONSTRUCCIONES VIVI AGRAGADOS", "CONSTRUCCIONES VIVI AGREGADOS"],
    ["BERMUDEZLONGODIAZ MASSO", "BERMUDEZ LONGO DIAZ MASSO"],
    ["DESING BUILD", "DESIGN BUILD"],
    ["JOSEPH HARRISON FLORESDBAHARISON CONSULTING", "JOSEPH HARRISON FLORES"],
    ["MUNICIPIO VIEQUES CCD", "MUNICIPIO VIEQUES"],
    ["MUNICIPIO SAN LOENZO", "MUNICIPIO SAN LORENZO"],
    ["AUTORIDAD FINANCIAMIENTO INFRAESTRUC", "AUTORIDAD FINANCIAMIENTO INFRAESTRUCTURA PUERTO RICO"],
    ["J F BUILDING LEASE MAINTENANCE", "JF BUILDING LEASE MAINTENANCE"],
    ["ISIDRO M MARTINEZ GILORMINI", "MARTINEZ GILORMINI ISIDRO M"],
    ["ADMINISTRACION COMPENSACIONES POR ACCIDENTES", "ADMINISTRACION COMPENSACIONES POR ACCIDENTES AUTOMOVILES"],
    ["CANCIO NADAL RIVERA", "CANCIONADAL RIVERA"],
    ["AQUINO CORDOVA ALFARO", "AQUINO CORDOVAALFARO"],
    ["RICHARD SANTOS GARCIA MA", "RICHARD SANTOS GARCIAMA"],
    ["UNIVERSITY PUERTO RICO PARKING SYSTEM", "UNIVERSIDA PUERTO RICO PARKING SYSTEM"],
    ["NAIOSCALY CRUZ PONCE", "CRUZ PONCE NAIOSCALY"],
    ["GIOVANY RIVERA CARRERO", "RIVERA CARRERO GIOVANY"],
    ["A1 GENERATOR SERVICES", "AI GENERATOR SERVICES"],
    ["T P CONSULTING", "QUANTUM HEALTH CONSULTING"],
    ["INTEGRA", "INTEGRA DESIGN GROUP"],
]);

const CONTRACTOR_SQL_CLEANUPS = [
    ["CHAR(0)", "' '"],
    ["'.'", "' '"],
    ["','", "' '"],
    ["';'", "' '"],
    ["':'", "' '"],
    ["'('", "' '"],
    ["')'", "' '"],
    ["'/'", "' '"],
    ["'-'", "' '"],
];

let _hasNormalizeContractorFamilySqlFunction = false;


function applySqlReplacements(expr, replacements) {
    return replacements.reduce(
        (sql, [needle, replacement]) => `REPLACE(${sql}, ${needle}, ${replacement})`,
        expr
    );
}

function normalizeContractorFamily(value) {
    if (!value) return "";

    let normalized = String(value)
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .replace(/[\u0000.,;:()/-]/g, " ")
        .replace(/&/g, " ")
        .toUpperCase();

    for (const [pattern, replacement] of CONTRACTOR_SPACED_SUFFIX_PATTERNS) {
        normalized = normalized.replace(pattern, replacement);
    }

    for (const suffix of CONTRACTOR_COMPACT_SUFFIXES) {
        normalized = normalized.replace(
            new RegExp(`([A-Z0-9])${suffix}\\b`, "gu"),
            `$1 ${suffix}`
        );
    }

    normalized = normalized
        .replace(/\bP\s*R\b/gu, "PUERTO RICO")
        .replace(/\s+/g, " ")
        .trim();

    for (const pattern of CONTRACTOR_ALIAS_PATTERNS) {
        normalized = normalized.replace(pattern, "").trim();
    }

    normalized = normalized.replace(LEADING_CONTRACTOR_TITLE_PATTERN, "").trim();

    const tokens = normalized
        .split(" ")
        .filter(token => token && !CONTRACTOR_STOPWORDS.has(token));

    const family = tokens.join(" ").trim();
    return CONTRACTOR_FAMILY_OVERRIDES.get(family) || family;
}

function registerSqlHelpers(db) {
    _hasNormalizeContractorFamilySqlFunction = false;

    if (!db) return;

    const registrar = typeof db.create_function === "function"
        ? db.create_function.bind(db)
        : (typeof db.createFunction === "function" ? db.createFunction.bind(db) : null);

    if (!registrar) return;

    registrar("normalize_contractor_family", normalizeContractorFamily);
    _hasNormalizeContractorFamilySqlFunction = true;
}

function normalizeAmendmentValue(value) {
    if (value == null) return "";
    return String(value)
        .replace(/\u0000/g, "")
        .trim();
}

function amendmentValueExpr(alias = "c") {
    return `CASE
        WHEN REPLACE(COALESCE(HEX(${alias}.amendment), ''), '00', '') = '' THEN ''
        ELSE TRIM(COALESCE(${alias}.amendment, ''))
    END`;
}

function blankAmendmentExpr(alias = "c") {
    return `(REPLACE(COALESCE(HEX(${alias}.amendment), ''), '00', '') = ''
        OR NULLIF(TRIM(COALESCE(${alias}.amendment, '')), '') IS NULL
        OR UPPER(TRIM(COALESCE(${alias}.amendment, ''))) = 'ORIGINAL')`;
}

function getSearchStateReference() {
    const params = new URLSearchParams(window.location.search);
    const savedBack = params.get("back");
    if (savedBack) return String(savedBack).replace(/^#/, "");
    const liveBackRef = window.__ocprCurrentSearchRef;
    if (liveBackRef) return String(liveBackRef).replace(/^#/, "");
    return window.location.hash.replace(/^#/, "");
}

function buildContractUrl(contractId, backRef = getSearchStateReference()) {
    const params = new URLSearchParams();
    params.set("id", contractId);

    const normalizedBackRef = String(backRef || "").replace(/^#/, "");
    if (normalizedBackRef) {
        params.set("back", normalizedBackRef);
    }

    return `contract.html?${params.toString()}`;
}

function buildFamilyContractUrl(contractNumber, entity, contractor, backRef = getSearchStateReference()) {
    const params = new URLSearchParams();
    params.set("contract_number", contractNumber || "");
    params.set("entity", entity || "");
    params.set("contractor", contractor || "");

    const normalizedBackRef = String(backRef || "").replace(/^#/, "");
    if (normalizedBackRef) {
        params.set("back", normalizedBackRef);
    }

    return `contract.html?${params.toString()}`;
}

function buildSearchUrl(backRef = getSearchStateReference()) {
    const normalizedBackRef = String(backRef || "").replace(/^#/, "");
    return normalizedBackRef ? `index.html#${normalizedBackRef}` : "index.html";
}

function getRecoveryTargets() {
    return Array.isArray(getManifest().recovery_targets)
        ? getManifest().recovery_targets
        : [];
}

function normalizeRecoveryStatus(value) {
    return String(value || "").trim().toLowerCase();
}

function getRecoveryTarget(contractNumber, entity, contractor) {
    const normalizedContractNumber = normalizeLookupValue(contractNumber);
    const normalizedEntity = normalizeLookupValue(entity);
    const normalizedContractorFamily = normalizeContractorFamily(contractor);

    if (!normalizedContractNumber || !normalizedEntity) {
        return null;
    }

    return getRecoveryTargets().find(target => (
        normalizeLookupValue(target.contract_number) === normalizedContractNumber &&
        normalizeLookupValue(target.entity) === normalizedEntity &&
        (
            !normalizedContractorFamily ||
            normalizeContractorFamily(target.contractor) === normalizedContractorFamily
        )
    )) || null;
}

function buildFamilyDetailUrlForRow(row, backRef = getSearchStateReference()) {
    const familySize = Number(row.family_size || 0);
    const familyHasOriginal = row.family_has_original == null
        ? null
        : Number(row.family_has_original) === 1;

    if (familySize > 1 || familyHasOriginal === false) {
        return buildFamilyContractUrl(row.contract_number, row.entity, row.contractor, backRef);
    }
    return buildContractUrl(row.id, backRef);
}

function contractorFamilyExpr(alias = "c") {
    const col = `${alias}.contractor`;
    if (_hasNormalizeContractorFamilySqlFunction) {
        return `normalize_contractor_family(${col})`;
    }
    const cleaned = applySqlReplacements(
        `UPPER(COALESCE(${col}, ''))`,
        CONTRACTOR_SQL_CLEANUPS
    );
    return `TRIM(REPLACE(REPLACE(REPLACE(${cleaned}, '  ', ' '), '  ', ' '), '  ', ' '))`;
}

function escapeLikeValue(value) {
    return String(value).replace(/[\\%_]/g, "\\$&");
}

function tokenizeKeywordSearch(value) {
    return String(value || "")
        .normalize("NFC")
        .replace(/['"*()]/g, " ")
        .replace(/[^0-9A-Za-z\u00C0-\u024F]+/g, " ")
        .trim()
        .split(/\s+/)
        .filter(Boolean);
}

let _ftsKeywordSupport = null;

function hasFtsKeywordSupport() {
    if (_ftsKeywordSupport != null) {
        return _ftsKeywordSupport;
    }

    if (!_db) {
        _ftsKeywordSupport = false;
        return _ftsKeywordSupport;
    }

    try {
        queryScalar(
            "SELECT COUNT(*) FROM contracts_fts WHERE contracts_fts MATCH ?",
            ["zzzzunlikelytoken*"]
        );
        _ftsKeywordSupport = true;
    } catch {
        _ftsKeywordSupport = false;
    }

    return _ftsKeywordSupport;
}

function buildKeywordSubstringParts(tokens, alias = "c") {
    const searchableColumns = [
        "contract_number",
        "entity",
        "contractor",
        "service_category",
        "service_type",
    ];
    const tokenClauses = [];
    const tokenParams = [];

    for (const token of tokens) {
        const likePattern = `%${escapeLikeValue(token.toLocaleUpperCase("es"))}%`;
        tokenClauses.push(`(${
            searchableColumns
                .map(column => `UPPER(COALESCE(${alias}.${column}, '')) LIKE ? ESCAPE '\\'`)
                .join(" OR ")
        })`);
        tokenParams.push(...searchableColumns.map(() => likePattern));
    }

    return {
        clause: tokenClauses.join(" AND "),
        params: tokenParams,
    };
}

function buildKeywordSearchParts(rawKeyword, alias = "c") {
    const tokens = tokenizeKeywordSearch(rawKeyword);
    if (!tokens.length) return null;

    const substringSearch = buildKeywordSubstringParts(tokens, alias);

    if (!hasFtsKeywordSupport()) {
        return substringSearch;
    }

    const ftsQuery = tokens.map(token => `${token}*`).join(" ");

    return {
        clause: `(
            ${alias}.rowid IN (
                SELECT rowid
                FROM contracts_fts
                WHERE contracts_fts MATCH ?
            )
            OR (
                NOT EXISTS (
                    SELECT 1
                    FROM contracts_fts
                    WHERE contracts_fts MATCH ?
                )
                AND (${substringSearch.clause})
            )
        )`,
        params: [
            ftsQuery,
            ftsQuery,
            ...substringSearch.params,
        ],
    };
}

function normalizeLookupValue(value) {
    if (value == null) return "";
    return String(value)
        .normalize("NFC")
        .trim()
        .toLocaleUpperCase("es");
}

function resolveExactOrPrefixMatches(column, rawValue) {
    const normalized = normalizeLookupValue(rawValue);
    if (!normalized) {
        return { mode: "none", matches: [] };
    }

    const options = getDistinct(column);
    const normalizedOptions = options.map(value => ({
        value,
        normalized: normalizeLookupValue(value),
    }));
    const exact = normalizedOptions.find(option => option.normalized === normalized);

    if (exact) {
        return { mode: "exact", matches: [exact.value] };
    }

    const matches = normalizedOptions
        .filter(option => option.normalized.startsWith(normalized))
        .map(option => option.value);

    return { mode: matches.length ? "prefix" : "none", matches };
}

function buildFilteredQueryParts(filters = {}) {
    const baseWhere = [];
    const params = [];

    if (filters.keyword) {
        const keywordSearch = buildKeywordSearchParts(filters.keyword, "c");
        if (keywordSearch) {
            baseWhere.push(keywordSearch.clause);
            params.push(...keywordSearch.params);
        }
    }

    if (filters.contractor) {
        const term = filters.contractor;
        if (term.includes("*")) {
            baseWhere.push("c.contractor LIKE ?");
            params.push(term.replace(/\*/g, "%"));
        } else {
            baseWhere.push("(c.contractor LIKE ? OR c.contractor LIKE ?)");
            params.push(term + "%");
            params.push("% " + term + "%");
        }
    }

    if (filters.entity) {
        const entityMatches = resolveExactOrPrefixMatches("entity", filters.entity);
        if (entityMatches.mode === "exact") {
            baseWhere.push("c.entity = ?");
            params.push(entityMatches.matches[0]);
        } else if (entityMatches.mode === "prefix") {
            baseWhere.push(`c.entity IN (${entityMatches.matches.map(() => "?").join(", ")})`);
            params.push(...entityMatches.matches);
        } else {
            baseWhere.push("1 = 0");
        }
    }

    if (filters.amountMin) {
        baseWhere.push("c.amount >= ?");
        params.push(Number(filters.amountMin));
    }

    if (filters.amountMax) {
        baseWhere.push("c.amount <= ?");
        params.push(Number(filters.amountMax));
    }

    if (filters.dateFrom) {
        baseWhere.push("c.award_date >= ?");
        params.push(filters.dateFrom);
    }

    if (filters.dateTo) {
        baseWhere.push("c.award_date <= ?");
        params.push(filters.dateTo);
    }

    if (filters.validFrom) {
        baseWhere.push("c.valid_from >= ?");
        params.push(filters.validFrom);
    }

    if (filters.validTo) {
        baseWhere.push("c.valid_to <= ?");
        params.push(filters.validTo);
    }

    if (filters.category) {
        baseWhere.push("c.service_category = ?");
        params.push(filters.category);
    }

    if (filters.serviceType) {
        const serviceTypeMatches = resolveExactOrPrefixMatches("service_type", filters.serviceType);
        if (serviceTypeMatches.mode === "exact") {
            baseWhere.push("c.service_type = ?");
            params.push(serviceTypeMatches.matches[0]);
        } else if (serviceTypeMatches.mode === "prefix") {
            baseWhere.push(`c.service_type IN (${serviceTypeMatches.matches.map(() => "?").join(", ")})`);
            params.push(...serviceTypeMatches.matches);
        } else {
            baseWhere.push("1 = 0");
        }
    }

    if (filters.fiscalYear) {
        baseWhere.push("c.fiscal_year = ?");
        params.push(filters.fiscalYear);
    }

    const baseWhereClause = baseWhere.length ? "WHERE " + baseWhere.join(" AND ") : "";
    let contractNumberClause = "";
    if (filters.contractNumber) {
        const normalized = String(filters.contractNumber).trim().toUpperCase();
        contractNumberClause = `
            WHERE (
                UPPER(COALESCE(bf.contract_number, '')) = ?
                OR (
                    NOT EXISTS (
                        SELECT 1
                        FROM base_filtered exact_match
                        WHERE UPPER(COALESCE(exact_match.contract_number, '')) = ?
                    )
                    AND UPPER(COALESCE(bf.contract_number, '')) LIKE ? ESCAPE '\\'
                )
            )
        `;
        params.push(normalized, normalized, escapeLikeValue(normalized) + "%");
    }

    const filteredCte = `
        WITH base_filtered AS (
            SELECT
                c.*,
                ${contractorFamilyExpr("c")} AS contractor_family
            FROM contracts c
            ${baseWhereClause}
        ),
        filtered AS (
            SELECT *
            FROM base_filtered bf
            ${contractNumberClause}
        )
    `;

    return { filteredCte, params };
}

function buildFamilyQueryParts(filters = {}) {
    const { filteredCte, params } = buildFilteredQueryParts(filters);
    const representativeOrderExpr = `
        CASE
            WHEN ${blankAmendmentExpr("f")} THEN 0
            ELSE 1
        END ASC,
        f.award_date ASC,
        f.id ASC
    `;

    const familyCte = `
        ${filteredCte},
        ranked AS (
            SELECT
                f.*,
                ROW_NUMBER() OVER (
                    PARTITION BY f.contract_number, f.entity, f.contractor_family
                    ORDER BY ${representativeOrderExpr}
                ) AS representative_row
            FROM filtered f
        ),
        families AS (
            SELECT
                contract_number,
                entity,
                contractor_family,
                COUNT(*) AS family_size,
                SUM(COALESCE(amount, 0)) AS family_total_amount,
                MAX(
                    CASE
                        WHEN ${blankAmendmentExpr("ranked")} THEN 1
                        ELSE 0
                    END
                ) AS family_has_original,
                MAX(CASE WHEN representative_row = 1 THEN id END) AS representative_id,
                MAX(CASE WHEN representative_row = 1 THEN contractor END) AS contractor,
                MAX(CASE WHEN representative_row = 1 THEN service_category END) AS service_category,
                MAX(CASE WHEN representative_row = 1 THEN service_type END) AS service_type,
                MAX(CASE WHEN representative_row = 1 THEN fiscal_year END) AS fiscal_year,
                MAX(CASE WHEN representative_row = 1 THEN valid_from END) AS valid_from,
                MAX(CASE WHEN representative_row = 1 THEN valid_to END) AS valid_to,
                MAX(CASE WHEN representative_row = 1 THEN procurement_method END) AS procurement_method,
                MAX(CASE WHEN representative_row = 1 THEN fund_type END) AS fund_type,
                MAX(CASE WHEN representative_row = 1 THEN amount END) AS representative_amount,
                MAX(CASE WHEN representative_row = 1 THEN award_date END) AS representative_award_date,
                MIN(award_date) AS family_earliest_award_date
            FROM ranked
            GROUP BY contract_number, entity, contractor_family
        )
    `;

    return { familyCte, params };
}

function buildSearchQuery(filters, page = 1, sortCol = "award_date", sortDir = "DESC", options = {}) {
    const { familyCte, params } = buildFamilyQueryParts(filters);
    const validCols = ["contract_number", "contractor", "entity", "amount", "award_date", "service_category"];
    if (!validCols.includes(sortCol)) sortCol = "award_date";
    if (sortDir !== "ASC") sortDir = "DESC";

    const sortExprByCol = {
        contract_number: "contract_number",
        contractor: "contractor",
        entity: "entity",
        amount: "display_amount",
        award_date: "display_award_date",
        service_category: "service_category",
    };
    const sortExpr = sortExprByCol[sortCol] || "display_award_date";
    const limit = Number.isFinite(options.limit) ? options.limit : PAGE_SIZE;
    const offset = Number.isFinite(options.offset) ? options.offset : (page - 1) * PAGE_SIZE;

    const dataSql = `${familyCte}
        SELECT
            representative_id AS id,
            contract_number,
            contractor,
            entity,
            service_category,
            service_type,
            fiscal_year,
            valid_from,
            valid_to,
            procurement_method,
            fund_type,
            NULL AS amendment,
            family_size,
            family_total_amount,
            family_has_original,
            CASE
                WHEN family_has_original = 1 THEN representative_amount
                ELSE family_total_amount
            END AS display_amount,
            CASE
                WHEN family_has_original = 1 THEN representative_award_date
                ELSE family_earliest_award_date
            END AS display_award_date,
            CASE
                WHEN family_has_original = 1 THEN representative_amount
                ELSE family_total_amount
            END AS amount,
            CASE
                WHEN family_has_original = 1 THEN representative_award_date
                ELSE family_earliest_award_date
            END AS award_date
        FROM families
        ORDER BY ${sortExpr} ${sortDir} NULLS LAST
        LIMIT ${limit} OFFSET ${offset}`;

    const countSql = `${familyCte}
        SELECT COUNT(*)
        FROM families`;

    const sumSql = `${familyCte}
        SELECT COALESCE(SUM(family_total_amount), 0)
        FROM families`;

    return { dataSql, countSql, sumSql, params };
}

function buildDetailedQuery(filters, page = 1, sortCol = "award_date", sortDir = "DESC", options = {}) {
    const { filteredCte, params } = buildFilteredQueryParts(filters);
    const validCols = [
        "contract_number",
        "amendment",
        "contractor",
        "entity",
        "amount",
        "award_date",
        "valid_from",
        "valid_to",
        "service_category",
        "service_type",
        "fiscal_year",
    ];
    if (!validCols.includes(sortCol)) sortCol = "award_date";
    if (sortDir !== "ASC") sortDir = "DESC";

    const limit = Number.isFinite(options.limit) ? options.limit : PAGE_SIZE;
    const offset = Number.isFinite(options.offset) ? options.offset : (page - 1) * PAGE_SIZE;
    const sortExprByCol = {
        contract_number: "contract_number",
        amendment: "amendment",
        contractor: "contractor",
        entity: "entity",
        amount: "amount",
        award_date: "award_date",
        valid_from: "valid_from",
        valid_to: "valid_to",
        service_category: "service_category",
        service_type: "service_type",
        fiscal_year: "fiscal_year",
    };
    const sortExpr = sortExprByCol[sortCol] || "award_date";

    const dataSql = `${filteredCte}
        SELECT
            id,
            contract_number,
            entity,
            entity_number,
            contractor,
            amendment,
            service_category,
            service_type,
            amount,
            amount_receivable,
            award_date,
            valid_from,
            valid_to,
            procurement_method,
            fund_type,
            pco_number,
            cancelled,
            document_url,
            fiscal_year,
            source_type,
            source_url,
            source_contract_id
        FROM filtered
        ORDER BY ${sortExpr} ${sortDir} NULLS LAST, id ${sortDir}
        LIMIT ${limit} OFFSET ${offset}`;

    const countSql = `${filteredCte}
        SELECT COUNT(*)
        FROM filtered`;

    const sumSql = `${filteredCte}
        SELECT COALESCE(SUM(amount), 0)
        FROM filtered`;

    return { dataSql, countSql, sumSql, params };
}

function isOriginalAmendment(value) {
    const normalized = normalizeLookupValue(normalizeAmendmentValue(value));
    return normalized === "" || normalized === "ORIGINAL";
}

function compareFamilyMembers(a, b) {
    const amendmentA = normalizeAmendmentValue(a.amendment);
    const amendmentB = normalizeAmendmentValue(b.amendment);
    const originalA = isOriginalAmendment(amendmentA);
    const originalB = isOriginalAmendment(amendmentB);

    if (originalA !== originalB) {
        return originalA ? -1 : 1;
    }

    const awardDateA = a.award_date || "";
    const awardDateB = b.award_date || "";
    if (awardDateA !== awardDateB) {
        return awardDateA.localeCompare(awardDateB);
    }

    if (amendmentA !== amendmentB) {
        return amendmentA.localeCompare(amendmentB);
    }

    return Number(a.id || 0) - Number(b.id || 0);
}

function compareSummaryValues(left, right, sortDir) {
    const direction = sortDir === "ASC" ? 1 : -1;
    const leftEmpty = left == null || left === "";
    const rightEmpty = right == null || right === "";

    if (leftEmpty && rightEmpty) return 0;
    if (leftEmpty) return 1;
    if (rightEmpty) return -1;

    if (typeof left === "number" || typeof right === "number") {
        const numericLeft = Number(left || 0);
        const numericRight = Number(right || 0);
        if (numericLeft === numericRight) return 0;
        return numericLeft < numericRight ? -1 * direction : 1 * direction;
    }

    const compared = String(left).localeCompare(String(right), "es", { sensitivity: "base" });
    return compared * direction;
}

function compareSummaryRows(a, b, sortCol = "award_date", sortDir = "DESC") {
    const sortValues = {
        contract_number: [a.contract_number, b.contract_number],
        contractor: [a.contractor, b.contractor],
        entity: [a.entity, b.entity],
        amount: [Number(a.display_amount || a.amount || 0), Number(b.display_amount || b.amount || 0)],
        award_date: [a.display_award_date || a.award_date, b.display_award_date || b.award_date],
        service_category: [a.service_category, b.service_category],
    };

    const [left, right] = sortValues[sortCol] || sortValues.award_date;
    const primary = compareSummaryValues(left, right, sortDir);
    if (primary !== 0) return primary;

    return Number(a.id || 0) - Number(b.id || 0);
}

function buildMergedFamilySummaries(rows = []) {
    const groups = new Map();

    for (const row of rows) {
        const key = [
            row.contract_number || "",
            row.entity || "",
            normalizeContractorFamily(row.contractor),
        ].join("\u001F");

        if (!groups.has(key)) {
            groups.set(key, []);
        }
        groups.get(key).push(row);
    }

    return Array.from(groups.values()).map(groupRows => {
        const members = groupRows.slice().sort(compareFamilyMembers);
        const representative = members[0];
        const familyHasOriginal = members.some(row => isOriginalAmendment(row.amendment));
        const familyTotalAmount = members.reduce(
            (sum, row) => sum + Number(row.amount || 0),
            0
        );
        const earliestAwardDate = members.reduce((earliest, row) => {
            const awardDate = row.award_date || "";
            if (!awardDate) return earliest;
            if (!earliest || awardDate < earliest) return awardDate;
            return earliest;
        }, "");

        const displayAmount = familyHasOriginal
            ? Number(representative.amount || 0)
            : familyTotalAmount;
        const displayAwardDate = familyHasOriginal
            ? (representative.award_date || earliestAwardDate)
            : earliestAwardDate;

        return {
            id: representative.id,
            contract_number: representative.contract_number,
            contractor: representative.contractor,
            entity: representative.entity,
            service_category: representative.service_category,
            service_type: representative.service_type,
            fiscal_year: representative.fiscal_year,
            valid_from: representative.valid_from,
            valid_to: representative.valid_to,
            procurement_method: representative.procurement_method,
            fund_type: representative.fund_type,
            amendment: null,
            family_size: members.length,
            family_total_amount: familyTotalAmount,
            family_has_original: familyHasOriginal ? 1 : 0,
            display_amount: displayAmount,
            display_award_date: displayAwardDate,
            amount: displayAmount,
            award_date: displayAwardDate,
        };
    });
}

function searchContractFamilies(filters, page = 1, sortCol = "award_date", sortDir = "DESC") {
    const initialQuery = buildDetailedQuery(filters, 1, "award_date", "DESC", { limit: 1, offset: 0 });
    const detailedCount = Number(queryScalar(initialQuery.countSql, initialQuery.params) || 0);

    if (detailedCount === 0) {
        return {
            rows: [],
            totalCount: 0,
            totalAmount: 0,
            detailedCount: 0,
        };
    }

    const rawQuery = buildDetailedQuery(filters, 1, "award_date", "DESC", {
        limit: detailedCount,
        offset: 0,
    });
    const rawRows = query(rawQuery.dataSql, rawQuery.params);
    const mergedFamilies = buildMergedFamilySummaries(rawRows)
        .sort((left, right) => compareSummaryRows(left, right, sortCol, sortDir));
    const totalAmount = mergedFamilies.reduce(
        (sum, row) => sum + Number(row.family_total_amount || 0),
        0
    );
    const offset = (page - 1) * PAGE_SIZE;

    return {
        rows: mergedFamilies.slice(offset, offset + PAGE_SIZE),
        totalCount: mergedFamilies.length,
        totalAmount,
        detailedCount,
    };
}

function getDistinct(column) {
    const validCols = ["entity", "service_category", "service_type", "fiscal_year"];
    if (!validCols.includes(column)) return [];
    if (_distinctCache.has(column)) {
        return _distinctCache.get(column);
    }
    const values = query(
        `SELECT DISTINCT ${column} FROM contracts WHERE ${column} IS NOT NULL AND ${column} != '' ORDER BY ${column}`
    ).map(row => row[column]);
    _distinctCache.set(column, values);
    return values;
}

function hasManifestDashboardData() {
    const dashboard = getManifest().dashboard || {};
    return Boolean(
        (dashboard.top_contractors && dashboard.top_contractors.length) ||
        (dashboard.top_entities && dashboard.top_entities.length) ||
        (dashboard.yearly_spending && dashboard.yearly_spending.length)
    );
}

function getSitewideDashboardData() {
    if (hasManifestDashboardData()) {
        return getManifest().dashboard;
    }
    return getDashboardData({});
}

function getDashboardData(filters = {}) {
    const { familyCte, params } = buildFamilyQueryParts(filters);

    const topContractors = query(
        `${familyCte}
            SELECT
                MAX(contractor) AS name,
                COUNT(*) AS family_count,
                COALESCE(SUM(family_total_amount), 0) AS total_amount
            FROM families
            WHERE contractor_family IS NOT NULL
              AND contractor_family != ''
            GROUP BY contractor_family
            ORDER BY total_amount DESC, family_count DESC, name ASC
            LIMIT 5`,
        params
    );

    const topEntities = query(
        `${familyCte}
            SELECT
                entity AS name,
                COUNT(*) AS family_count,
                COALESCE(SUM(family_total_amount), 0) AS total_amount
            FROM families
            WHERE entity IS NOT NULL
              AND TRIM(entity) != ''
            GROUP BY entity
            ORDER BY total_amount DESC, family_count DESC, name ASC
            LIMIT 5`,
        params
    );

    const yearlySpending = query(
        `${familyCte}
            SELECT
                fiscal_year,
                COUNT(*) AS family_count,
                COALESCE(SUM(family_total_amount), 0) AS total_amount
            FROM families
            WHERE fiscal_year IS NOT NULL
              AND TRIM(fiscal_year) != ''
            GROUP BY fiscal_year
            ORDER BY fiscal_year ASC`,
        params
    );

    return {
        top_contractors: topContractors.map(row => ({
            name: row.name,
            family_count: Number(row.family_count || 0),
            total_amount: Number(row.total_amount || 0),
        })),
        top_entities: topEntities.map(row => ({
            name: row.name,
            family_count: Number(row.family_count || 0),
            total_amount: Number(row.total_amount || 0),
        })),
        yearly_spending: yearlySpending.map(row => ({
            fiscal_year: row.fiscal_year,
            family_count: Number(row.family_count || 0),
            total_amount: Number(row.total_amount || 0),
        })),
    };
}

function getContractById(contractId) {
    return query("SELECT * FROM contracts WHERE id = ?", [contractId])[0] || null;
}

function buildMissingOriginalPlaceholderContract(contractNumber, entity, contractor, familyRows = [], recoveryTarget = null) {
    const firstFamilyRow = familyRows[0] || {};
    const recoveryStatus = normalizeRecoveryStatus(recoveryTarget?.status);
    const showRecoveryMetadata = recoveryStatus && recoveryStatus !== "recovered";
    return {
        id: null,
        contract_number: contractNumber || firstFamilyRow.contract_number || null,
        entity: recoveryTarget?.entity || entity || firstFamilyRow.entity || null,
        entity_number: null,
        contractor: recoveryTarget?.contractor || contractor || firstFamilyRow.contractor || null,
        amendment: "",
        service_category: null,
        service_type: null,
        amount: null,
        amount_receivable: null,
        award_date: null,
        valid_from: null,
        valid_to: null,
        procurement_method: null,
        fund_type: null,
        pco_number: null,
        cancelled: 0,
        document_url: null,
        fiscal_year: null,
        source_type: null,
        source_url: recoveryTarget?.source_url || null,
        source_contract_id: null,
        recovery_status: showRecoveryMetadata ? recoveryTarget?.status || null : null,
        recovery_notes: showRecoveryMetadata ? recoveryTarget?.notes || null : null,
        recovery_lookup_mode: showRecoveryMetadata ? recoveryTarget?.lookup_mode || null : null,
        is_placeholder_original: true,
    };
}

function resolveContractFamilyDetail(contractNumber, entity, contractor) {
    const familyRows = getContractFamilyRows(contractNumber, entity, contractor);
    const recoveryTarget = getRecoveryTarget(contractNumber, entity, contractor);
    const originalRow = familyRows.find(row => isOriginalAmendment(row.amendment)) || null;

    if (originalRow) {
        return {
            contract: originalRow,
            familyRows,
            recoveryTarget,
            isPlaceholder: false,
        };
    }

    if (familyRows.length > 0) {
        return {
            // Keep family navigation anchored on a synthetic parent header whenever
            // the dataset only has amendment rows for this contract family.
            contract: buildMissingOriginalPlaceholderContract(
                contractNumber,
                entity,
                contractor,
                familyRows,
                recoveryTarget
            ),
            familyRows,
            recoveryTarget,
            isPlaceholder: true,
        };
    }

    return {
        contract: null,
        familyRows,
        recoveryTarget,
        isPlaceholder: false,
    };
}

function getContractFamilyRows(contractNumber, entity, contractor) {
    if (!contractNumber || !entity || !contractor) return [];

    const contractorFamily = normalizeContractorFamily(contractor);
    return query(
        `SELECT *
         FROM contracts
         WHERE contract_number = ?
           AND entity = ?`,
        [contractNumber, entity]
    )
        .filter(row => normalizeContractorFamily(row.contractor) === contractorFamily)
        .sort(compareFamilyMembers);
}

function getAmendmentCount(contractNumber, entity, contractor, currentId) {
    return getContractFamilyRows(contractNumber, entity, contractor)
        .filter(row => !isOriginalAmendment(row.amendment) || row.id === currentId)
        .length;
}

function getAmendments(contractNumber, entity, contractor, excludeId) {
    return getContractFamilyRows(contractNumber, entity, contractor)
        .filter(row => excludeId == null || row.id !== excludeId)
        .map(row => ({
            id: row.id,
            amendment: row.amendment,
            amount: row.amount,
            award_date: row.award_date,
            valid_from: row.valid_from,
            valid_to: row.valid_to,
            service_type: row.service_type,
        }));
}

function getStats() {
    const manifest = getManifest();
    const total = manifest.row_count != null
        ? manifest.row_count
        : (queryScalar("SELECT COUNT(*) FROM contracts") || 0);
    const amount = manifest.total_amount != null
        ? manifest.total_amount
        : (queryScalar("SELECT SUM(amount) FROM contracts WHERE amount IS NOT NULL") || 0);
    const archivedYears = Array.isArray(manifest.archived_csv_fiscal_years) && manifest.archived_csv_fiscal_years.length
        ? manifest.archived_csv_fiscal_years
        : (Array.isArray(manifest.fiscal_years) ? manifest.fiscal_years : []);
    if (archivedYears.length) {
        return {
            total,
            amount,
            minYear: archivedYears[archivedYears.length - 1],
            maxYear: archivedYears[0],
        };
    }
    const years = query(
        "SELECT MIN(fiscal_year) AS min_fy, MAX(fiscal_year) AS max_fy FROM contracts"
    )[0];
    return { total, amount, minYear: years.min_fy, maxYear: years.max_fy };
}
