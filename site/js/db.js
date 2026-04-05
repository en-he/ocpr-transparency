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
    dashboard: {
        top_contractors: [],
        top_entities: [],
        yearly_spending: [],
    },
    raw_csv_base_url: "https://github.com/en-he/ocpr-transparency/raw/main/data/raw/",
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
        rows.push(stmt.getAsObject());
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

function normalizeContractorFamily(value) {
    if (!value) return "";
    return String(value)
        .toUpperCase()
        .replace(/[\u0000.,;:]/g, "")
        .replace(/\s+/g, " ")
        .trim();
}

function contractorFamilyExpr(alias = "c") {
    const col = `${alias}.contractor`;
    return `TRIM(
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(UPPER(COALESCE(${col}, '')), '.', ''),
                    ',', ''),
                ';', ''),
            ':', ''),
        CHAR(0), '')
    )`;
}

function escapeLikeValue(value) {
    return String(value).replace(/[\\%_]/g, "\\$&");
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
        baseWhere.push("c.rowid IN (SELECT rowid FROM contracts_fts WHERE contracts_fts MATCH ?)");
        const escaped = filters.keyword.replace(/['\"*()]/g, "").trim();
        params.push(escaped + "*");
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
            WHEN NULLIF(TRIM(COALESCE(f.amendment, '')), '') IS NULL THEN 0
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
                        WHEN NULLIF(TRIM(COALESCE(amendment, '')), '') IS NULL THEN 1
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
            fiscal_year
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

function getAmendmentCount(contractNumber, entity, contractor, currentId) {
    if (!contractNumber || !entity || !contractor) return 0;
    const contractorFamily = normalizeContractorFamily(contractor);
    return queryScalar(
        `SELECT COUNT(*) FROM contracts
         WHERE contract_number = ?
           AND entity = ?
           AND ${contractorFamilyExpr("contracts")} = ?
           AND (NULLIF(TRIM(COALESCE(amendment, '')), '') IS NOT NULL OR id = ?)`,
        [contractNumber, entity, contractorFamily, currentId]
    ) || 0;
}

function getAmendments(contractNumber, entity, contractor, excludeId) {
    if (!contractNumber || !entity || !contractor) return [];
    const contractorFamily = normalizeContractorFamily(contractor);
    return query(
        `SELECT id, amendment, amount, award_date, valid_from, valid_to, service_type
         FROM contracts
         WHERE contract_number = ?
           AND entity = ?
           AND ${contractorFamilyExpr("contracts")} = ?
           AND (? IS NULL OR id != ?)
         ORDER BY
           CASE WHEN NULLIF(TRIM(COALESCE(amendment, '')), '') IS NULL THEN 0 ELSE 1 END,
           amendment ASC,
           award_date ASC,
           id ASC`,
        [contractNumber, entity, contractorFamily, excludeId, excludeId]
    );
}

function getStats() {
    const manifest = getManifest();
    const total = manifest.row_count != null
        ? manifest.row_count
        : (queryScalar("SELECT COUNT(*) FROM contracts") || 0);
    const amount = manifest.total_amount != null
        ? manifest.total_amount
        : (queryScalar("SELECT SUM(amount) FROM contracts WHERE amount IS NOT NULL") || 0);
    const years = query(
        "SELECT MIN(fiscal_year) AS min_fy, MAX(fiscal_year) AS max_fy FROM contracts"
    )[0];
    return { total, amount, minYear: years.min_fy, maxYear: years.max_fy };
}
