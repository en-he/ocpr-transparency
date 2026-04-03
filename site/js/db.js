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
    raw_csv_base_url: "https://github.com/en-he/ocpr-transparency/raw/main/data/raw/",
    browser_db: {
        url: "contratos.db.gz",
        sha256: "legacy-browser-db",
    },
    full_download_db: {
        url: "https://github.com/en-he/ocpr-transparency/releases/download/data-latest/contratos-full.db.gz",
        sha256: null,
    },
};

let _db = null;
let _manifest = DEFAULT_MANIFEST;

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
            browser_db: {
                ...DEFAULT_MANIFEST.browser_db,
                ...(loaded.browser_db || {}),
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

    const SQL = await initSqlJs({
        locateFile: file => `https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.10.3/${file}`,
    });

    const browserDb = _manifest.browser_db || DEFAULT_MANIFEST.browser_db;
    const browserHash = browserDb.sha256 || "legacy-browser-db";

    onStatus("Checking local cache...");
    let dbBytes = await loadFromCache(browserHash);

    if (!dbBytes) {
        onStatus("Downloading contracts...");
        const response = await fetch(browserDb.url, { cache: "no-store" });
        if (!response.ok) {
            throw new Error(`Failed to fetch ${browserDb.url}: ${response.status}`);
        }

        const compressed = await response.arrayBuffer();
        onStatus("Decompressing...");
        dbBytes = await decompress(new Uint8Array(compressed));
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

function buildSearchQuery(filters, page = 1, sortCol = "award_date", sortDir = "DESC") {
    const where = [];
    const params = [];

    if (filters.keyword) {
        where.push("c.rowid IN (SELECT rowid FROM contracts_fts WHERE contracts_fts MATCH ?)");
        const escaped = filters.keyword.replace(/['\"*()]/g, "").trim();
        params.push(escaped + "*");
    }

    if (filters.contractor) {
        const term = filters.contractor;
        if (term.includes("*")) {
            where.push("c.contractor LIKE ?");
            params.push(term.replace(/\*/g, "%"));
        } else {
            where.push("(c.contractor LIKE ? OR c.contractor LIKE ?)");
            params.push(term + "%");
            params.push("% " + term + "%");
        }
    }

    if (filters.entity) {
        where.push("c.entity = ?");
        params.push(filters.entity);
    }

    if (filters.amountMin) {
        where.push("c.amount >= ?");
        params.push(Number(filters.amountMin));
    }

    if (filters.amountMax) {
        where.push("c.amount <= ?");
        params.push(Number(filters.amountMax));
    }

    if (filters.dateFrom) {
        where.push("c.award_date >= ?");
        params.push(filters.dateFrom);
    }

    if (filters.dateTo) {
        where.push("c.award_date <= ?");
        params.push(filters.dateTo);
    }

    if (filters.category) {
        where.push("c.service_category = ?");
        params.push(filters.category);
    }

    if (filters.fiscalYear) {
        where.push("c.fiscal_year = ?");
        params.push(filters.fiscalYear);
    }

    const whereClause = where.length ? "WHERE " + where.join(" AND ") : "";

    const validCols = ["contract_number", "contractor", "entity", "amount", "award_date", "service_category"];
    if (!validCols.includes(sortCol)) sortCol = "award_date";
    if (sortDir !== "ASC") sortDir = "DESC";

    const offset = (page - 1) * PAGE_SIZE;
    const familyExpr = contractorFamilyExpr("c");
    const representativeOrderExpr = `
        CASE
            WHEN NULLIF(TRIM(COALESCE(f.amendment, '')), '') IS NULL THEN 0
            ELSE 1
        END ASC,
        f.award_date ASC,
        f.id ASC
    `;

    const sortExprByCol = {
        contract_number: "contract_number",
        contractor: "contractor",
        entity: "entity",
        amount: "display_amount",
        award_date: "display_award_date",
        service_category: "service_category",
    };
    const sortExpr = sortExprByCol[sortCol] || "display_award_date";

    const familyCte = `
        WITH filtered AS (
            SELECT
                c.*,
                ${familyExpr} AS contractor_family
            FROM contracts c
            ${whereClause}
        ),
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
                MAX(CASE WHEN representative_row = 1 THEN amount END) AS representative_amount,
                MAX(CASE WHEN representative_row = 1 THEN award_date END) AS representative_award_date,
                MIN(award_date) AS family_earliest_award_date
            FROM ranked
            GROUP BY contract_number, entity, contractor_family
        )
    `;

    const dataSql = `${familyCte}
        SELECT
            representative_id AS id,
            contract_number,
            contractor,
            entity,
            service_category,
            service_type,
            fiscal_year,
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
        LIMIT ${PAGE_SIZE} OFFSET ${offset}`;

    const countSql = `${familyCte}
        SELECT COUNT(*)
        FROM families`;

    const sumSql = `${familyCte}
        SELECT COALESCE(SUM(family_total_amount), 0)
        FROM families`;

    return { dataSql, countSql, sumSql, params };
}

function getDistinct(column) {
    const validCols = ["entity", "service_category", "fiscal_year"];
    if (!validCols.includes(column)) return [];
    return query(
        `SELECT DISTINCT ${column} FROM contracts WHERE ${column} IS NOT NULL AND ${column} != '' ORDER BY ${column}`
    ).map(row => row[column]);
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
