/**
 * db.js — sql.js wrapper with query builder and IndexedDB caching.
 *
 * Loads the SQLite database (gzipped) into the browser via WebAssembly,
 * caches it in IndexedDB for instant repeat visits.
 */

const DB_FILE = "contratos.db.gz";
const IDB_NAME = "ocpr-transparency";
const IDB_STORE = "db-cache";
const PAGE_SIZE = 50;

let _db = null;

/**
 * Initialize sql.js and load the database.
 * @param {function} onStatus - callback for status updates
 * @returns {Promise<void>}
 */
async function initDB(onStatus) {
    onStatus("Inicializando...");

    const SQL = await initSqlJs({
        locateFile: file => `https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.10.3/${file}`
    });

    // Try IndexedDB cache first
    onStatus("Verificando cache...");
    let dbBytes = await loadFromCache();

    if (!dbBytes) {
        onStatus("Descargando base de datos...");
        const response = await fetch(DB_FILE);
        if (!response.ok) throw new Error(`Failed to fetch ${DB_FILE}: ${response.status}`);

        const compressed = await response.arrayBuffer();
        onStatus("Descomprimiendo...");
        dbBytes = await decompress(new Uint8Array(compressed));

        // Cache for next time
        await saveToCache(dbBytes);
    } else {
        onStatus("Cargado desde cache");
    }

    onStatus("Abriendo base de datos...");
    _db = new SQL.Database(dbBytes);
    _db.run("PRAGMA cache_size = -32000"); // 32MB cache
}

/**
 * Decompress gzipped data using DecompressionStream (modern browsers)
 * or fall back to manual approach.
 */
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

// ── IndexedDB cache ───────────────────────────────────────────────────────

function openIDB() {
    return new Promise((resolve, reject) => {
        const req = indexedDB.open(IDB_NAME, 1);
        req.onupgradeneeded = () => req.result.createObjectStore(IDB_STORE);
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
    });
}

async function loadFromCache() {
    try {
        const idb = await openIDB();
        return await new Promise((resolve, reject) => {
            const tx = idb.transaction(IDB_STORE, "readonly");
            const req = tx.objectStore(IDB_STORE).get("db");
            req.onsuccess = () => resolve(req.result || null);
            req.onerror = () => resolve(null);
        });
    } catch {
        return null;
    }
}

async function saveToCache(bytes) {
    try {
        const idb = await openIDB();
        const tx = idb.transaction(IDB_STORE, "readwrite");
        tx.objectStore(IDB_STORE).put(bytes, "db");
    } catch {
        // Cache failure is non-fatal
    }
}

/** Clear the IndexedDB cache (useful when DB is updated). */
async function clearCache() {
    try {
        const idb = await openIDB();
        const tx = idb.transaction(IDB_STORE, "readwrite");
        tx.objectStore(IDB_STORE).clear();
    } catch {
        // ignore
    }
}

// ── Query helpers ─────────────────────────────────────────────────────────

/**
 * Run a SELECT query and return array of row objects.
 */
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

/**
 * Run a single-value query (e.g. COUNT).
 */
function queryScalar(sql, params = []) {
    const result = _db.exec(sql, params);
    if (result.length > 0 && result[0].values.length > 0) {
        return result[0].values[0][0];
    }
    return null;
}

/**
 * Build a search query from filter values.
 * @param {object} filters
 * @param {number} page - 1-based page number
 * @param {string} sortCol - column to sort by
 * @param {string} sortDir - 'ASC' or 'DESC'
 * @returns {{ dataSql: string, countSql: string, sumSql: string, params: array }}
 */
function buildSearchQuery(filters, page = 1, sortCol = "award_date", sortDir = "DESC") {
    const where = [];
    const params = [];

    if (filters.keyword) {
        where.push("c.rowid IN (SELECT rowid FROM contracts_fts WHERE contracts_fts MATCH ?)");
        // Escape special FTS5 characters and add prefix matching
        const escaped = filters.keyword.replace(/['"*()]/g, "").trim();
        params.push(escaped + "*");
    }

    if (filters.contractor) {
        const term = filters.contractor;
        if (term.includes("*")) {
            // Wildcard mode: * becomes %, user controls matching
            where.push("c.contractor LIKE ?");
            params.push(term.replace(/\*/g, "%"));
        } else {
            // Word-boundary mode: match start of name or start of any word
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

    // Validate sort column to prevent injection
    const validCols = ["contract_number", "contractor", "entity", "amount", "award_date", "service_category"];
    if (!validCols.includes(sortCol)) sortCol = "award_date";
    if (sortDir !== "ASC") sortDir = "DESC";

    const offset = (page - 1) * PAGE_SIZE;

    const dataSql = `SELECT c.* FROM contracts c ${whereClause}
        ORDER BY c.${sortCol} ${sortDir} NULLS LAST
        LIMIT ${PAGE_SIZE} OFFSET ${offset}`;

    const countSql = `SELECT COUNT(*) FROM contracts c ${whereClause}`;
    const sumSql = `SELECT COALESCE(SUM(c.amount), 0) FROM contracts c ${whereClause}`;

    return { dataSql, countSql, sumSql, params };
}

/**
 * Get distinct values for a column (for populating dropdowns).
 */
function getDistinct(column) {
    const validCols = ["entity", "service_category", "fiscal_year"];
    if (!validCols.includes(column)) return [];
    return query(
        `SELECT DISTINCT ${column} FROM contracts WHERE ${column} IS NOT NULL AND ${column} != '' ORDER BY ${column}`
    ).map(r => r[column]);
}

/**
 * Get database stats for the stats bar.
 */
function getStats() {
    const total = queryScalar("SELECT COUNT(*) FROM contracts") || 0;
    const amount = queryScalar("SELECT SUM(amount) FROM contracts WHERE amount IS NOT NULL") || 0;
    const years = query(
        "SELECT MIN(fiscal_year) as min_fy, MAX(fiscal_year) as max_fy FROM contracts"
    )[0];
    return { total, amount, minYear: years.min_fy, maxYear: years.max_fy };
}
