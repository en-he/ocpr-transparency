# Normalization registry

This directory is the deterministic, reviewed source of normalization knowledge.
It is intentionally separate from source evidence and from the immutable Phase 1
certification artifacts. The ingestion and browser projections consume this registry;
changes require a new registry version and reviewed decision rows.

## Contract

- `registry_version`: `normalization-registry-1`
- `algorithm_version`: `lookup-v1`
- Supported domains: `contractor`, `entity`, `service_category`, and `service_type`.
- Alias files use the schema declared in `schema-profiles.json` and are kept in
  stable alias-key order.
- Canonical IDs are manually assigned, lowercase, domain-prefixed IDs such as
  `contractor:quantum-health-consulting`. They are data identities, not IDs generated
  from a row position or a runtime hash.
- Alias lookup is exact after NFD accent removal, punctuation/symbol-to-space
  normalization, whitespace collapse, and uppercase conversion. It does not perform
  fuzzy matching, candidate scoring, confidence estimation, or automatic alias
  expansion.
- Missing, unresolved, and contradictory values remain distinct. A contradictory
  alias key is returned as `collision` and never resolves to one identity.
- The registry payload contains only versioned schema/alias/review data. It contains
  no machine paths, timestamps, or generated values, so its SHA-256 is reproducible.
- `registry-manifest.json` is the separate Milestone B provenance artifact. It records
  the payload identity and source-file hashes without rewriting immutable Phase 1
  certification reports or their manifest.

## Reviewed contents

`contractor-aliases.csv` contains exactly the 23 reviewed mappings formerly
duplicated in Python and JavaScript override constants. They are retained as reviewed
aliases with stable canonical IDs and display labels; both ingestion and browser
projections now consume the projected registry identities rather than separate tables.

`entity-aliases.csv`, `service-category-aliases.csv`, and `service-type-aliases.csv`
are deliberately header-only because no additional reviewed explicit mappings were
available for this isolated foundation.

`review-decisions.csv` records the two high-risk existing decisions for `T P
CONSULTING` and `INTEGRA` as `retained`. Those rows document review; they are not
additional aliases and do not contain candidate scores.
