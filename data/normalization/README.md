# Normalization registry

This directory is the deterministic, reviewed foundation for normalization knowledge.
It is intentionally separate from source evidence and from the legacy normalization
constants in `pipeline/contract_utils.py`. The legacy constants remain in place until
a later task wires this registry into ingestion.

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

## Reviewed contents

`contractor-aliases.csv` contains exactly the 23 explicit override mappings that
currently live in `CONTRACTOR_FAMILY_OVERRIDES` in `pipeline/contract_utils.py`.
They are retained as reviewed aliases with stable canonical IDs and display labels;
this task does not add fuzzy variants, remove the source constants, or wire the
registry into the existing pipeline.

`entity-aliases.csv`, `service-category-aliases.csv`, and `service-type-aliases.csv`
are deliberately header-only because no additional reviewed explicit mappings were
available for this isolated foundation.

`review-decisions.csv` records the two high-risk existing decisions for `T P
CONSULTING` and `INTEGRA` as `retained`. Those rows document review; they are not
additional aliases and do not contain candidate scores.
