# ADR-0002: Isolate PII in a vault table, pseudonymise everything else

**Status:** accepted

## Context
The CRM feed contains names, e-mail addresses, phone numbers and addresses (GDPR personal data). Analysts,
dashboards, Genie and ML models need customer-level data but not identities.

## Decision
1. Direct identifiers are stored **only** in `silver.customer_pii`.
2. Every other table uses `customer_key = sha256(customer_id || salt)`. The salt lives in a secret scope,
   so the key cannot be reversed by brute-forcing CRM ids.
3. Unity Catalog **column masks** on the vault reveal values only to the `pii_readers` group.
4. Columns and tables carry **tags** (`pii`, `data_classification`, `gdpr`) for discovery and audits.
5. **Right to erasure** = delete the vault row + `REORG ... APPLY (PURGE)`; gold keys become unlinkable.
6. The RAG assistant has an output guardrail that fails evaluation if an e-mail or phone number appears.

## Consequences
* Erasure and access reviews involve one table, not a hunt across the lakehouse.
* Gold is safe to expose to BI and Genie.
* Raw files and bronze/silver CRM snapshots still hold identifiers. Retention policy: raw files 90 days,
  bronze/silver CRM snapshots rebuilt from a 90-day window, so erased identifiers age out. In production
  this would be enforced with storage lifecycle rules and a scheduled purge.

## Alternatives considered
* Masking columns directly in gold: every new table needs masks, and pseudonymous joins are still needed.
* Encryption with per-customer keys (crypto-shredding): strongest erasure guarantee, more key management.
  A good next step for regulated environments such as banking.
