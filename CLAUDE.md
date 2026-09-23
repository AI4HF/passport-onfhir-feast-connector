# Passport onFHIR Feast Connector (`passport-onfhir-feast-connector`)

> [!IMPORTANT]
> **Superseded by [`../passport-node-agent`](../passport-node-agent).** Its `dataset-sync` module does
> what this script does and more: it carries the fields this connector drops, keys writes on
> `(url, version)` so re-running converges instead of duplicating, and authenticates as a Keycloak
> service account. **This script no longer runs** — it still posts to `/user/connector/login`, which the
> Passport removed when machine identity moved to `client_credentials`. It is kept as the reference
> prototype it was; the offline token in `main.py` is dead and should not be reused.


A **one-shot Python script** (no framework, no state, no tests) that reads a single dataset descriptor
from an **onFHIR Feast** server (a.k.a. Studyfyr — the engine executing the declarative definitions in
[`../feature-extraction-suite`](../feature-extraction-suite/CLAUDE.md)) and creates the corresponding
records in the **AI4HF Passport Server** (`../passport`) through its REST API.

Remote: https://github.com/AI4HF/passport-onfhir-feast-connector · Python 3.10 · deps: `requests`, `PyJWT`,
`python-dateutil` · Docker image `srdc/passport-onfhir-feast-connector` (unversioned, `latest`).

Workspace context: this is the current, AI4HF-project prototype of **integration #1 (Studyfyr ↔ Passport)**
in [`../design/use-case-flow.md`](../design/use-case-flow.md) — pull-based, manually configured per dataset.
The federated redesign will rethink direction, trigger, and identity (Q2/Q4 there).

## Files

| File | Role |
|---|---|
| `main.py` | Everything: `FeastConnector` class + `__main__` entrypoint reading env vars |
| `feast_models.py` | Plain-Python mirror of the Feast dataset descriptor (`RootObject` → `Entity` → population/featureSet/variables/stats) |
| `passport_models.py` | Plain-Python mirror of the Passport entities it writes (Population, FeatureSet, Feature, Dataset, FeatureDatasetCharacteristic) |
| `MAPPING.md` | **Read this first** — full field-by-field Feast→Passport mapping, what's dropped, what's constant |
| `docker-compose.yml`, `Dockerfile`, `entrypoint.sh` | Container runs the script once and exits |

## What it does (order matters)

1. `POST /user/connector/login` with the raw `CONNECTOR_SECRET` (offline Keycloak token) as the request
   body → bearer `access_token`. The unverified-decoded `sub` claim becomes `createdBy`/`lastUpdatedBy`.
2. `GET {FEAST_URL}/Dataset/{DATASET_ID}` — the only Feast read; no auth on the Feast side.
3. Creates, in this order (each id feeds the next): **Population → FeatureSet → Dataset →
   Features + per-feature characteristics → Outcomes + characteristics**. Every write is
   `POST …?studyId={STUDY_ID}`.

Feast statistics are flattened into one `FeatureDatasetCharacteristic` row per statistic key — the set of
characteristic names is whatever Feast computed, not a fixed schema.

## Configuration (env vars, defaults in `main.py` / `docker-compose.yml`)

`PASSPORT_SERVER_URL`, `FEAST_URL`, `DATASET_ID`, `STUDY_ID`, `EXPERIMENT_ID`, `ORGANIZATION_ID`,
`CONNECTOR_SECRET`. Study/experiment/organization must already exist in the Passport — the connector
does not create them. Compose attaches to the external `passport-network` and `onfhir-feast-network`.

## Gotchas

- **Not idempotent** — running twice creates a second full set of Passport records; there is no
  lookup-by-`url`/version despite Population/FeatureSet carrying canonical URLs.
- **Outcome `mandatory` is effectively always `false`**: the derivation looks the outcome name up in
  `datasetStats.featureStats` (not `outcomeStats`).
- `_refreshTokenAndRetry` re-authenticates once on 401 and retries — but always with `requests.post`,
  so it only suits the write calls (currently fine: all Passport calls are POSTs).
- Passport fields with no Feast counterpart are constants: `synthetic=false`, `isUnique=false`,
  `units`/`equipment`/`dataCollection`="Unknown"; `description` reuses other text fields (see MAPPING.md §3
  for everything parsed but *not* transferred — temporal coverage, populationStats, valueSet codings, …).
- A demo `CONNECTOR_SECRET` is committed in `main.py` and `docker-compose.yml` (same debt pattern as the
  main repos).

## Conventions

Gitmoji commits (`:sparkles: feat:`, `:bug: fix:`, `:memo:`), short kebab-case branches off `main`, PRs.
No automated tests — verify by running against a live Feast + Passport stack and checking the created
records in the Passport UI.
