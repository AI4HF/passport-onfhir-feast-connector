# onFHIR Feast → AI4HF Passport Mapping

Summary of what the connector does and how each onFHIR Feast dataset field is placed into the
AI4HF Passport data model. All logic lives in `main.py`; the mapping itself is performed in
`fetch_and_send_dataset()` (`main.py:174`).

## 1. What the connector does

The connector is a one-shot Python script. It

1. authenticates against the Passport Server as a *connector* (machine) user,
2. reads a single dataset descriptor from the onFHIR Feast Server,
3. transforms that descriptor into Passport entities, and
4. creates those entities on the Passport Server through its REST API.

It keeps no state and no database of its own; running it twice creates a second set of Passport
records.

### Configuration (environment variables)

| Variable | Meaning |
| --- | --- |
| `PASSPORT_SERVER_URL` | Base URL of the AI4HF Passport Server |
| `FEAST_URL` | Base URL of the onFHIR Feast API |
| `DATASET_ID` | Id of the Feast dataset to be transferred |
| `STUDY_ID` | Passport study the new records belong to |
| `EXPERIMENT_ID` | Passport experiment the feature set is attached to |
| `ORGANIZATION_ID` | Passport organization that owns the dataset |
| `CONNECTOR_SECRET` | Offline Keycloak token used for connector login |

### Authentication

`CONNECTOR_SECRET` (an offline Keycloak token) is POSTed as the raw request body to
`POST /user/connector/login`, and the returned `access_token` is used as a bearer token for every
subsequent call. If any call answers `401`, the connector re-authenticates once and retries the same
request. The `sub` claim of the access token is decoded locally and used as the `createdBy` /
`lastUpdatedBy` value of every created record.

### Source

A single call, `GET {FEAST_URL}/Dataset/{DATASET_ID}`, returns the whole dataset descriptor
(population, feature set, data source, variables, and statistics). Nothing else is read from Feast.

### Order of creation

Population → FeatureSet → Dataset → Features (+ their characteristics) → Outcomes (+ their
characteristics). The ids returned by the earlier calls are used as foreign keys in the later ones.
Every write is a `POST` carrying `?studyId={STUDY_ID}` as a query parameter.

## 2. Field-by-field mapping

### Population — `POST /population?studyId=…`

| Passport field | Feast source |
| --- | --- |
| `studyId` | `STUDY_ID` (environment) |
| `populationUrl` | `population.url` |
| `description` | `population.description` |
| `characteristics` | `population.description` (same value reused) |

### FeatureSet — `POST /featureset?studyId=…`

| Passport field | Feast source |
| --- | --- |
| `experimentId` | `EXPERIMENT_ID` (environment) |
| `title` | `featureSet.title` |
| `featuresetURL` | `featureSet.url` |
| `description` | `featureSet.description` |
| `createdBy`, `lastUpdatedBy` | `sub` claim of the access token |

### Dataset — `POST /dataset?studyId=…`

| Passport field | Feast source |
| --- | --- |
| `featuresetId` | id of the FeatureSet created above |
| `populationId` | id of the Population created above |
| `organizationId` | `ORGANIZATION_ID` (environment) |
| `title` | `dataSource.name` |
| `description` | `dataSource.name` (same value reused) |
| `version` | `dataSource.version` |
| `referenceEntity` | `population.title` |
| `numOfRecords` | `datasetStats.numOfEntries` |
| `synthetic` | constant `false` |
| `createdBy`, `lastUpdatedBy` | `sub` claim of the access token |

### Feature — `POST /feature?studyId=…` (one call per `features[]` entry)

| Passport field | Feast source |
| --- | --- |
| `featuresetId` | id of the FeatureSet created above |
| `title` | `features[i].name` |
| `description` | `features[i].description` |
| `dataType` | `features[i].dataType` |
| `isOutcome` | constant `false` |
| `mandatory` | derived: `true` when `datasetStats.featureStats[name].numOfNotNull == datasetStats.numOfEntries`, i.e. the feature has no missing values |
| `isUnique` | constant `false` |
| `units`, `equipment`, `dataCollection` | constant `"Unknown"` (not expressed in Feast) |
| `createdBy`, `lastUpdatedBy` | `sub` claim of the access token |

### Outcome — also `POST /feature?studyId=…` (one call per `outcomes[]` entry)

Passport has no separate outcome entity; a Feast outcome becomes a Feature with `isOutcome = true`.
All other fields are filled exactly as for features, from `outcomes[i]`. (Note: the `mandatory`
derivation looks the outcome name up in `datasetStats.featureStats`, so for outcomes it currently
evaluates to `false` unless the name also appears among the feature statistics.)

### FeatureDatasetCharacteristic — `POST /feature-dataset-characteristic?studyId=…`

Feast statistics are flattened into one Passport characteristic record per statistic. For every
feature, each key of `datasetStats.featureStats[name]` becomes one record; for every outcome, each
key of `datasetStats.outcomeStats[name]`.

| Passport field | Feast source |
| --- | --- |
| `datasetId` | id of the Dataset created above |
| `featureId` | id of the Feature/Outcome created above |
| `characteristicName` | the statistic's key (`numOfNotNull`, and whatever else Feast reports: min, max, average, standard deviation, category counts, …) |
| `value` | the statistic's value, stringified |
| `valueDataType` | Python type name of the original value (`int`, `float`, `str`, `dict`, `list`) |

Because the statistic names are taken verbatim from Feast, the set of characteristics is not fixed:
whatever Feast computes for a variable is carried over as-is.

## 3. What is read but not transferred

The Feast descriptor is richer than the Passport model, so the following are parsed into the
in-memory model but are not currently sent to Passport:

* `baseVariables` — the raw variables the features are derived from
* `populationStats` — `numOfEntries`, `entityStats`, `eligibilityPeriodStats`,
  `eligibilityCriteriaStats`
* `temporal` (`start`, `end`) and `issued` — the dataset's observation window and issue date
* `dataSource.id`, `dataSource.interface`, `dataSource.sourceType`
* `population.pipeline` / `featureSet.pipeline` references
* per-variable `valueSet` / `concept` codings, `generatedDescription`, `default`
* `datasetStats.entityStats`, `datasetStats.samplingStats`,
  `datasetStats.secondaryTimePointStats`

Conversely, the Passport fields `units`, `equipment`, `dataCollection`, `isUnique` and `synthetic`
have no counterpart in the Feast descriptor and are filled with constants.
