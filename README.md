# Passport onFHIR Feast Connector

> [!IMPORTANT]
> **Superseded by [`../passport-node-agent`](../passport-node-agent).** Its `dataset-sync` module does
> what this script does and more: it carries the fields this connector drops, keys writes on
> `(url, version)` so re-running converges instead of duplicating, and authenticates as a Keycloak
> service account. **This script no longer runs** — it still posts to `/user/connector/login`, which the
> Passport removed when machine identity moved to `client_credentials`. It is kept as the reference
> prototype it was; the offline token in `main.py` is dead and should not be reused.

This connector includes a one-time Python script that reads dataset information from onFHIR Feast, 
transforms it into AI4HF Passport objects, and pushes them to the AI4HF Passport Server.

## Usage
Deploy the Passport server before running the connector.
```
git clone https://github.com/AI4HF/passport.git
```
Deploy onFHIR Feast before running the connector.
```
git clone https://gitlab.srdc.com.tr/onfhir/onfhir-feast.git
```
Once both the AI4HF Passport Server and the onFHIR Feast have been deployed, you can deploy the connector by running the following command:
```
docker compose up -d
```