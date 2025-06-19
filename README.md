# Passport onFHIR Feast Connector
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