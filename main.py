from datetime import datetime
import os
import traceback
from typing import List

import jwt
import requests

import feast_models
import passport_models


class FeastConnector:
    """
    Feast Connector that fetches data from onFHIR Feast Server and sent them into the AI4HF Passport Server.
    """

    def __init__(self, passport_server_url: str, study_id: str, organization_id: str, experiment_id: str, connector_secret: str,
                 feast_url: str, dataset_id: str):
        """
        Initialize the API client with authentication and study details.
        """
        self.passport_server_url = passport_server_url
        self.study_id = study_id
        self.organization_id = organization_id
        self.experiment_id = experiment_id
        self.connector_secret = connector_secret
        self.feast_url = feast_url
        self.dataset_id = dataset_id
        self.token = self._authenticate()

    def _authenticate(self) -> str:
        """
        Authenticate with login endpoint and retrieve an access token.
        """
        auth_url = f"{self.passport_server_url}/user/connector/login"

        response = requests.post(auth_url, data=self.connector_secret)
        response.raise_for_status()
        return response.json().get("access_token")

    def _refreshTokenAndRetry(self, response, headers, payload, url):
        """
        If token is expired, refresh token and retry

        :param response: Response object from previous request.
        :param headers: Headers object from previous request.
        :param payload: Payload object from previous request.
        :param url: The url to sent.

        :return response: Response algorithm object from the server.
        """

        if response.status_code == 401:  # Token expired, refresh and retry
            self.token = self._authenticate()
            headers["Authorization"] = f"Bearer {self.token}"
            return requests.post(url, json=payload, headers=headers)
        else:
            return response

    def fetch_feast_dataset(self, dataset_id: str) -> feast_models.RootObject:
        """
        Fetch dataset from the onFHIR Feast Server.

        :param dataset_id: ID of the dataset to fetch.
        :return: RootObject representing the dataset.
        """
        url = f"{self.feast_url}/Dataset/{dataset_id}"
        headers = {"Content-Type": "application/json"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        response_json = response.json()
        entity_data = response_json["entity"]

        # Parse nested structures manually
        population = feast_models.Population(
            url=entity_data["population"]["url"],
            title=entity_data["population"]["title"],
            description=entity_data["population"]["description"],
            pipeline=feast_models.Pipeline(**entity_data["population"]["pipeline"])
        )

        feature_set = feast_models.FeatureSet(
            url=entity_data["featureSet"]["url"],
            title=entity_data["featureSet"]["title"],
            description=entity_data["featureSet"]["description"],
            pipeline=feast_models.Pipeline(**entity_data["featureSet"]["pipeline"])
        )

        data_source = feast_models.DataSource(**entity_data["dataSource"])
        temporal = feast_models.Temporal(**entity_data["temporal"])

        base_variables = [
            feast_models.Variable(
                name=v["name"],
                description=v["description"],
                dataType=v["dataType"],
                generatedDescription=v.get("generatedDescription", []),
                default=v.get("default"),
                valueSet=feast_models.ValueSet(
                    url=v["valueSet"]["url"],
                    concept=[feast_models.Concept(**c) for c in v["valueSet"]["concept"]]
                ) if "valueSet" in v else None
            ) for v in entity_data["baseVariables"]
        ]

        features = [
            feast_models.Variable(
                name=v["name"],
                description=v["description"],
                dataType=v["dataType"],
                generatedDescription=v.get("generatedDescription", []),
                default=v.get("default"),
                valueSet=feast_models.ValueSet(
                    url=v["valueSet"]["url"] if "url" in v["valueSet"] else None,
                    concept=[feast_models.Concept(**c) for c in v["valueSet"]["concept"]]
                ) if "valueSet" in v else None
            ) for v in entity_data["features"]
        ]

        outcomes = [
            feast_models.Variable(
                name=v["name"],
                description=v["description"],
                dataType=v["dataType"],
                generatedDescription=v.get("generatedDescription", [])
            ) for v in entity_data["outcomes"]
        ]

        population_stats = feast_models.PopulationStats(
            numOfEntries=entity_data["populationStats"]["numOfEntries"],
            entityStats=entity_data["populationStats"]["entityStats"],
            eligibilityPeriodStats=entity_data["populationStats"]["eligibilityPeriodStats"],
            eligibilityCriteriaStats=entity_data["populationStats"]["eligibilityCriteriaStats"]
        )

        feature_stats = {
            k: feast_models.Stats(numOfNotNull=v["numOfNotNull"],
                                  **{kk: vv for kk, vv in v.items() if kk != "numOfNotNull"})
            for k, v in entity_data["datasetStats"]["featureStats"].items()
        }

        outcome_stats = {
            k: feast_models.Stats(numOfNotNull=v["numOfNotNull"],
                                  **{kk: vv for kk, vv in v.items() if kk != "numOfNotNull"})
            for k, v in entity_data["datasetStats"]["outcomeStats"].items()
        }

        dataset_stats = feast_models.DatasetStats(
            numOfEntries=entity_data["datasetStats"]["numOfEntries"],
            entityStats=entity_data["datasetStats"]["entityStats"],
            samplingStats=entity_data["datasetStats"]["samplingStats"],
            secondaryTimePointStats=entity_data["datasetStats"]["secondaryTimePointStats"],
            featureStats=feature_stats,
            outcomeStats=outcome_stats
        )

        entity = feast_models.Entity(
            id=entity_data["id"],
            population=population,
            featureSet=feature_set,
            dataSource=data_source,
            issued=entity_data["issued"],
            temporal=temporal,
            baseVariables=base_variables,
            features=features,
            outcomes=outcomes,
            populationStats=population_stats,
            datasetStats=dataset_stats
        )

        return feast_models.RootObject(entity=entity)

    def fetch_and_send_dataset(self):
        """
        Fetch needed information from onFHIR Feast Server, transform data according to the AI4HF Passport Server,
        and sent it into the AI4HF Passport Server.

        :return
        """
        print("Fetching the data from onFHIR Feast Server...", flush=True)
        # Fetch dataset information from onFHIR Feast Server
        root_object: feast_models.RootObject = self.fetch_feast_dataset(self.dataset_id)

        # Extract population and sent it to AI4HF Passport Server
        feast_population: feast_models.Population = root_object.entity.population
        passport_population = passport_models.Population(studyId=self.study_id,
                                                         populationUrl=feast_population.url,
                                                         description=feast_population.description,
                                                         characteristics=feast_population.description)
        created_population = self.send_population(passport_population)
        print(f"Created population: {created_population}", flush=True)

        # Extract featureSet and sent it to AI4HF Passport Server
        feast_feature_set: feast_models.FeatureSet = root_object.entity.featureSet
        user_id = jwt.decode(self.token, options={"verify_signature": False})['sub']
        passport_feature_set = passport_models.FeatureSet(experimentId=self.experiment_id,
                                                          title=feast_feature_set.title,
                                                          featuresetURL=feast_feature_set.url,
                                                          description=feast_feature_set.description,
                                                          createdBy=user_id,
                                                          lastUpdatedBy=user_id)
        created_feature_set = self.send_feature_set(passport_feature_set)
        print(f"Created featureSet: {created_feature_set}", flush=True)

        # Extract dataset from entity object and sent it to AI4HF Passport Server
        entity: feast_models.Entity = root_object.entity
        passport_dataset: passport_models.Dataset = passport_models.Dataset(
            featuresetId=created_feature_set.featuresetId,
            populationId=created_population.populationId,
            organizationId=self.organization_id,
            title=entity.dataSource.name,
            description=entity.dataSource.name,
            version=entity.dataSource.version,
            referenceEntity=entity.population.title,
            numOfRecords=entity.datasetStats.numOfEntries,
            synthetic=False,
            createdBy=user_id,
            lastUpdatedBy=user_id
        )
        created_dataset = self.send_dataset(passport_dataset)
        print(f"Created dataset: {created_dataset}", flush=True)

        # Extract features and sent it to AI4HF Passport Server
        feast_features: List[feast_models.Variable] = root_object.entity.features
        for feast_feature in feast_features:
            feature_stat = root_object.entity.datasetStats.featureStats.get(feast_feature.name)
            isMandatory: bool = (
                    feature_stat is not None
                    and root_object.entity.datasetStats.numOfEntries == feature_stat.numOfNotNull
            )
            passport_feature: passport_models.Feature = passport_models.Feature(
                featuresetId=created_feature_set.featuresetId,
                title=feast_feature.name,
                description=feast_feature.description,
                dataType=feast_feature.dataType,
                isOutcome=False,
                mandatory=isMandatory,
                isUnique=False,
                units="Unknown",
                equipment="Unknown",
                dataCollection="Unknown",
                createdBy=user_id,
                lastUpdatedBy=user_id)
            created_feature = self.send_feature(passport_feature)
            print(f"Created feature: {created_feature}", flush=True)

        # Extract outcomes and sent it to AI4HF Passport Server
        feast_outcomes: List[feast_models.Variable] = root_object.entity.outcomes
        for feast_outcome in feast_outcomes:
            feature_stat = root_object.entity.datasetStats.featureStats.get(feast_outcome.name)
            isMandatory: bool = (
                    feature_stat is not None
                    and root_object.entity.datasetStats.numOfEntries == feature_stat.numOfNotNull
            )

            passport_outcome: passport_models.Feature = passport_models.Feature(
                featuresetId=created_feature_set.featuresetId,
                title=feast_outcome.name,
                description=feast_outcome.description,
                dataType=feast_outcome.dataType,
                isOutcome=True,
                mandatory=isMandatory,
                isUnique=False,
                units="Unknown",
                equipment="Unknown",
                dataCollection="Unknown",
                createdBy=user_id,
                lastUpdatedBy=user_id)

            created_outcome = self.send_feature(passport_outcome)
            print(f"Created outcome: {created_outcome}", flush=True)

            # Extract feature dataset characteristics and sent it to AI4HF Passport Server
            feast_feature_stats: feast_models.Stats = root_object.entity.datasetStats.featureStats.get(
                created_feature.title)
            all_fields = {'numOfNotNull': feast_feature_stats.numOfNotNull, **feast_feature_stats.additional_stats}
            for k, v in all_fields.items():
                passport_feature_dataset_characteristic: passport_models.FeatureDatasetCharacteristic = (
                    passport_models.FeatureDatasetCharacteristic(datasetId=created_dataset.datasetId,
                                                                 featureId=created_feature.featureId,
                                                                 characteristicName=k,
                                                                 value=str(v),
                                                                 valueDataType=type(v).__name__))
                created_feature_dataset_characteristic = self.send_feature_dataset_characteristic(
                    passport_feature_dataset_characteristic)
                print(f"Created feature dataset characteristic: {created_feature_dataset_characteristic}", flush=True)

        print("Data is sent to the AI4HF Passport Server!", flush=True)

    def send_feature_set(self, feature_set: passport_models.FeatureSet) -> passport_models.FeatureSet:
        """
        Send FeatureSet to the AI4HF Passport Server.

        :param feature_set: The featureSet object to be sent.

        :return FeatureSet: The featureSet object from the AI4HF Passport Server.
        """

        url = f"{self.passport_server_url}/featureset?studyId={self.study_id}"
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        payload = {"experimentId": feature_set.experimentId, "title": feature_set.title,
                   "featuresetURL": feature_set.featuresetURL, "description": feature_set.description,
                   "createdBy": feature_set.createdBy, "lastUpdatedBy": feature_set.lastUpdatedBy}

        response = requests.post(url, json=payload, headers=headers)

        # If token is expired, retry
        response = self._refreshTokenAndRetry(response, headers, payload, url)

        response.raise_for_status()

        response_json = response.json()

        return passport_models.FeatureSet(**response_json)

    def send_population(self, population: passport_models.Population) -> passport_models.Population:
        """
        Send Population to the AI4HF Passport Server.

        :param population: The population object to be sent.

        :return Population: The population object from the AI4HF Passport Server.
        """

        url = f"{self.passport_server_url}/population?studyId={self.study_id}"
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        payload = {"studyId": self.study_id, "populationUrl": population.populationUrl,
                   "description": population.description, "characteristics": population.characteristics}

        response = requests.post(url, json=payload, headers=headers)

        # If token is expired, retry
        response = self._refreshTokenAndRetry(response, headers, payload, url)

        response.raise_for_status()

        response_json = response.json()

        return passport_models.Population(**response_json)

    def send_feature(self, feature: passport_models.Feature) -> passport_models.Feature:
        """
        Send Feature to the AI4HF Passport Server.

        :param feature: The feature object to be sent.

        :return feature: The feature object from the AI4HF Passport Server.
        """

        url = f"{self.passport_server_url}/feature?studyId={self.study_id}"
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        payload = {"featuresetId": feature.featuresetId, "title": feature.title,
                   "description": feature.description, "dataType": feature.dataType,
                   "isOutcome": feature.isOutcome, "mandatory": feature.mandatory,
                   "isUnique": feature.isUnique, "units": feature.units,
                   "equipment": feature.equipment, "dataCollection": feature.dataCollection,
                   "createdBy": feature.createdBy, "lastUpdatedBy": feature.lastUpdatedBy}

        response = requests.post(url, json=payload, headers=headers)

        # If token is expired, retry
        response = self._refreshTokenAndRetry(response, headers, payload, url)

        response.raise_for_status()

        response_json = response.json()

        return passport_models.Feature(**response_json)

    def send_dataset(self, dataset: passport_models.Dataset) -> passport_models.Dataset:
        """
        Send Dataset to the AI4HF Passport Server.

        :param dataset: The dataset object to be sent.

        :return dataset: The dataset object from the AI4HF Passport Server.
        """

        url = f"{self.passport_server_url}/dataset?studyId={self.study_id}"
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        payload = {"datasetId": dataset.datasetId, "featuresetId": dataset.featuresetId,
                   "populationId": dataset.populationId, "organizationId": dataset.organizationId,
                   "title": dataset.title, "description": dataset.description,
                   "version": dataset.version, "referenceEntity": dataset.referenceEntity,
                   "numOfRecords": dataset.numOfRecords, "synthetic": dataset.synthetic,
                   "createdBy": dataset.createdBy, "lastUpdatedBy": dataset.lastUpdatedBy}

        response = requests.post(url, json=payload, headers=headers)

        # If token is expired, retry
        response = self._refreshTokenAndRetry(response, headers, payload, url)

        response.raise_for_status()

        response_json = response.json()

        return passport_models.Dataset(**response_json)

    def send_feature_dataset_characteristic(self,
                                            feature_dataset_characteristic: passport_models.FeatureDatasetCharacteristic) \
            -> passport_models.FeatureDatasetCharacteristic:
        """
        Send Feature Dataset Characteristic to the AI4HF Passport Server.

        :param feature_dataset_characteristic: The Feature Dataset Characteristic object to be sent.

        :return feature_dataset_characteristic: The Feature Dataset Characteristic object from the AI4HF Passport Server.
        """

        url = f"{self.passport_server_url}/feature-dataset-characteristic?studyId={self.study_id}"
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        payload = {"datasetId": feature_dataset_characteristic.datasetId,
                   "featureId": feature_dataset_characteristic.featureId,
                   "characteristicName": feature_dataset_characteristic.characteristicName,
                   "value": feature_dataset_characteristic.value,
                   "valueDataType": feature_dataset_characteristic.valueDataType}

        response = requests.post(url, json=payload, headers=headers)

        # If token is expired, retry
        response = self._refreshTokenAndRetry(response, headers, payload, url)

        response.raise_for_status()

        response_json = response.json()

        return passport_models.FeatureDatasetCharacteristic(**response_json)


if __name__ == "__main__":
    print("passport-onfhir-feast-connector has been started.")
    passport_server_url = os.getenv("PASSPORT_SERVER_URL", "http://localhost:8080")
    study_id = os.getenv("STUDY_ID", "0197a6f8-2b78-71e4-81c1-b7b6a744ece3")
    experiment_id = os.getenv("EXPERIMENT_ID", "0197a6f9-1f49-74a5-ab8a-e64fae0ca141")
    organization_id = os.getenv("ORGANIZATION_ID", "0197a6f5-bb48-7855-b248-95697e913f22")
    connector_secret = os.getenv("CONNECTOR_SECRET", "eyJhbGciOiJIUzUxMiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICI5ZTFiZTExNi0yMzg1LTRlZDctYTBiOC01ZDc0NWNjYzllOGMifQ.eyJpYXQiOjE3NTEyNzA4MjgsImp0aSI6ImIxMWE5NGI1LWQ5MzItNDhiNC1iMjc4LWFkZjQ1ZDJjMTMxOCIsImlzcyI6Imh0dHA6Ly9rZXljbG9hazo4MDgwL3JlYWxtcy9BSTRIRi1BdXRob3JpemF0aW9uIiwiYXVkIjoiaHR0cDovL2tleWNsb2FrOjgwODAvcmVhbG1zL0FJNEhGLUF1dGhvcml6YXRpb24iLCJzdWIiOiJkYXRhX3NjaWVudGlzdCIsInR5cCI6Ik9mZmxpbmUiLCJhenAiOiJBSTRIRi1BdXRoIiwic2Vzc2lvbl9zdGF0ZSI6IjE3YzU2ZjhkLTljZmEtNDM2OC05MzQ4LTkzN2ZjY2QyMjY0ZCIsInNjb3BlIjoib2ZmbGluZV9hY2Nlc3MgcHJvZmlsZSBlbWFpbCIsInNpZCI6IjE3YzU2ZjhkLTljZmEtNDM2OC05MzQ4LTkzN2ZjY2QyMjY0ZCJ9.obYaa744bmJoQAFO-nh1sCwPKwArWaOUo9_a1I0Uzc--HBuTLy6oOJVmnVI62bxnMkqoYo97SYGlKGKwVStz5g")
    feast_url = os.getenv("FEAST_URL", "http://localhost:8086")
    dataset_id = os.getenv("DATASET_ID", "318e10a9-c579-4e8c-ad2e-47df377740f8")
    try:
        connector = FeastConnector(
            passport_server_url=passport_server_url,
            study_id=study_id,
            experiment_id=experiment_id,
            organization_id=organization_id,
            connector_secret=connector_secret,
            feast_url=feast_url,
            dataset_id=dataset_id
        )

        connector.fetch_and_send_dataset()
    except Exception as e:
        print(f"[{datetime.now()}] ERROR: {e}", flush=True)
        print(traceback.format_exc(), flush=True)
