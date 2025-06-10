import datetime
import os
import traceback
from typing import List

import jwt
import requests
from dateutil.parser import isoparse

import feast_models
import passport_models


class FeastConnector:
    """
    Feast Connector that fetches data from onFHIR Feast Server and sent them into the AI4HF Passport Server.
    """

    def __init__(self, passport_server_url: str, study_id: str, organization_id: str, experiment_id: str, username: str,
                 password: str, feast_url: str, dataset_id: str, timestamp_file: str):
        """
        Initialize the API client with authentication and study details.
        """
        self.passport_server_url = passport_server_url
        self.study_id = study_id
        self.organization_id = organization_id
        self.experiment_id = experiment_id
        self.username = username
        self.password = password
        self.feast_url = feast_url
        self.dataset_id = dataset_id
        self.timestamp_file = timestamp_file
        self.token = self._authenticate()

    def _authenticate(self) -> str:
        """
        Authenticate with login endpoint and retrieve an access token.
        """
        auth_url = f"{self.passport_server_url}/user/login"

        data = {
            "username": self.username,
            "password": self.password
        }

        response = requests.post(auth_url, json=data)
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
        url = f"{self.feast_url}/onfhir-feast/api/Dataset/{dataset_id}"
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
            isMandatory: bool = (root_object.entity.datasetStats.numOfEntries ==
                                 root_object.entity.datasetStats.featureStats[feast_feature.name].numOfNotNull)
            passport_feature: passport_models.Feature = passport_models.Feature(
                featuresetId=created_feature_set.featuresetId,
                title=feast_feature.name,
                description=feast_feature.description,
                dataType="Unknown",
                featureType=feast_feature.dataType,
                mandatory=isMandatory,
                isUnique=False,
                units="Unknown",
                equipment="Unknown",
                dataCollection="Unknown",
                createdBy=user_id,
                lastUpdatedBy=user_id)
            created_feature = self.send_feature(passport_feature)
            print(f"Created feature: {created_feature}", flush=True)

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
                   "featureType": feature.featureType, "mandatory": feature.mandatory,
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

    def load_last_processed_timestamp(self):
        if not os.path.exists(self.timestamp_file):
            print(f"[INFO] Timestamp file not found, assuming first run.")
            return None
        try:
            with open(self.timestamp_file, "r") as f:
                content = f.read().strip()
                return isoparse(content) if content else None
        except Exception as e:
            print(f"[WARN] Failed to read timestamp file: {e}")
            return None

    def save_last_processed_timestamp(self, ts: datetime):
        try:
            with open(self.timestamp_file, "w") as f:
                f.write(ts.isoformat())
            print(f"[INFO] Updated last processed timestamp: {ts.isoformat()}")
        except Exception as e:
            print(f"[ERROR] Failed to write timestamp file: {e}")


if __name__ == "__main__":
    print("passport-onfhir-feast-connector has been started.")
    passport_server_url = os.getenv("PASSPORT_SERVER_URL", "http://localhost:80/ai4hf/passport/api")
    study_id = os.getenv("STUDY_ID", "initial_study")
    experiment_id = os.getenv("EXPERIMENT_ID", "initial_experiment")
    organization_id = os.getenv("ORGANIZATION_ID", "initial_organization")
    username = os.getenv("USERNAME", "data_engineer")
    password = os.getenv("PASSWORD", "data_engineer")
    feast_url = os.getenv("FEAST_URL", "http://localhost:8086")
    dataset_id = os.getenv("DATASET_ID", "318e10a9-c579-4e8c-ad2e-47df377740f8")
    timestamp_file = os.getenv("TIMESTAMP_FILE", "/data/last_processed_timestamp.txt")
    try:
        connector = FeastConnector(
            passport_server_url=passport_server_url,
            study_id=study_id,
            experiment_id=experiment_id,
            organization_id=organization_id,
            username="data_engineer",
            password=password,
            feast_url=feast_url,
            dataset_id=dataset_id,
            timestamp_file=timestamp_file
        )

        connector.fetch_and_send_dataset()
    except Exception as e:
        print(f"[{datetime.now()}] CRON ERROR: {e}", flush=True)
        print(traceback.format_exc(), flush=True)
