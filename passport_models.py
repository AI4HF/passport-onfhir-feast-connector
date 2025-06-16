import json
from typing import Optional


class Population:
    def __init__(self, studyId: str, populationUrl: str, description: str, characteristics: str,
                 populationId: Optional[str] = None):
        """
        Initialize the Population object from arguments.
        """
        self.populationId = populationId
        self.studyId = studyId
        self.populationUrl = populationUrl
        self.description = description
        self.characteristics = characteristics

    def __str__(self):
        return json.dumps(
            {"populationId": self.populationId, "studyId": self.studyId, "populationUrl": self.populationUrl,
             "description": self.description, "characteristics": self.characteristics})


class FeatureSet:
    def __init__(self, experimentId: str, title: str, featuresetURL: str, description: str,
                 createdAt: Optional[str] = None, createdBy: Optional[str] = None, lastUpdatedAt: Optional[str] = None,
                 lastUpdatedBy: Optional[str] = None, featuresetId: Optional[str] = None):
        """
        Initialize the FeatureSet object from arguments.
        """
        self.featuresetId = featuresetId
        self.experimentId = experimentId
        self.title = title
        self.featuresetURL = featuresetURL
        self.description = description
        self.createdAt = createdAt
        self.createdBy = createdBy
        self.lastUpdatedAt = lastUpdatedAt
        self.lastUpdatedBy = lastUpdatedBy

    def __str__(self):
        return json.dumps({"featuresetId": self.featuresetId, "experimentId": self.experimentId,
                           "title": self.title, "featuresetURL": self.featuresetURL,
                           "description": self.description, "createdAt": self.createdAt,
                           "createdBy": self.createdBy, "lastUpdatedAt": self.lastUpdatedAt,
                           "lastUpdatedBy": self.lastUpdatedBy})


class Feature:
    def __init__(self, featuresetId: str, title: str, description: str, dataType: str, isOutcome: bool,
                 mandatory: bool, isUnique: bool, units: str, equipment: str, dataCollection: str,
                 createdAt: Optional[str] = None, createdBy: Optional[str] = None, lastUpdatedAt: Optional[str] = None,
                 lastUpdatedBy: Optional[str] = None, featureId: Optional[str] = None):
        """
        Initialize the Feature object from arguments.
        """
        self.featureId = featureId
        self.featuresetId = featuresetId
        self.title = title
        self.description = description
        self.dataType = dataType
        self.isOutcome = isOutcome
        self.mandatory = mandatory
        self.isUnique = isUnique
        self.units = units
        self.equipment = equipment
        self.dataCollection = dataCollection
        self.createdAt = createdAt
        self.createdBy = createdBy
        self.lastUpdatedAt = lastUpdatedAt
        self.lastUpdatedBy = lastUpdatedBy

    def __str__(self):
        return json.dumps({"featureId": self.featureId, "featuresetId": self.featuresetId, "title": self.title,
                           "description": self.description, "dataType": self.dataType, "isOutcome": self.isOutcome,
                           "mandatory": self.mandatory, "isUnique": self.isUnique, "units": self.units,
                           "equipment": self.equipment, "dataCollection": self.dataCollection,
                           "createdAt": self.createdAt,
                           "createdBy": self.createdBy, "lastUpdatedAt": self.lastUpdatedAt,
                           "lastUpdatedBy": self.lastUpdatedBy})


class Dataset:
    def __init__(self, featuresetId: str, populationId: str, organizationId: str, title: str, description: str,
                 version: str, referenceEntity: str, numOfRecords: int, synthetic: bool,
                 createdAt: Optional[str] = None, createdBy: Optional[str] = None, lastUpdatedAt: Optional[str] = None,
                 lastUpdatedBy: Optional[str] = None, datasetId: Optional[str] = None):
        """
        Initialize the Feature object from arguments.
        """
        self.datasetId = datasetId
        self.featuresetId = featuresetId
        self.populationId = populationId
        self.organizationId = organizationId
        self.title = title
        self.description = description
        self.version = version
        self.referenceEntity = referenceEntity
        self.numOfRecords = numOfRecords
        self.synthetic = synthetic
        self.createdAt = createdAt
        self.createdBy = createdBy
        self.lastUpdatedAt = lastUpdatedAt
        self.lastUpdatedBy = lastUpdatedBy

    def __str__(self):
        return json.dumps(
            {"datasetId": self.datasetId, "featuresetId": self.featuresetId, "populationId": self.populationId,
             "organizationId": self.organizationId, "title": self.title, "description": self.description,
             "version": self.version, "referenceEntity": self.referenceEntity, "numOfRecords": self.numOfRecords,
             "synthetic": self.synthetic, "createdAt": self.createdAt, "createdBy": self.createdBy,
             "lastUpdatedAt": self.lastUpdatedAt, "lastUpdatedBy": self.lastUpdatedBy})

class FeatureDatasetCharacteristic:
    def __init__(self, datasetId: str, featureId: str, characteristicName: str, value: str,
                 valueDataType: str):
        """
        Initialize the Feature Dataset Characteristic object from arguments.
        """
        self.datasetId = datasetId
        self.featureId = featureId
        self.characteristicName = characteristicName
        self.value = value
        self.valueDataType = valueDataType

    def __str__(self):
        return json.dumps(
            {"datasetId": self.datasetId, "featureId": self.featureId, "characteristicName": self.characteristicName,
             "value": self.value, "valueDataType": self.valueDataType})
