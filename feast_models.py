import json
from typing import List, Optional, Dict, Any

class Pipeline:
    def __init__(self, reference: str, display: Optional[str] = None):
        self.reference = reference
        self.display = display


class Population:
    def __init__(self, url: str, title: str, description: Optional[str] = None, pipeline: Optional[Pipeline] = None):
        self.url = url
        self.title = title
        self.description = description
        self.pipeline = pipeline


class FeatureSet:
    def __init__(self, url: str, title: str, description: Optional[str] = None, pipeline: Optional[Pipeline] = None):
        self.url = url
        self.title = title
        self.description = description
        self.pipeline = pipeline


class DataSource:
    def __init__(self, id: str, name: str, interface: str, version: str, sourceType: str):
        self.id = id
        self.name = name
        self.interface = interface
        self.version = version
        self.sourceType = sourceType


class Temporal:
    def __init__(self, end: str, start: Optional[str] = None):
        self.start = start
        self.end = end


class Concept:
    def __init__(self, code: str, display: Optional[str] = None):
        self.code = code
        self.display = display


class ValueSet:
    def __init__(self, url: Optional[str], concept: Optional[List[Concept]] = None):
        self.url = url
        self.concept = concept


class Variable:
    def __init__(self, name: str, dataType: str, generatedDescription: List[str], description: Optional[str] = None,
                 valueSet: Optional[ValueSet] = None, default: Optional[Any] = None):
        self.name = name
        self.description = description
        self.dataType = dataType
        self.generatedDescription = generatedDescription
        self.valueSet = valueSet
        self.default = default


class Stats:
    def __init__(self, numOfNotNull: int, **kwargs):
        self.numOfNotNull = numOfNotNull
        self.additional_stats = kwargs


class PopulationStats:
    def __init__(self, numOfEntries: int, entityStats: Dict, eligibilityPeriodStats: Optional[Dict] = None,
                 eligibilityCriteriaStats: Optional[Dict] = None):
        self.numOfEntries = numOfEntries
        self.entityStats = entityStats
        self.eligibilityPeriodStats = eligibilityPeriodStats
        self.eligibilityCriteriaStats = eligibilityCriteriaStats


class DatasetStats:
    def __init__(self, numOfEntries: int, entityStats: Dict, samplingStats: Dict,
                 secondaryTimePointStats: Dict, featureStats: Dict[str, Stats], outcomeStats: Dict[str, Stats]):
        self.numOfEntries = numOfEntries
        self.entityStats = entityStats
        self.samplingStats = samplingStats
        self.secondaryTimePointStats = secondaryTimePointStats
        self.featureStats = featureStats
        self.outcomeStats = outcomeStats


class Entity:
    def __init__(self, id: str,
                 population: Population,
                 featureSet: FeatureSet,
                 dataSource: DataSource,
                 issued: str,
                 temporal: Temporal,
                 baseVariables: List[Variable],
                 features: List[Variable],
                 outcomes: List[Variable],
                 populationStats: PopulationStats,
                 datasetStats: DatasetStats):
        self.id = id
        self.population = population
        self.featureSet = featureSet
        self.dataSource = dataSource
        self.issued = issued
        self.temporal = temporal
        self.baseVariables = baseVariables
        self.features = features
        self.outcomes = outcomes
        self.populationStats = populationStats
        self.datasetStats = datasetStats


class RootObject:
    def __init__(self, entity: Entity):
        self.entity = entity

    def __str__(self):
        return json.dumps(self, default=lambda o: o.__dict__, indent=2)
