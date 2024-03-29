from typing import List, Union, Dict, Type

from cqapi.namespace import Keys, QueryType
from cqapi.queries.base_elements import QueryObject, ConceptElement
from cqapi.queries.utils import remove_null_values, get_start_end_date
from cqapi.queries.base_elements import QueryDescription, SingleRootQueryDescription, SingleChildQueryObject
from cqapi.queries.validate import validate_resolutions, validate_time_unit, validate_time_count, \
    validate_index_selector, validate_index_plament
from cqapi.conquery_ids import ConqueryId, DateId


class ExportForm(QueryDescription):

    def __init__(self, query_id: str, features: List[QueryObject],
                 resolutions=None, create_resolution_subdivisions: bool = True):
        if resolutions is None:
            resolutions = ["COMPLETE"]
        super().__init__(query_type=QueryType.EXPORT_FORM)
        validate_resolutions(resolutions)
        self.query_id = query_id
        self.features = self.validate_and_prepare_features(features)
        self.resolutions = resolutions
        self.create_resolution_subdivisions = create_resolution_subdivisions

    @staticmethod
    def validate_and_prepare_features(features):
        new_features = list()
        for feature in features:
            if not isinstance(feature, QueryObject):
                raise ValueError(f"{feature=} not of type QueryObject")

            if isinstance(feature, QueryDescription):
                if isinstance(feature, SingleRootQueryDescription):
                    new_features.append(feature.root)
                    continue
                if isinstance(feature, SingleChildQueryObject):
                    new_features.append(feature.child)

                raise ValueError(f"At least one export form feature is of type QueryDescription "
                                 f"but not SingleRootQueryDescription: \n"
                                 f"{feature=}")
            else:
                new_features.append(feature)

        return new_features

    @remove_null_values
    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            Keys.query_group: self.query_id,
            Keys.resolutions: self.resolutions,
            Keys.create_resolution_subdivisions: self.create_resolution_subdivisions,
            Keys.features: [feature.to_dict() for feature in self.features]
        }


class AbsoluteExportForm(ExportForm):
    def __init__(self, query_id: str, features: List[QueryObject], resolutions: List[str] = None,
                 create_resolution_subdivisions: bool = True, date_range: Union[List[str], dict] = None,
                 start_date: str = None, end_date: str = None):
        super().__init__(query_id=query_id, features=features, resolutions=resolutions,
                         create_resolution_subdivisions=create_resolution_subdivisions)

        start_date, end_date = get_start_end_date(date_range=date_range, start_date=start_date, end_date=end_date)

        self.start_date = start_date
        self.end_date = end_date

    def write_time_mode(self):
        return {
            Keys.value: "ABSOLUTE",
            Keys.date_range: {
                Keys.min: self.start_date,
                Keys.max: self.end_date
            }
        }

    @remove_null_values
    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            Keys.time_mode: self.write_time_mode()
        }


class EntityDateExportForm(AbsoluteExportForm):
    def __init__(self, query_id: str, features: List[QueryObject], resolutions: List[str] = None,
                 create_resolution_subdivisions: bool = True, date_aggregation_mode: str = "LOGICAL",
                 alignment_hint: str = "YEAR", date_range: Union[List[str], dict] = None, start_date: str = None,
                 end_date: str = None):
        super().__init__(query_id=query_id, resolutions=resolutions,
                         create_resolution_subdivisions=create_resolution_subdivisions,
                         features=features, date_range=date_range, start_date=start_date, end_date=end_date)

        self.date_aggregation_mode = date_aggregation_mode
        self.alignment_hint = alignment_hint

    @remove_null_values
    def to_dict(self) -> dict:
        time_mode = self.write_time_mode()
        time_mode[Keys.date_aggregation_mode] = self.date_aggregation_mode
        time_mode[Keys.value] = "ENTITY_DATE"
        time_mode[Keys.alignment_hint] = self.alignment_hint
        return {
            **super().to_dict(),
            Keys.time_mode: time_mode
        }


class RelativeExportForm(ExportForm):
    def __init__(self, query_id: str, features: List[QueryObject],
                 resolutions: List[str] = None, create_resolution_subdivisions: bool = True,
                 time_unit: str = "QUARTERS", time_count_before: int = 1, time_count_after: int = 1,
                 index_selector: str = 'EARLIEST', index_placement: str = 'BEFORE'):
        super().__init__(query_id=query_id, features=features, resolutions=resolutions,
                         create_resolution_subdivisions=create_resolution_subdivisions)

        validate_time_unit(time_unit)
        self.time_unit = time_unit
        validate_time_count(time_count_before)
        self.time_count_before = time_count_before
        validate_time_count(time_count_after)
        self.time_count_after = time_count_after
        validate_index_selector(index_selector)
        self.index_selector = index_selector
        validate_index_plament(index_placement)
        self.index_placement = index_placement

    @remove_null_values
    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            Keys.time_mode: {
                Keys.value: 'RELATIVE',
                Keys.time_unit: self.time_unit,
                Keys.time_count_before: self.time_count_before,
                Keys.time_count_after: self.time_count_after,
                Keys.index_selector: self.index_selector,
                Keys.index_placement: self.index_placement
            }
        }


class FullExportForm(QueryDescription):
    def __init__(self, query_id: str,
                 concept_id: ConqueryId, concept: dict, validity_date_ids: List[DateId] = None,
                 start_date: str = None, end_date: str = None,
                 date_range: Union[List[str], Dict[str, str]] = None):
        super().__init__(query_type=QueryType.FULL_EXPORT_FORM, label=None)
        self.query_id = query_id
        start_date, end_date = get_start_end_date(date_range=date_range, start_date=start_date, end_date=end_date)
        self.start_date = start_date
        self.end_date = end_date

        self.tables: List[ConceptElement] = [ConceptElement(ids=[concept_id], concept=concept,
                                                            validity_date_ids=validity_date_ids)]

    @remove_null_values
    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            Keys.query_group: self.query_id,
            Keys.date_range: {"min": self.start_date, "max": self.end_date},
            Keys.tables: [table.to_dict() for table in self.tables]
        }


def get_query_obj_from_query_type(query: dict) -> Type[QueryObject]:
    """Helper to map ENUM to query_type, since we have to call value for each enum member"""
    query_type_to_obj_map = {
        QueryType.EXPORT_FORM: ExportForm,
        QueryType.FULL_EXPORT_FORM: FullExportForm
    }
    for key, value in query_type_to_obj_map.items():
        if key.value == query[Keys.type]:
            return value
    raise ValueError(f"Could not find query_type {query[Keys.type]}")
