from enum import unique, Enum
from typing import Union, List, Type

from cqapi.conquery_ids import get_dataset as get_dataset_from_id
from cqapi.namespace import Keys
from cqapi.queries import validate_date, QueryObject
from cqapi.queries.base_elements import QueryObject, ConceptElement, ConceptQuery, AndElement, OrElement, \
    SecondaryIdQuery, DateRestriction, Negation, SavedQuery, External
from cqapi.queries.form_elements import ExportForm, FullExportForm
from cqapi.queries.validate import validate_date


def get_dataset_from_query(query: dict):
    if "type" not in query.keys():
        raise ValueError(f"Query object {query} has no key 'type'")

    query_type = query.get("type")

    if query_type == "SECONDARY_ID_QUERY":
        return get_dataset_from_id(query["secondaryId"])

    if query_type == "CONCEPT_QUERY":
        return get_dataset_from_query(query["root"])

    if query_type in ["DATE_RESTRICTION", "NEGATION"]:
        return get_dataset_from_query(query["child"])

    if query_type in ["AND", "OR"]:
        return get_dataset_from_query(query["children"][0])

    if query_type == "SAVED_QUERY":
        return get_dataset_from_id[query["query"]]

    if query_type == "CONCEPT":
        return get_dataset_from_id(query["ids"][0])

    if query_type == "EXPORT_FORM":
        return get_dataset_from_id(query["queryGroup"])

    if query_type == "EXTERNAL":
        raise ValueError(f"Can not guess dataset from External query")


def remove_null_values(func):
    def wrapper(*args, **kwargs):
        return {key: value for key, value in func(*args, **kwargs).items() if value is not None}

    return wrapper


def get_start_end_date(date_range: Union[List[str], dict] = None, start_date: str = None, end_date: str = None):
    if date_range is not None:
        if isinstance(date_range, dict):
            start_date = date_range["min"]
            end_date = date_range["max"]
        elif isinstance(date_range, list):
            start_date = date_range[0]
            end_date = date_range[1]
        else:
            raise TypeError(f"{date_range=} must be type List[str] or dict, not {type(date_range)}")

    if start_date is not None:
        validate_date(start_date)
    if end_date is not None:
        validate_date(end_date)

    return start_date, end_date


def get_query_obj_from_query_type(query: dict) -> Type[QueryObject]:
    """Helper to map ENUM to query_type, since we have to call value for each enum member"""
    query_type_to_obj_map = {
        QueryType.CONCEPT: ConceptElement,
        QueryType.CONCEPT_QUERY: ConceptQuery,
        QueryType.AND: AndElement,
        QueryType.OR: OrElement,
        QueryType.SECONDARY_ID_QUERY: SecondaryIdQuery,
        QueryType.DATE_RESTRICTION: DateRestriction,
        QueryType.NEGATION: Negation,
        QueryType.SAVED_QUERY: SavedQuery,
        QueryType.EXPORT_FORM: ExportForm,
        QueryType.EXTERNAL: External,
        QueryType.FULL_EXPORT_FORM: FullExportForm
    }
    for key, value in query_type_to_obj_map.items():
        if key.value == query[Keys.type]:
            return value
    raise ValueError(f"Could not find query_type {query[Keys.type]}")


def create_query_obj(query: dict) -> QueryObject:
    """Converts dict query to QueryObject"""
    return get_query_obj_from_query_type(query).from_query(query)


def create_query_obj_list(queries: List[dict]) -> List[QueryObject]:
    """Converts list of dicts to list of QueryObjects"""
    return [create_query_obj(query) for query in queries]


def validate_query_type(query_object_type: Type[QueryObject], query: dict):
    """Validates if type of query dict matches QueryObject"""
    query_type = query[Keys.type]
    class_type = query_object_type
    valid_query_type = get_query_obj_from_query_type(query)
    if valid_query_type != class_type:
        raise ValueError(f"Can not create class {class_type} from query with type {query_type}, "
                         f"only from {valid_query_type}")


@unique
class QueryType(Enum):
    CONCEPT = "CONCEPT",
    CONCEPT_QUERY = "CONCEPT_QUERY",
    AND = "AND"
    OR = "OR"
    SECONDARY_ID_QUERY = "SECONDARY_ID_QUERY"
    DATE_RESTRICTION = "DATE_RESTRICTION"
    NEGATION = "NEGATION"
    SAVED_QUERY = "SAVED_QUERY"
    EXPORT_FORM = "EXPORT_FORM"
    EXTERNAL = "EXTERNAL"
    FULL_EXPORT_FORM = "FULL_EXPORT_FORM"