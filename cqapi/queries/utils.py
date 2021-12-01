from typing import List, Union
from cqapi.conquery_ids import get_dataset_from_id_string
from cqapi.namespace import Keys, QueryType
from cqapi.queries.validate import validate_date


def remove_null_values(func):
    def wrapper(*args, **kwargs):
        return {key: value for key, value in func(*args, **kwargs).items() if value is not None}

    return wrapper


def get_dataset_from_query(query: dict):
    if Keys.type not in query.keys():
        raise ValueError(f"Query object {query} has no key 'type'")

    query_type = map_query_type_to_enum(query.get(Keys.type))

    if query_type == QueryType.SECONDARY_ID_QUERY:
        return get_dataset_from_id_string(query[Keys.secondary_id])

    if query_type == QueryType.CONCEPT_QUERY:
        return get_dataset_from_query(query[Keys.root])

    if query_type in [QueryType.DATE_RESTRICTION, QueryType.NEGATION]:
        return get_dataset_from_query(query[Keys.child])

    if query_type in [QueryType.AND, QueryType.OR]:
        return get_dataset_from_query(query[Keys.children][0])

    if query_type == QueryType.SAVED_QUERY:
        return get_dataset_from_id_string(query[Keys.query])

    if query_type == QueryType.CONCEPT:
        return get_dataset_from_id_string(query[Keys.ids][0])

    if query_type == QueryType.EXPORT_FORM:
        return get_dataset_from_id_string(query[Keys.query_group])

    if query_type == QueryType.EXTERNAL:
        raise ValueError(f"Can not guess dataset from External query")


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


def map_query_type_to_enum(query_type: str):
    for qtype in QueryType:
        if qtype.value == query_type:
            return qtype
    raise ValueError(f"Unknown {query_type=}")
