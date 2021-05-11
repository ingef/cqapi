from __future__ import annotations
from cqapi.queries.validate import validate_date, validate_date_range
from cqapi.conquery_ids import is_same_conquery_id, is_in_conquery_ids, get_dataset, contains_dataset_id, \
    add_dataset_id_to_conquery_id, get_root_concept_id, get_connector_id
from typing import List, Type


class QueryObject:
    """Base Class of all query elements"""

    def __init__(self, query_type: str, label: str = None):
        self.query_type = query_type
        self.label = label

    def write_query(self) -> dict:
        return {
            Keys.type: self.query_type,
            Keys.label: self.label
        }

    @classmethod
    def from_query(cls, query: dict) -> QueryObject:
        raise NotImplementedError()

    def set_label(self, label: str) -> None:
        self.label = label

    def add_concept_select(self, select_id: str) -> None:
        raise NotImplementedError()

    def add_connector_select(self, select_id: str) -> None:
        raise NotImplementedError()

    def add_connector_selects(self, select_ids: List[str]) -> None:
        for select_id in select_ids:
            self.add_concept_select(select_id=select_id)

    def add_filter(self, filter_obj: dict) -> None:
        raise NotImplementedError()

    def exclude_from_time_aggregation(self) -> None:
        raise NotImplementedError()

    def exclude_from_secondary_id(self) -> None:
        raise NotImplementedError()

    def and_with(self, *queries: QueryObject):
        return AndElement(children=[self, *queries])

    def or_with(self, *queries: QueryObject):
        return OrElement(children=[self, *queries])


class SingleRootQueryObject(QueryObject):
    """
    Base Class for all query elements that have one sub-query element with key "root" or "child":
    CONCEPT_QUERY, SECONDARY_ID_QUERY, DATE_RESTRICTION, NEGATION, ..
    """

    def __init__(self, root: QueryObject, query_type: str, label: str = None, root_child_key: str = "root"):
        super().__init__(query_type, label=label)

        self.root = root
        self.root_child_key = root_child_key

    @classmethod
    def from_query(cls, query: dict) -> QueryObject:
        raise NotImplementedError()

    def write_query(self) -> dict:
        return {
            **super().write_query(),
            self.root_child_key: self.root.write_query()
        }

    def add_concept_select(self, select_id: str) -> None:
        return self.root.add_concept_select(select_id)

    def add_connector_select(self, select_id: str) -> None:
        return self.root.add_connector_select(select_id)

    def unwrap(self):
        return self.root

    def exclude_from_secondary_id(self) -> None:
        return self.root.exclude_from_secondary_id()

    def exclude_from_time_aggregation(self) -> None:
        return self.root.exclude_from_time_aggregation()

    def add_filter(self, filter_obj: dict) -> None:
        return self.add_filter(filter_obj=filter_obj)


class ConceptQuery(SingleRootQueryObject):

    def __init__(self, root: QueryObject,
                 date_aggregation_mode: str = None):
        super().__init__(root=root, query_type=obj_to_query_type(ConceptQuery))

        self.date_aggregation_mode = date_aggregation_mode

    def write_query(self):
        return {
            **super().write_query(),
            Keys.date_aggregation_mode: self.date_aggregation_mode
        }

    @classmethod
    def from_query(cls, query: dict) -> QueryObject:
        validate_query_type(cls, query)

        root = convert_from_query(query[Keys.root])

        return cls(
            root=root,
            date_aggregation_mode=query.get(Keys.date_aggregation_mode)
        )


class SecondaryIdQuery(SingleRootQueryObject):
    def __init__(self, root: QueryObject, secondary_id: str):
        super().__init__(root=root, query_type=obj_to_query_type(SecondaryIdQuery))

        self.secondary_id = secondary_id

    def write_query(self):
        return {
            **super().write_query(),
            Keys.secondary_id: self.secondary_id
        }

    @classmethod
    def from_query(cls, query: dict) -> QueryObject:
        validate_query_type(cls, query)

        root = convert_from_query(query[Keys.root])
        return cls(
            root=root,
            secondary_id=query[Keys.secondary_id]
        )


class DateRestriction(SingleRootQueryObject):
    def __init__(self, child: QueryObject, start_date: str = None, end_date: str = None, label: str = None):
        super().__init__(root=child, query_type=obj_to_query_type(DateRestriction), label=label,
                         root_child_key=Keys.child)

        if start_date is not None:
            validate_date(start_date)
        if end_date is not None:
            validate_date(end_date)

        self.start_date = start_date
        self.end_date = end_date

    @classmethod
    def from_query(cls, query: dict) -> QueryObject:
        validate_query_type(cls, query)

        child = convert_from_query(query[Keys.child])

        return cls(
            child=child,
            start_date=query.get(Keys.date_range, {}).get(Keys.min),
            end_date=query.get(Keys.date_range, {}).get(Keys.max),
            label=query.get(Keys.label)
        )

    def write_query(self):
        query = {
            **super().write_query(),
            Keys.date_range: [self.start_date, self.end_date]
        }
        return query


class Negation(SingleRootQueryObject):
    def __init__(self, child: QueryObject, label: str = None):
        super().__init__(root=child, query_type=obj_to_query_type(Negation), label=label, root_child_key=Keys.child)

    @classmethod
    def from_query(cls, query: dict) -> QueryObject:
        validate_query_type(cls, query)

        return cls(
            child=convert_from_query(query[Keys.child]),
            label=query.get(Keys.label)
        )

    def write_query(self) -> dict:
        query = {
            **super().write_query()
        }
        return query


class AndOrElement(QueryObject):
    """
    Base class for query elements that have multiple sub query elements ("children").
    E.g. "AND", "OR"
    """

    def __init__(self, query_type: str, children: List[QueryObject], create_exist: bool = None, label: str = None):

        super().__init__(query_type=query_type)

        self.children = children
        self.label = label
        self.create_exist = create_exist

    @classmethod
    def from_query(cls, query: dict) -> QueryObject:
        validate_query_type(cls, query)

        children = [convert_from_query(child) for child in query[Keys.children]]
        return AndOrElement(
            query_type=query[Keys.type],
            children=children,
            create_exist=query.get(Keys.create_exist),
            label=query.get(Keys.label)
        )

    def add_concept_select(self, select_id: str):
        for child in self.children:
            child.add_concept_select(select_id)

    def add_connector_select(self, select_id: str):
        for child in self.children:
            child.add_connector_select(select_id)

    def add_filter(self, filter_obj: dict) -> None:
        for child in self.children:
            child.add_filter(filter_obj)

    def exclude_from_time_aggregation(self) -> None:
        for child in self.children:
            child.exclude_from_time_aggregation()

    def exclude_from_secondary_id(self) -> None:
        for child in self.children:
            child.exclude_from_secondary_id()

    def write_query(self):
        return {
            **super().write_query(),
            Keys.children: [child.write_query() for child in self.children],
            Keys.create_exist: self.create_exist
        }


class AndElement(AndOrElement):
    def __init__(self, children: List[QueryObject], create_exist: bool = None, label: str = None):
        super().__init__(query_type=obj_to_query_type(AndElement), children=children, create_exist=create_exist,
                         label=label)


class OrElement(AndOrElement):
    def __init__(self, children: List[QueryObject], create_exist: bool = None, label: str = None):
        super().__init__(query_type=obj_to_query_type(OrElement), children=children, create_exist=create_exist,
                         label=label)


class ConceptQueryTable:
    """ Table/Connectors for query element CONCEPT"""
    selects = list()
    filters = list()

    def __init__(self, connector_id: str, date_column_id: str = None,
                 select_ids: List[str] = None, filter_objs: List[dict] = None):
        self.connector_id = connector_id
        self.date_column = {Keys.value: date_column_id}

        if select_ids is not None:
            self.add_selects(select_ids=select_ids)

        if filter_objs is not None:
            self.add_filters(filter_objs=filter_objs)

    def add_select(self, select_id: str):
        self.selects.append(select_id)

    def add_selects(self, select_ids: list):
        for select_id in select_ids:
            self.add_select(select_id=select_id)

    def add_filter(self, filter_obj: dict):
        self.filters.append(filter_obj)

    def add_filters(self, filter_objs: List[dict]):
        for filter_obj in filter_objs:
            self.add_filter(filter_obj=filter_obj)

    def write_table(self):
        return {
            Keys.id: self.connector_id,
            Keys.date_column: self.date_column,
            Keys.filters: self.filters,
            Keys.selects: self.selects
        }


class SavedQuery(QueryObject):
    _exclude_from_secondary_id = None

    def __init__(self, query_id: str, label: str = None):
        super().__init__(query_type=obj_to_query_type(SavedQuery), label=label)

        self.query_id = query_id

    def exclude_from_secondary_id(self) -> None:
        self._exclude_from_secondary_id = True

    def write_query(self) -> dict:
        return {
            **super().write_query(),
            Keys.query: self.query_id,
            Keys.exclude_from_secondary_id: self._exclude_from_secondary_id
        }

    @classmethod
    def from_query(cls, query: dict) -> QueryObject:
        validate_query_type(cls, query)

        return cls(
            query_id=query[Keys.query],
            label=query.get(Keys.label)
        )

    def add_concept_select(self, select_id: str) -> None:
        pass

    def add_connector_select(self, select_id: str) -> None:
        pass

    def add_filter(self, filter_obj: dict) -> None:
        pass

    def exclude_from_time_aggregation(self) -> None:
        pass


class ConceptElement(QueryObject):
    """Query element of type "CONCEPT". Has no sub query elements."""

    # tables: List[ConceptQueryTable] = None
    def __init__(self, ids: list, concept: dict = None, tables: List[ConceptQueryTable] = None,
                 concept_selects: list = None, exclude_from_secondary_id: bool = None,
                 exclude_from_time_aggregation: bool = None, label: str = None):

        super().__init__(query_type=obj_to_query_type(ConceptElement), label=label)

        self.ids = ids
        self._exclude_from_secondary_id = exclude_from_secondary_id
        self._exclude_from_time_aggregation = exclude_from_time_aggregation
        self.selects = concept_selects or list()

        self.tables: List[ConceptQueryTable] = tables or list()
        if concept is not None:
            self.tables = self.create_tables(concept)

    @classmethod
    def from_query(cls, query: dict) -> QueryObject:
        validate_query_type(cls, query)

        tables = list()
        for query_table in query[Keys.tables]:
            tables.append(ConceptQueryTable(connector_id=query_table[Keys.id],
                                            date_column_id=query_table.get(Keys.date_column, {}).get(Keys.value),
                                            select_ids=query.get(Keys.selects),
                                            filter_objs=query.get(Keys.filters)))
        return cls(ids=query[Keys.ids],
                   label=query.get(Keys.label),
                   tables=tables,
                   concept_selects=query.get(Keys.selects, []),
                   exclude_from_secondary_id=query.get(Keys.exclude_from_secondary_id),
                   exclude_from_time_aggregation=query.get(Keys.exclude_from_time_aggregation)
                   )

    def create_tables(self, concept: dict, connector_ids: list = None):

        for table in concept[Keys.tables]:
            table_connector_id = table[Keys.connector_id]
            if connector_ids is not None and not is_in_conquery_ids(table_connector_id, connector_ids):
                continue
            self.tables.append(ConceptQueryTable(table_connector_id))

        if not self.tables:
            raise ValueError(f"Could not find any connector for concept element")

    def add_connector_select(self, select_id):
        from cqapi.conquery_ids import get_connector_id
        for table in self.tables:
            if is_same_conquery_id(get_connector_id(select_id), table.connector_id):
                table.add_select(select_id)

    def add_concept_select(self, select_id: str):

        if is_same_conquery_id(get_root_concept_id(self.ids[0]),
                               get_root_concept_id(select_id)):
            self.selects.append(select_id)

    def add_filter(self, filter_obj: dict) -> None:
        for table in self.tables:
            if is_same_conquery_id(get_connector_id(filter_obj[Keys.id]), table.connector_id):
                table.add_filter(filter_obj)

    def exclude_from_secondary_id(self) -> None:
        self._exclude_from_secondary_id = True

    def exclude_from_time_aggregation(self) -> None:
        self._exclude_from_time_aggregation = True

    def write_query(self):
        return {
            **super().write_query(),
            Keys.ids: self.ids,
            Keys.exclude_from_secondary_id: self._exclude_from_secondary_id,
            Keys.exclude_from_time_aggregation: self._exclude_from_time_aggregation,
            Keys.selects: self.selects,
            Keys.tables: [table.write_table() for table in self.tables]
        }


query_type_to_obj_map = {
    "CONCEPT": ConceptElement,
    "CONCEPT_QUERY": ConceptQuery,
    "AND": AndElement,
    "OR": OrElement,
    "SECONDARY_ID_QUERY": SecondaryIdQuery,
    "DATE_RESTRICTION": DateRestriction,
    "NEGATION": Negation,
    "SAVED_QUERY": SavedQuery
}


def convert_from_query(query: dict) -> QueryObject:
    return query_type_to_obj_map[query[Keys.type]].from_query(query)


def query_type_to_obj(query_type: str) -> Type[QueryObject]:
    return query_type_to_obj_map[query_type]


def obj_to_query_type(query_object_type: Type[QueryObject]) -> str:
    for key, value in query_type_to_obj_map.items():
        if value == query_object_type:
            return key


def validate_query_type(query_object_type: Type[QueryObject], query: dict):
    query_type = query[Keys.type]
    class_type = query_object_type
    valid_query_type = query_type_to_obj_map[query_type]
    if valid_query_type != class_type:
        raise ValueError(f"Can not create class {class_type} from query with type {query_type}, "
                         f"only from {valid_query_type}")


class Keys:
    type = "type"
    date_aggregation_mode = "dateAggregationMode"
    id = "id"
    ids = "ids"
    connector_id = "connectorId"
    date_column = "dateColumn"
    exclude_from_secondary_id = "excludeFromSecondaryIdQuery"
    exclude_from_time_aggregation = "excludeFromTimeAggregation"
    selects = "selects"
    filters = "filters"
    tables = "tables"
    root = "root"
    child = "child"
    children = "children"
    secondary_id = "secondaryId"
    date_range = "dateRange"
    min = "min"
    max = "max"
    label = "label"
    create_exist = "createExist"
    value = "value"
    query = "query"


