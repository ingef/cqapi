from __future__ import annotations
from cqapi.queries.validate import validate_date
from cqapi.conquery_ids import is_same_conquery_id, is_in_conquery_ids, get_root_concept_id, get_connector_id
from cqapi.util import check_arg_type
from typing import List, Type, Union


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


def remove_null_values_from_query(query: dict):
    return {key: value for key, value in query.items() if value is not None}


class QueryObject:
    """Base Class of all query elements"""

    def __init__(self, query_type: str, label: str = None):
        self.query_type = query_type
        self.label = label

    def write_query(self) -> dict:
        query = {
            Keys.type: self.query_type,
            Keys.label: self.label
        }
        return remove_null_values_from_query(query)

    @classmethod
    def from_query(cls, query: dict) -> QueryObject:
        raise NotImplementedError()

    def set_label(self, label: str) -> None:
        self.label = label

    def add_concept_select(self, select_id: str) -> None:
        raise NotImplementedError()

    def add_concept_selects(self, select_ids: List[str]) -> None:
        for select_id in select_ids:
            self.add_concept_select(select_id)

    def add_connector_select(self, select_id: str) -> None:
        raise NotImplementedError()

    def add_connector_selects(self, select_ids: List[str]) -> None:
        for select_id in select_ids:
            self.add_concept_select(select_id=select_id)

    def add_filter(self, filter_obj: dict) -> None:
        raise NotImplementedError()

    def add_filters(self, filter_objs: List[dict]) -> None:
        for filter_obj in filter_objs:
            self.add_filter(filter_obj)

    def exclude_from_time_aggregation(self) -> None:
        raise NotImplementedError()

    def exclude_from_secondary_id(self) -> None:
        raise NotImplementedError()

    def and_with(self, *queries: QueryObject):
        return AndElement(children=[self, *queries])

    def or_with(self, *queries: QueryObject):
        return OrElement(children=[self, *queries])

    def validate_sub_query(self, sub_query: QueryObject):
        if isinstance(sub_query, QueryDescription):
            self.raise_query_description_as_sub_query_error()

    def raise_query_description_as_sub_query_error(self):
        raise TypeError(f"QueryDescription can not be Sub-Query of {type(self)}")

    def unwrap(self):
        pass


class QueryDescription(QueryObject):

    @classmethod
    def from_query(cls, query: dict) -> QueryObject:
        pass

    def add_concept_select(self, select_id: str) -> None:
        pass

    def add_connector_select(self, select_id: str) -> None:
        pass

    def add_filter(self, filter_obj: dict) -> None:
        pass

    def exclude_from_time_aggregation(self) -> None:
        pass

    def exclude_from_secondary_id(self) -> None:
        pass

    def unwrap(self):
        raise NotImplementedError()


class SingleRootQueryDescription(QueryDescription):
    def __init__(self, root: QueryObject, query_type: str, date_aggregation_mode: str):
        super().__init__(query_type)
        self.validate_sub_query(root)
        self.root = root
        self.date_aggregation_mode = date_aggregation_mode

    @classmethod
    def from_query(cls, query: dict) -> QueryObject:
        raise NotImplementedError()

    def write_query(self) -> dict:
        query = {
            Keys.type: self.query_type,
            Keys.root: self.root.write_query(),
            Keys.date_aggregation_mode: self.date_aggregation_mode
        }
        return remove_null_values_from_query(query)

    def set_label(self, label: str) -> None:
        raise ValueError(f"Class QueryDescription has no attribute label")

    def add_concept_select(self, select_id: str) -> None:
        self.root.add_concept_select(select_id)

    def add_connector_select(self, select_id: str) -> None:
        self.root.add_connector_select(select_id)

    def unwrap(self) -> QueryObject:
        return self.root

    def exclude_from_secondary_id(self) -> None:
        self.root.exclude_from_secondary_id()

    def exclude_from_time_aggregation(self) -> None:
        self.root.exclude_from_time_aggregation()

    def add_filter(self, filter_obj: dict) -> None:
        self.root.add_filter(filter_obj=filter_obj)


class SingleChildQueryObject(QueryObject):
    """
    Base Class for all query elements that have one sub-query element with key "root" or "child":
    CONCEPT_QUERY, SECONDARY_ID_QUERY, DATE_RESTRICTION, NEGATION, ..
    """

    def __init__(self, child: QueryObject, query_type: str, label: str = None):
        super().__init__(query_type, label=label)
        self.validate_sub_query(child)
        self.child = child

    @classmethod
    def from_query(cls, query: dict) -> QueryObject:
        raise NotImplementedError()

    def write_query(self) -> dict:
        query = {
            **super().write_query(),
            Keys.child: self.child.write_query()
        }
        return remove_null_values_from_query(query)

    def add_concept_select(self, select_id: str) -> None:
        self.child.add_concept_select(select_id)

    def add_connector_select(self, select_id: str) -> None:
        self.child.add_connector_select(select_id)

    def exclude_from_secondary_id(self) -> None:
        self.child.exclude_from_secondary_id()

    def exclude_from_time_aggregation(self) -> None:
        self.child.exclude_from_time_aggregation()

    def add_filter(self, filter_obj: dict) -> None:
        self.child.add_filter(filter_obj=filter_obj)


class ConceptQuery(SingleRootQueryDescription):

    def __init__(self, root: QueryObject,
                 date_aggregation_mode: str = None):
        super().__init__(root=root, query_type=obj_to_query_type(ConceptQuery),
                         date_aggregation_mode=date_aggregation_mode)

    @classmethod
    def from_query(cls, query: dict) -> QueryObject:
        validate_query_type(cls, query)
        root = convert_from_query(query[Keys.root])

        return cls(
            root=root,
            date_aggregation_mode=query.get(Keys.date_aggregation_mode)
        )


class SecondaryIdQuery(SingleRootQueryDescription):
    def __init__(self, root: QueryObject, secondary_id: str, date_aggregation_mode: str = None):
        super().__init__(root=root, query_type=obj_to_query_type(SecondaryIdQuery),
                         date_aggregation_mode=date_aggregation_mode)

        self.secondary_id = secondary_id

    def write_query(self) -> dict:
        query = {
            **super().write_query(),
            Keys.secondary_id: self.secondary_id,
        }
        return remove_null_values_from_query(query)

    @classmethod
    def from_query(cls, query: dict) -> QueryObject:
        validate_query_type(cls, query)

        root = convert_from_query(query[Keys.root])
        return cls(
            root=root,
            secondary_id=query[Keys.secondary_id]
        )


class DateRestriction(SingleChildQueryObject):
    def __init__(self, child: QueryObject, start_date: str = None, end_date: str = None, date_range: List[str] = None,
                 label: str = None):
        super().__init__(child=child, query_type=obj_to_query_type(DateRestriction), label=label)

        if date_range is not None:
            start_date = date_range[0]
            end_date = date_range[1]

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
            Keys.date_range: {"min": self.start_date, "max": self.end_date}
        }
        return remove_null_values_from_query(query)


class Negation(SingleChildQueryObject):
    def __init__(self, child: QueryObject, label: str = None):
        super().__init__(child=child, query_type=obj_to_query_type(Negation), label=label)

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
        return remove_null_values_from_query(query)


class AndOrElement(QueryObject):
    """
    Base class for query elements that have multiple sub query elements ("children").
    E.g. "AND", "OR"
    """

    def __init__(self, query_type: str, children: List[QueryObject], create_exist: bool = None, label: str = None):

        super().__init__(query_type=query_type)

        for child in children:
            if isinstance(child, QueryDescription):
                raise TypeError(f"Instance of ")
        self.children = children
        self.label = label
        self.create_exist = create_exist

    @classmethod
    def from_query(cls, query: dict) -> QueryObject:
        validate_query_type(cls, query)

        children = list()
        for child in query[Keys.children]:
            children.append(convert_from_query(child))
        # children = [convert_from_query(child) for child in query[Keys.children]]
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

    def write_query(self) -> dict:
        query = {
            **super().write_query(),
            Keys.children: [child.write_query() for child in self.children],
            Keys.create_exist: self.create_exist
        }
        return remove_null_values_from_query(query)


class AndElement(AndOrElement):
    def __init__(self, children: List[QueryObject], create_exist: bool = None, label: str = None):
        super().__init__(query_type=obj_to_query_type(AndElement), children=children, create_exist=create_exist,
                         label=label)


class OrElement(AndOrElement):
    def __init__(self, children: List[QueryObject], create_exist: bool = None, label: str = None):
        super().__init__(query_type=obj_to_query_type(OrElement), children=children, create_exist=create_exist,
                         label=label)


class SavedQuery(QueryObject):

    def __init__(self, query_id: str, label: str = None, exclude_from_secondary_id: bool = None):
        super().__init__(query_type=obj_to_query_type(SavedQuery), label=label)

        self.query_id = query_id
        self._exclude_from_secondary_id = exclude_from_secondary_id

    def exclude_from_secondary_id(self) -> None:
        self._exclude_from_secondary_id = True

    def write_query(self) -> dict:
        query = {
            **super().write_query(),
            Keys.query: self.query_id,
            Keys.exclude_from_secondary_id: self._exclude_from_secondary_id
        }
        return remove_null_values_from_query(query)

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


class ConceptTable:
    """ Table/Connectors for query element CONCEPT"""

    def __init__(self, connector_id: str, date_column_id: str = None,
                 select_ids: List[str] = None, filter_objs: List[dict] = None):
        self.connector_id = connector_id
        self.date_column = {Keys.value: date_column_id}

        self.selects = select_ids or list()
        self.filters = filter_objs or list()

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

    def write_table(self) -> dict:
        query = {
            Keys.id: self.connector_id,
            Keys.date_column: self.date_column,
            Keys.filters: self.filters,
            Keys.selects: self.selects
        }
        return remove_null_values_from_query(query)


class ConceptElement(QueryObject):
    """Query element of type "CONCEPT". Has no sub query elements."""

    def __init__(self, ids: list, concept: dict = None, tables: List[ConceptTable] = None,
                 connector_ids: List[str] = None, concept_selects: list = None,
                 connector_selects: List[str] = None, filter_objs: List[str] = None,
                 exclude_from_secondary_id: bool = None,
                 exclude_from_time_aggregation: bool = None, label: str = None):

        super().__init__(query_type=obj_to_query_type(ConceptElement), label=label)

        self.ids = ids
        self._exclude_from_secondary_id = exclude_from_secondary_id
        self._exclude_from_time_aggregation = exclude_from_time_aggregation
        self.selects = concept_selects or list()

        self.tables: List[ConceptTable] = tables or list()
        if concept is not None:
            self.tables = self.create_tables(concept=concept, connector_ids=connector_ids,
                                             selects=connector_selects, filter_objs=filter_objs)

    @classmethod
    def from_query(cls, query: dict) -> QueryObject:
        validate_query_type(cls, query)

        tables = list()
        for query_table in query[Keys.tables]:
            if query_table.get(Keys.date_column) is None:
                date_column_id = None
            else:
                date_column_id = query_table[Keys.date_column][Keys.value]

            tables.append(ConceptTable(connector_id=query_table[Keys.id],
                                       date_column_id=date_column_id,
                                       select_ids=query_table.get(Keys.selects),
                                       filter_objs=query_table.get(Keys.filters)))
        return cls(ids=query[Keys.ids],
                   label=query.get(Keys.label),
                   tables=tables,
                   concept_selects=query.get(Keys.selects, []),
                   exclude_from_secondary_id=query.get(Keys.exclude_from_secondary_id),
                   exclude_from_time_aggregation=query.get(Keys.exclude_from_time_aggregation)
                   )

    def create_tables(self, concept: dict, connector_ids: List[str] = None,
                      selects: List[str] = None, filter_objs: List[dict] = None):

        for table in concept[Keys.tables]:
            table_connector_id = table[Keys.connector_id]
            if connector_ids is not None and not is_in_conquery_ids(table_connector_id, connector_ids):
                continue
            connector_selects = [select for select in selects
                                 if is_same_conquery_id(table_connector_id, get_connector_id(select))]
            connector_filters = [filter_obj for filter_obj in filter_objs
                                 if is_same_conquery_id(table_connector_id, get_connector_id(filter_obj["filter"]))]
            self.tables.append(ConceptTable(table_connector_id,
                                            select_ids=connector_selects,
                                            filter_objs=connector_filters))

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

    def write_query(self) -> dict:
        query = {
            **super().write_query(),
            Keys.ids: self.ids,
            Keys.exclude_from_secondary_id: self._exclude_from_secondary_id,
            Keys.exclude_from_time_aggregation: self._exclude_from_time_aggregation,
            Keys.selects: self.selects,
            Keys.tables: [table.write_table() for table in self.tables]
        }
        return remove_null_values_from_query(query)


class QueryEditor:
    query: QueryObject = None

    def __init__(self, query: Union[QueryObject, dict, str]):
        if isinstance(query, dict):
            query = convert_from_query(query)
        if isinstance(query, str):
            query = SavedQuery(query_id=query)
        if not isinstance(query, QueryObject):
            raise ValueError(f"query must be of type QueryObject or dict")

        self.query = query

    def date_restriction(self, date_range: List[str] = None, start_date: str = None, end_date: str = None,
                         label: str = None):
        self.query = DateRestriction(child=self.query, date_range=date_range, start_date=start_date, end_date=end_date,
                                     label=label)

    def concept_query(self, date_aggregation_mode: str = None):
        self.query = ConceptQuery(root=self.query, date_aggregation_mode=date_aggregation_mode)

    def secondary_id_query(self, secondary_id: str, date_aggregation_mode: str = None):
        self.query = SecondaryIdQuery(root=self.query, date_aggregation_mode=date_aggregation_mode,
                                      secondary_id=secondary_id)

    def negate(self, label: str = None):
        self.query = Negation(child=self.query, label=label)

    def _and_or_queries(self, query_type: str, queries: List[Union[dict, QueryObject, QueryEditor, str]],
                        create_exist: bool = None, label: str = None):
        and_or_queries = [self.query]

        for query in queries:
            if isinstance(query, str):
                query = SavedQuery(query_id=query)
            if isinstance(query, dict):
                query = convert_from_query(query)
            if isinstance(query, QueryEditor):
                query = query.query
            if not isinstance(query, QueryObject):
                raise ValueError(f"Query must be of type Union[dict, QueryObject, QueryEditor], not {type(query)}")
            and_or_queries.append(query)

        self.query = AndOrElement(query_type=query_type, children=and_or_queries,
                                  create_exist=create_exist, label=label)

    def and_query(self, query: Union[dict, QueryObject, QueryEditor, str], create_exist: bool = None,
                  label: str = None):
        self._and_or_queries(query_type="AND", queries=[query],
                             create_exist=create_exist, label=label)

    def or_query(self, query: Union[dict, QueryObject, QueryEditor, str], create_exist: bool = None, label: str = None):
        self._and_or_queries(query_type="OR", queries=[query],
                             create_exist=create_exist, label=label)

    def and_queries(self, queries: List[Union[dict, QueryObject, QueryEditor, str]],
                    create_exist: bool = None, label: str = None):
        self._and_or_queries(query_type="AND", queries=queries,
                             create_exist=create_exist, label=label)

    def or_queries(self, queries: List[Union[dict, QueryObject, QueryEditor, str]],
                   create_exist: bool = None, label: str = None):
        self._and_or_queries(query_type="OR", queries=queries,
                             create_exist=create_exist, label=label)

    def exclude_from_secondary_id(self) -> None:
        self.query.exclude_from_secondary_id()

    def exclude_from_time_aggregation(self) -> None:
        self.query.exclude_from_time_aggregation()

    def add_concept_select(self, select_id: str) -> None:
        self.query.add_concept_select(select_id)

    def add_connector_select(self, select_id: str) -> None:
        self.query.add_connector_select(select_id)

    def add_concept_selects(self, select_ids: List[str]) -> None:
        self.query.add_concept_selects(select_ids)

    def add_connector_selects(self, select_ids: List[str]) -> None:
        self.query.add_connector_selects(select_ids)

    def unwrap(self):
        if isinstance(self.query, QueryDescription):
            self.query = self.query.unwrap()

    def add_filter(self, filter_obj: dict) -> None:
        self.query.add_filter(filter_obj=filter_obj)

    def add_filters(self, filter_objs: List[dict]) -> None:
        self.query.add_filters(filter_objs)

    def write_query(self):
        return self.query.write_query()


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


def create_query(concept_id: str, concepts: dict, concept_query: bool = False, connector_ids: List[str] = None,
                 concept_select_ids: List[str] = None, connector_select_ids: List[str] = None,
                 filter_objs: List[dict] = None,
                 exclude_from_secondary_id: bool = None, exclude_from_time_aggregation: bool = None,
                 date_aggregation_mode: str = None,
                 start_date: str = None, end_date: str = None) -> QueryObject:
    root_concept_id = get_root_concept_id(concept_id)

    query = ConceptElement(ids=[concept_id], concept=concepts[root_concept_id],
                           connector_ids=connector_ids,
                           concept_selects=concept_select_ids,
                           connector_selects=connector_select_ids,
                           filter_objs=filter_objs,
                           exclude_from_secondary_id=exclude_from_secondary_id,
                           exclude_from_time_aggregation=exclude_from_time_aggregation
                           )

    if start_date is not None or end_date is not None:
        query = DateRestriction(child=query, start_date=start_date, end_date=end_date)

    if concept_query:
        return ConceptQuery(root=query, date_aggregation_mode=date_aggregation_mode)

    return query
