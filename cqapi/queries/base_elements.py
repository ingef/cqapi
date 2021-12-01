from __future__ import annotations
from cqapi.namespace import Keys, QueryType
from cqapi.queries.utils import remove_null_values, get_start_end_date
from cqapi.conquery_ids import ConqueryId, ConqueryIdCollection, ConceptId, ConnectorId, ChildId, DateId, SelectId, \
    get_copy_of_id_with_changed_dataset, FilterId, conquery_id_separator, get_dataset_from_id_string
from cqapi.search_conquery_id import find_concept_id
from typing import List, Union, Tuple, Type
from copy import deepcopy
from cqapi.exceptions import SavedQueryTranslationError, ExternalQueryTranslationError
import attr
import json
from typeguard import typechecked


def validate_root_child_query(instance, attribute, value):
    if isinstance(value, QueryDescription):
        raise TypeError(f"QueryDescription {value} can not be {attribute.name} of query {instance}")


def validate_children_queries(instance, attribute, value):
    for value_entry in value:
        validate_root_child_query(instance, attribute, value_entry)


@attr.s(auto_attribs=True, kw_only=True)
class QueryObject:
    """Base Class of all query elements"""
    row_prefix: str = attr.ib(None, init=False)
    query_type: QueryType
    label: str = None

    def __eq__(self, other):
        return isinstance(other, QueryObject) and self.to_dict() == other.to_dict()

    def copy(self):
        return QueryObject(query_type=self.query_type, label=self.label)

    @remove_null_values
    def to_dict(self) -> dict:
        return {
            Keys.type: self.query_type.value,
            Keys.label: self.label,
            Keys.row_prefix: self.row_prefix
        }

    def print(self):
        print(json.dumps(self.to_dict(), indent=4))

    @classmethod
    def from_dict(cls, query: dict) -> QueryObject:
        raise NotImplementedError()

    def set_label(self, label: str) -> None:
        self.label = label

    def get_concept_elements(self) -> List[ConceptElement]:
        pass

    def get_selects(self):
        return [*self.get_concept_selects(), *self.get_connector_selects()]

    def get_concept_selects(self) -> List[SelectId]:
        return [concept_select
                for concept_element in self.get_concept_elements()
                for concept_select in concept_element.selects]

    def get_connector_selects(self):
        return [connector_select
                for concept_element in self.get_concept_elements()
                for table in concept_element.tables
                for connector_select in table.selects]

    def set_validity_date(self, validity_date_id: DateId) -> None:
        pass

    def remove_all_tables_but(self, connector_ids: List[ConnectorId]) -> None:
        pass

    def add_concept_select(self, select_id: SelectId) -> None:
        raise NotImplementedError()

    def add_concept_selects(self, select_ids: List[SelectId]) -> None:
        for select_id in select_ids:
            self.add_concept_select(select_id)

    def add_connector_select(self, select_id: SelectId) -> None:
        raise NotImplementedError()

    def add_connector_selects(self, select_ids: List[SelectId]) -> None:
        for select_id in select_ids:
            self.add_connector_select(select_id=select_id)

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

    def unwrap(self):
        pass

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[ChildId]) -> \
            Tuple[Union[QueryObject, None], Union[QueryObject, None]]:
        raise NotImplementedError

    def get_concept_ids(self):
        raise NotImplementedError

    def remove_connector_selects(self, connector_select_ids: List[SelectId] = None):
        pass

    def remove_concept_selects(self, concept_select_ids: List[SelectId] = None):
        pass

    def remove_all_selects(self):
        self.remove_concept_selects()
        self.remove_connector_selects()


class QueryDescription(QueryObject):

    def get_concept_elements(self) -> List[QueryObject]:
        raise NotImplementedError

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[ChildId]) -> \
            Tuple[Union[QueryObject, None], Union[QueryObject, None]]:
        raise NotImplementedError

    def copy(self):
        raise NotImplementedError

    @classmethod
    def from_dict(cls, query: dict) -> QueryObject:
        raise NotImplementedError

    def add_concept_select(self, select_id: SelectId) -> None:
        raise NotImplementedError

    def add_connector_select(self, select_id: SelectId) -> None:
        raise NotImplementedError

    def add_filter(self, filter_obj: dict) -> None:
        raise NotImplementedError

    def exclude_from_time_aggregation(self) -> None:
        raise NotImplementedError

    def exclude_from_secondary_id(self) -> None:
        raise NotImplementedError

    def unwrap(self):
        raise NotImplementedError()

    def get_concept_ids(self):
        raise NotImplementedError()


@attr.s(auto_attribs=True, kw_only=True)
class SingleRootQueryDescription(QueryDescription):
    root: QueryObject = attr.ib(validator=validate_root_child_query)
    date_aggregation_mode: str = None

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[ChildId]):
        raise NotImplementedError

    def copy(self):
        raise NotImplementedError

    @classmethod
    def from_dict(cls, query: dict) -> QueryObject:
        raise NotImplementedError()

    @remove_null_values
    def to_dict(self) -> dict:
        return {
            Keys.type: self.query_type.value,
            Keys.root: self.root.to_dict(),
            Keys.date_aggregation_mode: self.date_aggregation_mode
        }

    def set_label(self, label: str) -> None:
        raise ValueError(f"Class QueryDescription has no attribute label")

    def set_validity_date(self, validity_date_id: DateId) -> None:
        self.root.set_validity_date(validity_date_id=validity_date_id)

    def add_concept_select(self, select_id: SelectId) -> None:
        self.root.add_concept_select(select_id)

    def add_connector_select(self, select_id: SelectId) -> None:
        self.root.add_connector_select(select_id)

    def remove_concept_selects(self, concept_select_ids: List[SelectId] = None):
        self.root.remove_concept_selects(concept_select_ids=concept_select_ids)

    def remove_connector_selects(self, connector_select_ids: List[SelectId] = None):
        self.root.remove_connector_selects(connector_select_ids=connector_select_ids)

    def unwrap(self) -> QueryObject:
        return self.root

    def exclude_from_secondary_id(self) -> None:
        self.root.exclude_from_secondary_id()

    def exclude_from_time_aggregation(self) -> None:
        self.root.exclude_from_time_aggregation()

    def add_filter(self, filter_obj: dict) -> None:
        self.root.add_filter(filter_obj=filter_obj)

    def get_concept_ids(self):
        return self.root.get_concept_ids()

    def get_concept_elements(self):
        return self.root.get_concept_elements()

    def remove_all_tables_but(self, connector_ids: List[ConnectorId]) -> None:
        self.root.remove_all_tables_but(connector_ids=connector_ids)


@attr.s(auto_attribs=True, kw_only=True)
class SingleChildQueryObject(QueryObject):
    """
    Base Class for all query elements that have one sub-query element with key "root" or "child":
    CONCEPT_QUERY, SECONDARY_ID_QUERY, DATE_RESTRICTION, NEGATION, ..
    """
    child: QueryObject = attr.ib(validator=validate_root_child_query)

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[str]):
        raise NotImplementedError

    def copy(self):
        raise NotImplementedError

    @classmethod
    def from_dict(cls, query: dict) -> QueryObject:
        raise NotImplementedError()

    @remove_null_values
    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            Keys.child: self.child.to_dict()
        }

    def set_validity_date(self, validity_date_id: DateId) -> None:
        self.child.set_validity_date(validity_date_id=validity_date_id)

    def add_concept_select(self, select_id: SelectId) -> None:
        self.child.add_concept_select(select_id)

    def add_connector_select(self, select_id: SelectId) -> None:
        self.child.add_connector_select(select_id)

    def remove_concept_selects(self, concept_select_ids: List[SelectId] = None):
        self.child.remove_concept_selects(concept_select_ids=concept_select_ids)

    def remove_connector_selects(self, connector_select_ids: List[SelectId] = None):
        self.child.remove_connector_selects(connector_select_ids=connector_select_ids)

    def exclude_from_secondary_id(self) -> None:
        self.child.exclude_from_secondary_id()

    def exclude_from_time_aggregation(self) -> None:
        self.child.exclude_from_time_aggregation()

    def add_filter(self, filter_obj: dict) -> None:
        self.child.add_filter(filter_obj=filter_obj)

    def get_concept_ids(self):
        return self.child.get_concept_ids()

    def get_concept_elements(self):
        return self.child.get_concept_elements()

    def remove_all_tables_but(self, connector_ids: List[ConnectorId]) -> None:
        self.child.remove_all_tables_but(connector_ids=connector_ids)


@attr.s(auto_attribs=True, kw_only=True)
class ConceptQuery(SingleRootQueryDescription):
    query_type: QueryType = attr.ib(QueryType.CONCEPT_QUERY, init=False)

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[ChildId]) -> \
            Tuple[Union[QueryObject, None], Union[QueryObject, None]]:
        new_root, root = self.root.translate(concepts=concepts,
                                             removed_ids=removed_ids,
                                             children_ids=children_ids)
        if new_root is None:
            return None, None
        new_concept_query = ConceptQuery(root=new_root,
                                         date_aggregation_mode=self.date_aggregation_mode)
        concept_query = ConceptQuery(root=root,
                                     date_aggregation_mode=self.date_aggregation_mode)
        return new_concept_query, concept_query

    def copy(self):
        return ConceptQuery(root=self.root.copy(),
                            date_aggregation_mode=self.date_aggregation_mode)

    @classmethod
    def from_dict(cls, query: dict) -> QueryObject:
        validate_query_type(cls, query)
        root = create_query_obj(query[Keys.root])

        return cls(
            root=root,
            date_aggregation_mode=query.get(Keys.date_aggregation_mode)
        )


@attr.s(auto_attribs=True, kw_only=True)
class SecondaryIdQuery(SingleRootQueryDescription):
    query_type: QueryType = attr.ib(QueryType.SECONDARY_ID_QUERY, init=False)
    secondary_id: str = None

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[ChildId]) -> \
            Tuple[Union[QueryObject, None], Union[QueryObject, None]]:
        new_root, root = self.root.translate(concepts=concepts, removed_ids=removed_ids,
                                             children_ids=children_ids)
        if new_root is None:
            return None, None

        new_secondary_id_query = SecondaryIdQuery(root=new_root,
                                                  secondary_id=self.secondary_id,
                                                  date_aggregation_mode=self.date_aggregation_mode)
        secondary_id_query = SecondaryIdQuery(root=root,
                                              secondary_id=self.secondary_id,
                                              date_aggregation_mode=self.date_aggregation_mode)
        return new_secondary_id_query, secondary_id_query

    def copy(self):
        return SecondaryIdQuery(root=self.root.copy(), secondary_id=self.secondary_id,
                                date_aggregation_mode=self.date_aggregation_mode)

    @remove_null_values
    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            Keys.secondary_id: self.secondary_id,
        }

    @classmethod
    def from_dict(cls, query: dict) -> QueryObject:
        validate_query_type(cls, query)

        root = create_query_obj(query[Keys.root])
        return cls(
            root=root,
            secondary_id=query[Keys.secondary_id]
        )


class DateRestriction(SingleChildQueryObject):
    date_range = start_date = end_date = None

    def __init__(self, child: QueryObject, start_date: str = None, end_date: str = None,
                 date_range: Union[List[str], dict] = None,
                 label: str = None):
        super().__init__(child=child, query_type=QueryType.DATE_RESTRICTION, label=label)

        start_date, end_date = get_start_end_date(date_range=date_range, start_date=start_date, end_date=end_date)
        self.start_date = start_date
        self.end_date = end_date

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[ChildId]):
        new_child, child = self.child.translate(concepts=concepts,
                                                removed_ids=removed_ids, children_ids=children_ids)
        if new_child is None:
            return None, None

        new_date_restriction = DateRestriction(child=new_child,
                                               start_date=self.start_date,
                                               end_date=self.end_date,
                                               label=self.label)
        date_restriction = DateRestriction(child=child,
                                           start_date=self.start_date,
                                           end_date=self.end_date,
                                           label=self.label)

        return new_date_restriction, date_restriction

    def copy(self):
        return DateRestriction(child=self.child.copy(), start_date=self.start_date, end_date=self.end_date,
                               label=self.label)

    @classmethod
    def from_dict(cls, query: dict) -> QueryObject:
        validate_query_type(cls, query)

        child = create_query_obj(query[Keys.child])

        return cls(
            child=child,
            start_date=query.get(Keys.date_range, {}).get(Keys.min),
            end_date=query.get(Keys.date_range, {}).get(Keys.max),
            label=query.get(Keys.label)
        )

    @remove_null_values
    def to_dict(self):
        return {
            **super().to_dict(),
            Keys.date_range: {"min": self.start_date, "max": self.end_date}
        }


@attr.s(auto_attribs=True, kw_only=True)
class Negation(SingleChildQueryObject):
    query_type: QueryType = attr.ib(QueryType.NEGATION, init=False)
    child: QueryObject
    label: str = None

    def copy(self):
        return Negation(child=self.child.copy(),
                        label=self.label)

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[ChildId]):
        new_child, child = self.child.translate(concepts=concepts, removed_ids=removed_ids,
                                                children_ids=children_ids)
        if new_child is None:
            return None, None

        new_negation = Negation(child=new_child,
                                label=self.label)
        negation = Negation(child=child,
                            label=self.label)

        return new_negation, negation

    @classmethod
    def from_dict(cls, query: dict) -> QueryObject:
        validate_query_type(cls, query)

        return cls(
            child=create_query_obj(query[Keys.child]),
            label=query.get(Keys.label)
        )

    @remove_null_values
    def to_dict(self) -> dict:
        return {
            **super().to_dict()
        }


@attr.s(auto_attribs=True, kw_only=True)
class AndOrElement(QueryObject):
    """
    Base class for query elements that have multiple sub query elements ("children").
    E.g. "AND", "OR"
    """
    query_type: QueryType = attr.ib()
    children: List[QueryObject] = attr.ib(validator=validate_children_queries)
    create_exist: bool = None
    label: str = None
    row_prefix: str = None
    date_action: str = None

    @query_type.validator
    def validate_query_type(self, attribute, value):
        if value not in [QueryType.AND, QueryType.OR]:
            raise ValueError(f"{value} as {attribute.name} must be in {[QueryType.AND, QueryType.OR]}")

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[ChildId]) -> \
            Tuple[Union[QueryObject, None], Union[QueryObject, None]]:
        raise NotImplementedError

    def copy(self):
        raise NotImplementedError

    def translate_children(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[ChildId]):
        children = list()
        new_children = list()

        for own_child in self.children:
            new_child, child = own_child.translate(concepts=concepts,
                                                   removed_ids=removed_ids,
                                                   children_ids=children_ids)
            if new_child is not None:
                new_children.append(new_child)
                children.append(child)

        return new_children, children

    def set_validity_date(self, validity_date_id: DateId) -> None:
        for child in self.children:
            child.set_validity_date(validity_date_id=validity_date_id)

    def add_concept_select(self, select_id: SelectId):
        for child in self.children:
            child.add_concept_select(select_id)

    def remove_concept_selects(self, concept_select_ids: List[SelectId] = None):
        for child in self.children:
            child.remove_concept_selects(concept_select_ids=concept_select_ids)

    def add_connector_select(self, select_id: SelectId):
        for child in self.children:
            child.add_connector_select(select_id)

    def remove_connector_selects(self, connector_select_ids: List[SelectId] = None):
        for child in self.children:
            child.remove_connector_selects(connector_select_ids=connector_select_ids)

    def remove_all_tables_but(self, connector_ids: List[ConnectorId]) -> None:
        for child in self.children:
            child.remove_all_tables_but(connector_ids=connector_ids)

    def add_filter(self, filter_obj: dict) -> None:
        for child in self.children:
            child.add_filter(filter_obj)

    def exclude_from_time_aggregation(self) -> None:
        for child in self.children:
            child.exclude_from_time_aggregation()

    def exclude_from_secondary_id(self) -> None:
        for child in self.children:
            child.exclude_from_secondary_id()

    @remove_null_values
    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            Keys.children: [child.to_dict() for child in self.children],
            Keys.create_exist: self.create_exist,
            Keys.date_action: self.date_action
        }

    @classmethod
    def from_dict(cls, query: dict) -> QueryObject:
        raise NotImplementedError

    def get_concept_ids(self):
        root_concept_ids = set()
        for child in self.children:
            root_concept_ids = root_concept_ids.union(child.get_concept_ids())
        return root_concept_ids

    def get_concept_elements(self):
        return [concept_element for child in self.children for concept_element in child.get_concept_elements()]


@attr.s(auto_attribs=True, kw_only=True)
class AndElement(AndOrElement):
    query_type: QueryType = attr.ib(QueryType.AND, init=False)
    children: List[QueryObject]
    create_exist: bool = None
    label: str = None
    date_action: str = None
    row_prefix: str = None

    @query_type.validator
    def validate_query_type(self, attribute, value):
        if value != QueryType.AND:
            raise ValueError(f"{value} as {attribute.name} must be {QueryType.AND}")

    def copy(self):
        return AndElement(children=[child.copy() for child in self.children],
                          create_exist=self.create_exist, label=self.label,
                          date_action=self.date_action)

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[ChildId]):

        new_children, children = self.translate_children(concepts=concepts,
                                                         removed_ids=removed_ids,
                                                         children_ids=children_ids)
        if not new_children:
            return None, None

        new_and_element = AndElement(children=new_children,
                                     create_exist=self.create_exist,
                                     label=self.label,
                                     row_prefix=self.row_prefix)
        and_element = AndElement(children=children,
                                 create_exist=self.create_exist,
                                 label=self.label,
                                 row_prefix=self.row_prefix)

        return new_and_element, and_element

    @classmethod
    def from_dict(cls, query: dict) -> QueryObject:

        return cls(
            children=[create_query_obj(child) for child in query[Keys.children]],
            create_exist=query.get(Keys.create_exist),
            label=query.get(Keys.label),
            row_prefix=query.get(Keys.row_prefix)
        )


@attr.s(auto_attribs=True, kw_only=True)
class OrElement(AndOrElement):
    query_type: QueryType = attr.ib(QueryType.OR, init=False)
    children: List[QueryObject]
    create_exist: bool = None
    label: str = None
    date_action: str = None
    row_prefix: str = None

    @query_type.validator
    def validate_query_type(self, attribute, value):
        if value != QueryType.OR:
            raise ValueError(f"{value} as {attribute.name} must be {QueryType.OR}")

    def copy(self):
        return OrElement(children=[child.copy() for child in self.children],
                         create_exist=self.create_exist, label=self.label,
                         date_action=self.date_action)

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[ChildId]):

        new_children, children = self.translate_children(concepts=concepts,
                                                         removed_ids=removed_ids,
                                                         children_ids=children_ids)
        if not new_children:
            return None, None

        new_or_element = OrElement(children=new_children,
                                   create_exist=self.create_exist,
                                   label=self.label,
                                   row_prefix=self.row_prefix)
        or_element = OrElement(children=children,
                               create_exist=self.create_exist,
                               label=self.label,
                               row_prefix=self.row_prefix)

        return new_or_element, or_element

    @classmethod
    def from_dict(cls, query: dict) -> QueryObject:

        return cls(
            children=[create_query_obj(child) for child in query[Keys.children]],
            create_exist=query.get(Keys.create_exist),
            label=query.get(Keys.label),
            row_prefix=query.get(Keys.row_prefix)
        )


class ConceptTable:
    """ Table/Connectors for query element CONCEPT"""

    def __init__(self, connector_id: ConnectorId, date_column_id: DateId = None,
                 select_ids: List[SelectId] = None, filter_objs: List[dict] = None):
        self.connector_id = connector_id
        self.date_column_id = date_column_id

        self.selects = select_ids or list()
        self.filters = filter_objs or list()

    def copy(self):
        return ConceptTable(connector_id=self.connector_id, date_column_id=self.date_column_id,
                            select_ids=deepcopy(self.selects), filter_objs=deepcopy(self.filters))

    def set_date_column_id(self, date_column_id: DateId):
        if not isinstance(date_column_id, DateId):
            raise ValueError("Provided date column is not of instance DateId(ConqueryId)")
        if date_column_id.get_connector_id().is_same_id(self.connector_id):
            self.date_column_id = date_column_id

    def add_select(self, select_id: SelectId):
        self.selects.append(select_id)

    def remove_selects(self, select_ids: List[SelectId] = None):
        if select_ids is None:
            self.selects = list()
        else:
            self.selects = [select for select in self.selects if not select.is_in_id_list(select_ids)]

    def add_selects(self, select_ids: List[SelectId]):
        for select_id in select_ids:
            self.add_select(select_id=select_id)

    def add_filter(self, filter_obj: dict):
        self.filters.append(filter_obj)

    def add_filters(self, filter_objs: List[dict]):
        for filter_obj in filter_objs:
            self.add_filter(filter_obj=filter_obj)

    def remove_filters(self, filter_objs: List[dict]):
        for filter_obj in filter_objs:
            self.filters.remove(filter_obj)

    def translate(self, concepts: dict,
                  removed_ids: ConqueryIdCollection) -> Tuple[Union[ConceptTable, None], Union[ConceptTable, None]]:
        new_dataset = get_dataset_from_id_string(next(iter(concepts)))

        new_root_concept_id = get_copy_of_id_with_changed_dataset(new_dataset=new_dataset,
                                                                  conquery_id=self.connector_id.get_concept_id())
        new_connector_id = get_copy_of_id_with_changed_dataset(new_dataset=new_dataset,
                                                               conquery_id=self.connector_id)

        # get table from concepts
        table_list = [table for table in concepts[new_root_concept_id.id]["tables"]
                      if ConnectorId.from_str(table["connectorId"]).is_same_id(new_connector_id)]
        if len(table_list) == 0:
            removed_ids.add(self.connector_id.deepcopy())
            return None, None

        table = table_list[0]

        # translate date column
        date_column_id = self.date_column_id
        new_date_column_id = None
        if self.date_column_id is not None:
            new_date_column_id = get_copy_of_id_with_changed_dataset(new_dataset=new_dataset,
                                                                     conquery_id=self.date_column_id)
            table_date_column_ids = [DateId.from_str(date_column_obj[Keys.value])
                                     for date_column_obj in table[Keys.date_column][Keys.options]]
            if new_date_column_id.is_in_id_list(table_date_column_ids):
                date_column_id = self.date_column_id
            else:
                removed_ids.add(self.date_column_id.deepcopy())

        # translate connector selects
        selects = list()
        new_selects = list()
        for select_id in self.selects:
            new_select_id = get_copy_of_id_with_changed_dataset(new_dataset=new_dataset, conquery_id=select_id)
            if new_select_id.is_in_id_list([SelectId.from_str(table_select[Keys.id])
                                            for table_select in table[Keys.selects]]):
                selects.append(select_id)
                new_selects.append(new_select_id)
            else:
                removed_ids.add(select_id.deepcopy())

        filter_objs = list()
        new_filter_objs = list()

        # translate filter
        for filter_obj in self.filters:
            new_filter_id = get_copy_of_id_with_changed_dataset(new_dataset=new_dataset, conquery_id=filter_obj[Keys.filter])
            if new_filter_id.is_in_id_list([FilterId.from_str(table_filter[Keys.id])
                                            for table_filter in table[Keys.filters]]):
                filter_objs.append(deepcopy(filter_obj))
                new_filter_obj = deepcopy(filter_obj)
                new_filter_obj[Keys.filter] = new_filter_id
                new_filter_objs.append(new_filter_obj)
            else:
                removed_ids.add(filter_obj[Keys.filter].deepcopy())

        new_table = ConceptTable(connector_id=new_connector_id,
                                 date_column_id=new_date_column_id,
                                 select_ids=new_selects,
                                 filter_objs=new_filter_objs
                                 )
        table = ConceptTable(connector_id=self.connector_id,
                             date_column_id=date_column_id,
                             select_ids=selects,
                             filter_objs=filter_objs
                             )
        return new_table, table

    @remove_null_values
    def write_table(self) -> dict:
        date_column = {Keys.value: self.date_column_id.id} if self.date_column_id is not None else None

        filters = list()
        for filter_element in self.filters:
            filter_element[Keys.filter] = filter_element[Keys.filter].id
            filters.append(filter_element)

        return {
            Keys.id: self.connector_id.id,
            Keys.date_column: date_column,
            Keys.filters: filters if filters else None,
            Keys.selects: [select_element.id for select_element in self.selects] if self.selects else None
        }

    def __eq__(self, other: ConceptTable):
        if isinstance(other, ConceptTable):
            return self.connector_id == other.connector_id and \
                   self.selects == other.selects and \
                   [_[Keys.filter] for _ in self.filters] == [_[Keys.filter] for _ in other.filters] and \
                   self.date_column_id == other.date_column_id
        raise NotImplementedError


class ConceptElement(QueryObject):
    """Query element of type "CONCEPT". Has no sub query elements."""

    def __init__(self, ids: List[ConqueryId], concept: dict = None, tables: List[ConceptTable] = None,
                 connector_ids: List[ConnectorId] = None, concept_selects: List[SelectId] = None,
                 connector_selects: List[SelectId] = None, filter_objs: List[dict] = None,
                 exclude_from_secondary_id: bool = None,
                 exclude_from_time_aggregation: bool = None, label: str = None,
                 row_prefix: str = None):

        super().__init__(query_type=QueryType.CONCEPT, label=label)

        self.ids = ids
        self._exclude_from_secondary_id = exclude_from_secondary_id
        self._exclude_from_time_aggregation = exclude_from_time_aggregation
        self.selects = concept_selects or list()
        self.row_prefix = row_prefix

        self.tables: List[ConceptTable] = tables or list()
        if concept is not None:
            self.create_tables(concept=concept, connector_ids=connector_ids,
                               selects=connector_selects, filter_objs=filter_objs)

    def __eq__(self, other):
        if isinstance(other, ConceptElement):
            return set(self.ids) == set(other.ids) and \
                   self._exclude_from_time_aggregation == other._exclude_from_time_aggregation and \
                   set(self.selects) == set(other.selects)

    def copy(self):
        return ConceptElement(ids=self.ids, tables=[table.copy() for table in self.tables],
                              exclude_from_secondary_id=self._exclude_from_secondary_id,
                              exclude_from_time_aggregation=self._exclude_from_time_aggregation,
                              concept_selects=deepcopy(self.selects),
                              label=self.label)

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection,
                  children_ids: List[ChildId]) -> Tuple[Union[ConceptElement, None], Union[ConceptElement, None]]:
        """
        Translates ConceptElement to new Dataset. Ids that can not be translated, are ignored.
        The Object itself won't be changed, a translated and a remaining query are returned.
        :param concepts: concepts of the new dataset
        :param removed_ids: ConqueryIdCollection to store removed ids
        :param children_ids: list of concept_ids of children concepts, since they are not stored in concepts
        :return: Translated concept element and remaining concept element
        """
        new_dataset = get_dataset_from_id_string(next(iter(concepts)))

        # translate concept ids
        new_concept_ids = list()
        concept_ids = list()
        for concept_id in self.ids:
            new_concept_id = get_copy_of_id_with_changed_dataset(new_dataset=new_dataset, conquery_id=concept_id)
            if find_concept_id(concept_id=new_concept_id, concepts=concepts, children_ids=children_ids):
                new_concept_ids.append(new_concept_id)
                concept_ids.append(concept_id)
            else:
                removed_ids.add(concept_id.deepcopy())
        if not concept_ids:
            return None, None

        # translate concept selects
        new_concept_select_ids = list()
        concept_select_ids = list()
        for concept_select_id in self.selects:
            new_concept_select_id = get_copy_of_id_with_changed_dataset(new_dataset=new_dataset, conquery_id=concept_select_id)
            new_root_concept_id = new_concept_select_id.get_concept_id()
            if new_concept_select_id.is_in_id_list([SelectId.from_str(select[Keys.id])
                                                    for select in concepts[new_root_concept_id.id].get(
                    Keys.selects, [])]):
                new_concept_select_ids.append(new_concept_select_id)
                concept_select_ids.append(concept_select_id)
            else:
                removed_ids.add(concept_select_id.deepcopy())

        # translate tables
        new_tables = []
        tables = []
        for table in self.tables:
            new_table, table = table.translate(concepts=concepts, removed_ids=removed_ids)
            if new_table is not None:
                new_tables.append(new_table)
                tables.append(table)
        if not new_tables:
            return None, None

        new_concept = ConceptElement(ids=new_concept_ids,
                                     concept_selects=new_concept_select_ids,
                                     tables=new_tables,
                                     exclude_from_secondary_id=self._exclude_from_secondary_id,
                                     exclude_from_time_aggregation=self._exclude_from_time_aggregation,
                                     label=self.label)
        concept = ConceptElement(ids=concept_ids,
                                 concept_selects=concept_select_ids,
                                 tables=tables,
                                 exclude_from_secondary_id=self._exclude_from_secondary_id,
                                 exclude_from_time_aggregation=self._exclude_from_time_aggregation,
                                 label=self.label)
        return new_concept, concept

    @classmethod
    def from_dict(cls, query: dict) -> QueryObject:
        validate_query_type(cls, query)

        tables = list()
        for query_table in query[Keys.tables]:
            if query_table.get(Keys.date_column) is None:
                date_column_id = None
            else:
                date_column_id = query_table[Keys.date_column][Keys.value]
                if isinstance(date_column_id, str):
                    date_column_id = DateId.from_str(date_column_id)

            select_ids = [SelectId.from_str(select_id) if isinstance(select_id, str) else select_id
                          for select_id in query_table.get(Keys.selects)] \
                if query_table.get(Keys.selects) else None

            filter_objs = query_table.get(Keys.filters)
            if filter_objs:
                for i, filter_obj in enumerate(filter_objs):
                    if isinstance(filter_obj[Keys.filter], str):
                        filter_objs[i][Keys.filter] = FilterId.from_str(filter_obj[Keys.filter])

            connector_id = ConnectorId.from_str(query_table[Keys.id]) if isinstance(
                query_table[Keys.id], str) else query_table[Keys.id]

            tables.append(ConceptTable(connector_id=connector_id,
                                       date_column_id=date_column_id,
                                       select_ids=select_ids,
                                       filter_objs=filter_objs))

        ids = [ConceptId.from_str(id_string) if len(id_string.split(conquery_id_separator)) == 2 else
               ChildId.from_str(id_string) for id_string in query[Keys.ids]] if isinstance(query[Keys.ids][0], str) \
            else query[Keys.ids]
        concept_selects = [SelectId.from_str(id_string) if isinstance(id_string, str) else id_string
                           for id_string in query.get(Keys.selects, [])]

        return cls(ids=ids,
                   label=query.get(Keys.label),
                   tables=tables,
                   concept_selects=concept_selects,
                   exclude_from_secondary_id=query.get(Keys.exclude_from_secondary_id),
                   exclude_from_time_aggregation=query.get(Keys.exclude_from_time_aggregation),
                   row_prefix=query.get(Keys.row_prefix)
                   )

    def create_tables(self, concept: dict, connector_ids: List[ConnectorId] = None,
                      selects: List[SelectId] = None, filter_objs: List[dict] = None):

        for table in concept[Keys.tables]:
            table_connector_id = ConnectorId.from_str(table[Keys.connector_id])
            if connector_ids is not None and not table_connector_id.is_in_id_list(connector_ids):
                continue
            selects = selects or list()
            connector_selects = [select for select in selects
                                 if table_connector_id.is_same_id(select.get_connector_id())]
            filter_objs = filter_objs or list()
            connector_filters = [filter_obj for filter_obj in filter_objs
                                 if table_connector_id.is_same_id(filter_obj[Keys.filter].get_connector_id())]

            self.tables.append(ConceptTable(table_connector_id,
                                            select_ids=connector_selects,
                                            filter_objs=connector_filters))

        if not self.tables:
            raise ValueError(f"Could not find any connector for concept element")

    def set_validity_date(self, validity_date_id: DateId) -> None:
        for table in self.tables:
            table.set_date_column_id(validity_date_id)

    def add_connector_select(self, select_id: SelectId):
        for table in self.tables:
            if select_id.get_connector_id().is_same_id(table.connector_id):
                table.add_select(select_id)

    def remove_connector_selects(self, connector_select_ids: List[SelectId] = None):
        for table in self.tables:
            table.remove_selects(select_ids=connector_select_ids)

    def add_concept_select(self, select_id: SelectId):
        if self.ids[0].get_concept_id().is_same_id(select_id.get_concept_id()):
            self.selects.append(select_id)

    def remove_concept_selects(self, concept_select_ids: List[SelectId] = None):
        if concept_select_ids is None:
            self.selects = list()
        else:
            self.selects = [select for select in self.selects if not select.is_in_id_list(concept_select_ids)]

    def add_filter(self, filter_obj: dict) -> None:
        for table in self.tables:
            if filter_obj[Keys.filter].get_connector_id().is_same_id(table.connector_id):
                table.add_filter(filter_obj)

    def exclude_from_secondary_id(self) -> None:
        self._exclude_from_secondary_id = True

    def exclude_from_time_aggregation(self) -> None:
        self._exclude_from_time_aggregation = True

    @remove_null_values
    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            Keys.ids: [id_object.id for id_object in self.ids],
            Keys.exclude_from_secondary_id: self._exclude_from_secondary_id,
            Keys.exclude_from_time_aggregation: self._exclude_from_time_aggregation,
            Keys.selects: [select_object.id for select_object in self.selects] if self.selects else None,
            Keys.tables: [table.write_table() for table in self.tables]
        }

    def get_concept_ids(self):
        return set([conquery_id.id for conquery_id in self.ids])

    def get_concept_elements(self):
        return [self]

    def remove_table(self, connector_id: ConnectorId):
        self.tables = [table
                       for table in self.tables
                       if not table.connector_id.is_same_id(connector_id)]

    def remove_all_tables_but(self, connector_ids: List[ConnectorId]):
        self.tables = [table
                       for table in self.tables
                       if table.connector_id.is_in_id_list(connector_ids)]


class SimpleQuery(QueryObject):

    @classmethod
    def from_dict(cls, query: dict) -> QueryObject:
        raise NotImplementedError

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

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[str]) -> \
            Tuple[Union[QueryObject, None], Union[QueryObject, None]]:
        raise NotImplementedError

    def get_concept_ids(self):
        pass


@attr.s(auto_attribs=True, kw_only=True)
class SavedQuery(SimpleQuery):
    query_type: QueryType = attr.ib(QueryType.SAVED_QUERY, init=False)
    query_id: str
    label: str = None
    exclude_from_secondary_id_bool: bool = None

    def copy(self):
        return SavedQuery(query_id=self.query_id, label=self.label,
                          exclude_from_secondary_id_bool=self.exclude_from_secondary_id_bool)

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[str]):
        raise SavedQueryTranslationError

    def exclude_from_secondary_id(self) -> None:
        self.exclude_from_secondary_id_bool = True

    @remove_null_values
    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            Keys.query: self.query_id,
            # TODO war typo in conquery(Hotfix)
            # Keys.exclude_from_secondary_id: self.exclude_from_secondary_id_bool
            "excludeFromSecondaryId": self.exclude_from_secondary_id_bool
        }

    @classmethod
    def from_dict(cls, query: dict) -> QueryObject:
        validate_query_type(cls, query)

        return cls(
            query_id=query[Keys.query],
            label=query.get(Keys.label)
        )


@attr.s(auto_attribs=True, kw_only=True)
class External(SimpleQuery):
    format_list: List[str]
    values: List[List[str]]
    label: str = None
    query_type: QueryType = QueryType.EXTERNAL

    def copy(self):
        return External(label=self.label, format_list=self.format_list, values=self.values)

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[str]):
        raise ExternalQueryTranslationError

    @classmethod
    def from_dict(cls, query: dict) -> QueryObject:
        validate_query_type(cls, query)

        return cls(
            format_list=query[Keys.format],
            values=query[Keys.values],
            label=query[Keys.label]
        )

    @remove_null_values
    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            Keys.format: self.format_list,
            Keys.values: self.values
        }


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
        QueryType.EXTERNAL: External

    }
    for key, value in query_type_to_obj_map.items():
        if key.value == query[Keys.type]:
            return value
    raise ValueError(f"Could not find query_type {query[Keys.type]}")


def create_query_obj(query: dict) -> QueryObject:
    """Converts dict query to QueryObject"""
    return get_query_obj_from_query_type(query).from_dict(query)


def create_query_obj_list(queries: List[dict]) -> List[QueryObject]:
    """Converts list of dicts to list of QueryObjects"""
    return [create_query_obj(query) for query in queries]


@typechecked()
def create_query(concept_id: Union[ConceptId, List[ConceptId], ChildId, List[ChildId], str, List[str]],
                 concepts: dict,
                 concept_query: bool = False,
                 connector_ids: Union[List[ConnectorId], List[str]] = None,
                 concept_select_ids: Union[List[SelectId], List[str]] = None,
                 connector_select_ids: Union[List[SelectId], List[str]] = None,
                 filter_objs: List[dict] = None,
                 exclude_from_secondary_id: bool = None, exclude_from_time_aggregation: bool = None,
                 date_aggregation_mode: str = None,
                 start_date: str = None, end_date: str = None,
                 label: str = None,
                 negate: bool = False) -> QueryObject:

    if isinstance(concept_id, ConqueryId) or isinstance(concept_id, str):
        concept_ids = [concept_id]
    else:
        concept_ids = concept_id
    if isinstance(concept_ids, list):
        for index, concept_id_item in enumerate(concept_ids):
            if isinstance(concept_id_item, str):
                if len(concept_id_item.split(conquery_id_separator)) > 2:
                    concept_ids[index] = ChildId.from_str(concept_id_item)
                elif len(concept_id_item.split(conquery_id_separator)) == 2:
                    concept_ids[index] = ConceptId.from_str(concept_id_item)
                else:
                    raise ValueError(f"concept_id needs to contain at least one {conquery_id_separator}"
                                     f"to split on. Provided: {concept_id_item}")
    else:
        raise ValueError(f"{concept_ids=} must be of type List[str], List[ConqueryId], str or ConqueryId.")

    root_concept_id = concept_ids[0].get_concept_id().id

    if connector_ids:
        for index, connector_element in enumerate(connector_ids):
            if isinstance(connector_element, str):
                connector_ids[index] = ConnectorId.from_str(connector_element)

    if concept_select_ids:
        for index, concept_select_element in enumerate(concept_select_ids):
            if isinstance(concept_select_element, str):
                concept_select_ids[index] = SelectId.from_str(concept_select_element)

    if connector_select_ids:
        for index, connector_select_element in enumerate(connector_select_ids):
            if isinstance(connector_select_element, str):
                connector_select_ids[index] = SelectId.from_str(connector_select_element)

    if filter_objs:
        for index, filter_obj_element in enumerate(filter_objs):
            if isinstance(filter_obj_element["filter_id"], str):
                filter_objs[index]["filter_id"] = FilterId.from_str(filter_obj_element["filter_id"])

    query = ConceptElement(ids=concept_ids, concept=concepts[root_concept_id],
                           connector_ids=connector_ids,
                           concept_selects=concept_select_ids,
                           connector_selects=connector_select_ids,
                           filter_objs=filter_objs,
                           exclude_from_secondary_id=exclude_from_secondary_id,
                           exclude_from_time_aggregation=exclude_from_time_aggregation,
                           label=label)

    if start_date is not None or end_date is not None:
        query = DateRestriction(child=query, start_date=start_date, end_date=end_date)

    if negate:
        query = Negation(child=query)

    if concept_query:
        return ConceptQuery(root=query, date_aggregation_mode=date_aggregation_mode)

    return query


def validate_query_type(query_object_type: Type[QueryObject], query: dict):
    """Validates if type of query dict matches QueryObject"""
    query_type = query[Keys.type]
    class_type = query_object_type
    valid_query_type = get_query_obj_from_query_type(query)
    if valid_query_type != class_type:
        raise ValueError(f"Can not create class {class_type} from query with type {query_type}, "
                         f"only from {valid_query_type}")
