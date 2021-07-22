from __future__ import annotations
from cqapi.namespace import Keys
from cqapi.queries.validate import validate_resolution, validate_time_unit, validate_time_count, \
    validate_index_selector, validate_index_plament, validate_date
from cqapi.conquery_ids import is_same_conquery_id, is_in_conquery_ids, get_root_concept_id, get_connector_id, \
    get_dataset, change_dataset, ConqueryId, ConqueryIdCollection
from cqapi.search_conquery_id import find_concept_id
from typing import List, Type, Union, Tuple
from copy import deepcopy
from cqapi.exceptions import SavedQueryTranslationError, ExternalQueryTranslationError


def remove_null_values_from_query(query: dict):
    return {key: value for key, value in query.items() if value is not None}


class QueryObject:
    """Base Class of all query elements"""
    row_prefix: str = None

    def __init__(self, query_type: str, label: str = None):
        self.query_type = query_type
        self.label = label

    def copy(self):
        return QueryObject(query_type=self.query_type, label=self.label)

    def write_query(self) -> dict:
        query = {
            Keys.type: self.query_type,
            Keys.label: self.label,
            Keys.row_prefix: self.row_prefix
        }
        return remove_null_values_from_query(query)

    @classmethod
    def from_query(cls, query: dict) -> QueryObject:
        raise NotImplementedError()

    def set_label(self, label: str) -> None:
        self.label = label

    def get_concept_elements(self) -> List[ConceptElement]:
        pass

    def get_selects(self):
        return [*self.get_concept_selects(), *self.get_connector_selects()]

    def get_concept_selects(self) -> List[str]:
        return [concept_select
                for concept_element in self.get_concept_elements()
                for concept_select in concept_element.selects]

    def get_connector_selects(self):
        return [connector_select
                for concept_element in self.get_concept_elements()
                for table in concept_element.tables
                for connector_select in table.selects]

    def set_validity_date(self, validity_date_id: str) -> None:
        pass

    def remove_all_tables_but(self, connector_id: str) -> None:
        pass

    def add_concept_select(self, select_id: str) -> None:
        raise NotImplementedError()

    def add_concept_selects(self, select_ids: List[str]) -> None:
        for select_id in select_ids:
            self.add_concept_select(select_id)

    def add_connector_select(self, select_id: str) -> None:
        raise NotImplementedError()

    def add_connector_selects(self, select_ids: List[str]) -> None:
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

    def validate_sub_query(self, sub_query: QueryObject):
        if isinstance(sub_query, QueryDescription):
            self.raise_query_description_as_sub_query_error()

    def raise_query_description_as_sub_query_error(self):
        raise TypeError(f"QueryDescription can not be Sub-Query of {type(self)}")

    def unwrap(self):
        pass

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[str]) -> \
            Tuple[Union[QueryObject, None], Union[QueryObject, None]]:
        raise NotImplementedError

    def get_concept_ids(self):
        raise NotImplementedError

    def remove_connector_selects(self, connector_select_ids: List[str] = None):
        pass

    def remove_concept_selects(self, concept_select_ids: List[str] = None):
        pass

    def remove_all_selects(self):
        self.remove_concept_selects()
        self.remove_connector_selects()


class QueryDescription(QueryObject):

    def get_concept_elements(self) -> List[QueryObject]:
        raise NotImplementedError

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[str]) -> \
            Tuple[Union[QueryObject, None], Union[QueryObject, None]]:
        raise NotImplementedError

    def copy(self):
        raise NotImplementedError

    @classmethod
    def from_query(cls, query: dict) -> QueryObject:
        raise NotImplementedError

    def add_concept_select(self, select_id: str) -> None:
        raise NotImplementedError

    def add_connector_select(self, select_id: str) -> None:
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


class SingleRootQueryDescription(QueryDescription):
    def __init__(self, root: QueryObject, query_type: str, date_aggregation_mode: str):
        super().__init__(query_type)
        self.validate_sub_query(root)
        self.root = root
        self.date_aggregation_mode = date_aggregation_mode

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[str]):
        raise NotImplementedError

    def copy(self):
        raise NotImplementedError

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

    def set_validity_date(self, validity_date_id: str) -> None:
        self.root.set_validity_date(validity_date_id=validity_date_id)

    def add_concept_select(self, select_id: str) -> None:
        self.root.add_concept_select(select_id)

    def add_connector_select(self, select_id: str) -> None:
        self.root.add_connector_select(select_id)

    def remove_concept_selects(self, concept_select_ids: List[str] = None):
        self.root.remove_concept_selects(concept_select_ids=concept_select_ids)

    def remove_connector_selects(self, connector_select_ids: List[str] = None):
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

    def remove_all_tables_but(self, connector_id: str) -> None:
        self.root.remove_all_tables_but(connector_id=connector_id)


class SingleChildQueryObject(QueryObject):
    """
    Base Class for all query elements that have one sub-query element with key "root" or "child":
    CONCEPT_QUERY, SECONDARY_ID_QUERY, DATE_RESTRICTION, NEGATION, ..
    """

    def __init__(self, child: QueryObject, query_type: str, label: str = None):
        super().__init__(query_type, label=label)
        self.validate_sub_query(child)
        self.child = child

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[str]):
        raise NotImplementedError

    def copy(self):
        raise NotImplementedError

    @classmethod
    def from_query(cls, query: dict) -> QueryObject:
        raise NotImplementedError()

    def write_query(self) -> dict:
        query = {
            **super().write_query(),
            Keys.child: self.child.write_query()
        }
        return remove_null_values_from_query(query)

    def set_validity_date(self, validity_date_id: str) -> None:
        self.child.set_validity_date(validity_date_id=validity_date_id)

    def add_concept_select(self, select_id: str) -> None:
        self.child.add_concept_select(select_id)

    def add_connector_select(self, select_id: str) -> None:
        self.child.add_connector_select(select_id)

    def remove_concept_selects(self, concept_select_ids: List[str] = None):
        self.child.remove_concept_selects(concept_select_ids=concept_select_ids)

    def remove_connector_selects(self, connector_select_ids: List[str] = None):
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

    def remove_all_tables_but(self, connector_id: str) -> None:
        self.child.remove_all_tables_but(connector_id=connector_id)


class ExportForm(QueryDescription):
    def __init__(self, query_id: str, resolution: str):
        super().__init__(query_type=obj_to_query_type(ExportForm))
        validate_resolution(resolution)
        self.query_id = query_id
        self.resolution = resolution

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

    def write_query(self) -> dict:
        return {
            **super().write_query(),
            'queryGroup': self.query_id,
            'resolution': self.resolution
        }


class AbsoluteExportForm(ExportForm):
    def __init__(self, query_id: str, features: List[QueryObject], resolution: str = "COMPLETE",
                 date_range: Union[List[str], dict] = None, start_date: str = None, end_date: str = None):
        super().__init__(query_id=query_id, resolution=resolution)

        start_date, end_date = get_start_end_date(date_range=date_range, start_date=start_date, end_date=end_date)

        self.features = self.validate_and_prepare_features(features)
        self.start_date = start_date
        self.end_date = end_date

    def write_time_mode(self):
        return {
            "value": "ABSOLUTE",
            'dateRange': {
                'min': self.start_date,
                'max': self.end_date
            },
            'features': [feature.write_query() for feature in self.features]
        }

    def write_query(self) -> dict:
        return {
            **super().write_query(),
            "timeMode": self.write_time_mode()
        }


class EntityDateExportForm(AbsoluteExportForm):
    def __init__(self, query_id: str, features: List[QueryObject], resolution: str = "COMPLETE",
                 date_aggregation_mode: str = "LOGICAL",
                 date_range: Union[List[str], dict] = None, start_date: str = None, end_date: str = None):
        super().__init__(query_id=query_id, resolution=resolution, features=features,
                         date_range=date_range, start_date=start_date, end_date=end_date)

        self.date_aggregation_mode = date_aggregation_mode

    def write_query(self) -> dict:
        time_mode = self.write_time_mode()
        time_mode["dateAggregationMode"] = self.date_aggregation_mode
        time_mode["value"] = "ENTITY_DATE"
        return {
            **super().write_query(),
            "timeMode": time_mode
        }


# TODO: Deal with differences between ExportForm and QueryDescription -> Probably BaseQueryObject
class RelativeExportForm(ExportForm):
    def __init__(self, query_id: str, resolution: str = "COMPLETE", before_index_queries: list = None,
                 after_index_queries: list = None,
                 time_unit: str = "QUARTERS", time_count_before: int = 1, time_count_after: int = 1,
                 index_selector: str = 'EARLIEST', index_placement: str = 'BEFORE'):

        super().__init__(query_id=query_id, resolution=resolution)

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

        # extract concept from "CONCEPT_QUERY"-Objects
        if before_index_queries is None and after_index_queries is None:
            raise ValueError(f"Either before_index_queries or after_index_queries must be a list of QueryObjects")

        if before_index_queries is None:
            before_index_queries = list()
        else:
            before_index_queries = self.validate_and_prepare_features(before_index_queries)
        self.before_index_queries = before_index_queries

        if after_index_queries is None:
            after_index_queries = list()
        else:
            after_index_queries = self.validate_and_prepare_features(after_index_queries)
        self.after_index_queries = after_index_queries

    def write_query(self) -> dict:
        return {
            **super().write_query(),
            "timeMode": {
                'value': 'RELATIVE',
                'timeUnit': self.time_unit,
                'timeCountBefore': self.time_count_before,
                'timeCountAfter': self.time_count_after,
                'indexSelector': self.index_selector,
                'indexPlacement': self.index_placement,
                'features': [query.write_query() for query in self.before_index_queries],
                'outcomes': [query.write_query() for query in self.after_index_queries]
            }
        }


class ConceptQuery(SingleRootQueryDescription):

    def __init__(self, root: QueryObject,
                 date_aggregation_mode: str = None):
        super().__init__(root=root, query_type=obj_to_query_type(ConceptQuery),
                         date_aggregation_mode=date_aggregation_mode)

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[str]) -> \
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
    def from_query(cls, query: dict) -> QueryObject:
        validate_query_type(cls, query)
        root = convert_query_dict(query[Keys.root])

        return cls(
            root=root,
            date_aggregation_mode=query.get(Keys.date_aggregation_mode)
        )


class SecondaryIdQuery(SingleRootQueryDescription):
    secondary_id = None

    def __init__(self, root: QueryObject, secondary_id: str = None, date_aggregation_mode: str = None):
        super().__init__(root=root, query_type=obj_to_query_type(SecondaryIdQuery),
                         date_aggregation_mode=date_aggregation_mode)

        self.set_secondary_id(secondary_id=secondary_id)

    def set_secondary_id(self, secondary_id: str = None):
        self.secondary_id = secondary_id

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[str]) -> \
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

    def write_query(self) -> dict:
        query = {
            **super().write_query(),
            Keys.secondary_id: self.secondary_id,
        }
        return remove_null_values_from_query(query)

    @classmethod
    def from_query(cls, query: dict) -> QueryObject:
        validate_query_type(cls, query)

        root = convert_query_dict(query[Keys.root])
        return cls(
            root=root,
            secondary_id=query[Keys.secondary_id]
        )


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


class DateRestriction(SingleChildQueryObject):
    date_range = start_date = end_date = None

    def __init__(self, child: QueryObject, start_date: str = None, end_date: str = None,
                 date_range: Union[List[str], dict] = None,
                 label: str = None):
        super().__init__(child=child, query_type=obj_to_query_type(DateRestriction), label=label)

        start_date, end_date = get_start_end_date(date_range=date_range, start_date=start_date, end_date=end_date)
        self.start_date = start_date
        self.end_date = end_date

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[str]):
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
    def from_query(cls, query: dict) -> QueryObject:
        validate_query_type(cls, query)

        child = convert_query_dict(query[Keys.child])

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

    def copy(self):
        return Negation(child=self.child.copy(),
                        label=self.label)

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[str]):
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
    def from_query(cls, query: dict) -> QueryObject:
        validate_query_type(cls, query)

        return cls(
            child=convert_query_dict(query[Keys.child]),
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

    def __init__(self, query_type: str, children: List[QueryObject], create_exist: bool = None, label: str = None,
                 row_prefix: str = None):

        super().__init__(query_type=query_type)

        for child in children:
            if isinstance(child, QueryDescription):
                raise TypeError(f"{child=} is of type QueryDescription and not allowed to be child of {self.__class__}")
        self.children = children
        self.label = label
        self.create_exist = create_exist

        self.row_prefix = row_prefix

    def copy(self):
        raise NotImplementedError

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[str]):
        cls = type(self)

        children = list()
        new_children = list()

        for own_child in self.children:
            new_child, child = own_child.translate(concepts=concepts,
                                                   removed_ids=removed_ids,
                                                   children_ids=children_ids)
            if new_child is not None:
                new_children.append(new_child)
                children.append(child)

        if not new_children:
            return None, None

        new_and_element = cls(children=new_children,
                              create_exist=self.create_exist,
                              label=self.label,
                              row_prefix=self.row_prefix, query_type=self.query_type)
        and_element = cls(children=children,
                          create_exist=self.create_exist,
                          label=self.label,
                          row_prefix=self.row_prefix, query_type=self.query_type)

        return new_and_element, and_element

    @classmethod
    def from_query(cls, query: dict) -> QueryObject:
        validate_query_type(cls, query)

        children = [convert_query_dict(child) for child in query[Keys.children]]

        return cls(
            children=children,
            create_exist=query.get(Keys.create_exist),
            label=query.get(Keys.label),
            row_prefix=query.get(Keys.row_prefix),
            query_type=query[Keys.type]  # this is not used, see class doc
        )

    def set_validity_date(self, validity_date_id: str) -> None:
        for child in self.children:
            child.set_validity_date(validity_date_id=validity_date_id)

    def add_concept_select(self, select_id: str):
        for child in self.children:
            child.add_concept_select(select_id)

    def remove_concept_selects(self, concept_select_ids: List[str] = None):
        for child in self.children:
            child.remove_concept_selects(concept_select_ids=concept_select_ids)

    def add_connector_select(self, select_id: str):
        for child in self.children:
            child.add_connector_select(select_id)

    def remove_connector_selects(self, connector_select_ids: List[str] = None):
        for child in self.children:
            child.remove_connector_selects(connector_select_ids=connector_select_ids)

    def remove_all_tables_but(self, connector_id: str) -> None:
        for child in self.children:
            child.remove_all_tables_but(connector_id=connector_id)

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

    def get_concept_ids(self):
        root_concept_ids = set()
        for child in self.children:
            root_concept_ids = root_concept_ids.union(child.get_concept_ids())
        return root_concept_ids

    def get_concept_elements(self):
        return [concept_element for child in self.children for concept_element in child.get_concept_elements()]


class AndElement(AndOrElement):
    def __init__(self, children: List[QueryObject], create_exist: bool = None, label: str = None,
                 row_prefix: str = None, query_type: str = None):
        """

        :param children:
        :param create_exist:
        :param label:
        :param row_prefix:
        :param query_type: Not used. Implemented "set" a query type in AndOrElement.from_query()
        """
        super().__init__(query_type=obj_to_query_type(AndElement), children=children, create_exist=create_exist,
                         label=label, row_prefix=row_prefix)

    def copy(self):
        return AndElement(children=[child.copy() for child in self.children],
                          create_exist=self.create_exist, label=self.label)


class OrElement(AndOrElement):
    def __init__(self, children: List[QueryObject], create_exist: bool = None, label: str = None,
                 row_prefix: str = None, query_type: str = None):
        """

        :param children:
        :param create_exist:
        :param label:
        :param row_prefix:
        :param query_type: Not used. Implemented "set" a query type in AndOrElement.from_query()
        """
        super().__init__(query_type=obj_to_query_type(OrElement), children=children, create_exist=create_exist,
                         label=label, row_prefix=row_prefix)

    def copy(self):
        return OrElement(children=[child.copy() for child in self.children],
                         create_exist=self.create_exist, label=self.label)


class ConceptTable:
    """ Table/Connectors for query element CONCEPT"""

    def __init__(self, connector_id: str, date_column_id: str = None,
                 select_ids: List[str] = None, filter_objs: List[dict] = None):
        self.connector_id = connector_id
        self.date_column_id = date_column_id

        self.selects = select_ids or list()
        self.filters = filter_objs or list()

    def copy(self):
        return ConceptTable(connector_id=self.connector_id, date_column_id=self.date_column_id,
                            select_ids=deepcopy(self.selects), filter_objs=deepcopy(self.filters))

    def set_date_column_id(self, date_column_id: str):
        if is_same_conquery_id(get_connector_id(date_column_id), self.connector_id):
            self.date_column_id = date_column_id

    def add_select(self, select_id: str):
        self.selects.append(select_id)

    def remove_selects(self, select_ids: List[str] = None):
        if select_ids is None:
            self.selects = list()
        else:
            self.selects = [select for select in self.selects if not is_in_conquery_ids(select, select_ids)]

    def add_selects(self, select_ids: list):
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
        new_dataset = get_dataset(next(iter(concepts)))
        new_root_concept_id = change_dataset(new_dataset=new_dataset,
                                             conquery_id=get_root_concept_id(self.connector_id))
        new_connector_id = change_dataset(new_dataset=new_dataset,
                                          conquery_id=self.connector_id)

        # get table from concepts
        table_list = [table for table in concepts[new_root_concept_id]["tables"]
                      if is_same_conquery_id(table["connectorId"], new_connector_id)]
        if len(table_list) == 0:
            removed_ids.add(conquery_id=ConqueryId(self.connector_id, id_type="connector"))
            return None, None

        table = table_list[0]

        # translate date column
        date_column_id = self.date_column_id
        new_date_column_id = None
        if self.date_column_id is not None:
            new_date_column_id = change_dataset(new_dataset=new_dataset,
                                                conquery_id=self.date_column_id)
            table_date_column_ids = [date_column_obj[Keys.value]
                                     for date_column_obj in table[Keys.date_column][Keys.options]]
            if is_in_conquery_ids(new_date_column_id, table_date_column_ids):
                date_column_id = self.date_column_id
            else:
                removed_ids.add(ConqueryId(self.date_column_id, id_type="date"))

        # translate connector selects
        selects = list()
        new_selects = list()
        for select_id in self.selects:
            new_select_id = change_dataset(new_dataset=new_dataset, conquery_id=select_id)
            if new_select_id in [table_select[Keys.id] for table_select in table[Keys.selects]]:
                selects.append(select_id)
                new_selects.append(new_select_id)
            else:
                removed_ids.add(ConqueryId(select_id, id_type="connector_select"))

        filter_objs = list()
        new_filter_objs = list()

        # translate filter
        for filter_obj in self.filters:
            new_filter_id = change_dataset(new_dataset=new_dataset, conquery_id=filter_obj[Keys.filter])
            if is_in_conquery_ids(new_filter_id,
                                  [table_filter[Keys.id] for table_filter in table[Keys.filters]]):
                filter_objs.append(deepcopy(filter_obj))
                new_filter_obj = deepcopy(filter_obj)
                new_filter_obj[Keys.filter] = new_filter_id
                new_filter_objs.append(new_filter_obj)
            else:
                removed_ids.add(ConqueryId(filter_obj[Keys.filter], id_type="filter"))

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

    def write_table(self) -> dict:
        date_column = {Keys.value: self.date_column_id} if self.date_column_id is not None else None

        query = {
            Keys.id: self.connector_id,
            Keys.date_column: date_column,
            Keys.filters: self.filters if self.filters else None,
            Keys.selects: self.selects if self.selects else None
        }
        return remove_null_values_from_query(query)

    def __eq__(self, other: ConceptTable):
        if isinstance(other, ConceptTable):
            return self.connector_id == other.connector_id and \
                   self.selects == other.selects and \
                   [_[Keys.filter] for _ in self.filters] == [_[Keys.filter] for _ in other.filters] and \
                   self.date_column_id == other.date_column_id
        raise NotImplementedError


class ConceptElement(QueryObject):
    """Query element of type "CONCEPT". Has no sub query elements."""

    def __init__(self, ids: list, concept: dict = None, tables: List[ConceptTable] = None,
                 connector_ids: List[str] = None, concept_selects: list = None,
                 connector_selects: List[str] = None, filter_objs: List[str] = None,
                 exclude_from_secondary_id: bool = None,
                 exclude_from_time_aggregation: bool = None, label: str = None,
                 row_prefix: str = None):

        super().__init__(query_type=obj_to_query_type(ConceptElement), label=label)

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
                  children_ids: List[str]) -> Tuple[Union[ConceptElement, None], Union[ConceptElement, None]]:
        """
        Translates ConceptElement to new Dataset. Ids that can not be translated, are ignored.
        The Object itself won't be changed, a translated and a remaining query are returned.
        :param concepts: concepts of the new dataset
        :param removed_ids: ConqueryIdCollection to store removed ids
        :param children_ids: list of concept_ids of children concepts, since they are not stored in concepts
        :return: Translated concept element and remaining concept element
        """
        new_dataset = get_dataset(next(iter(concepts)))

        # translate concept ids
        new_concept_ids = list()
        concept_ids = list()
        for concept_id in self.ids:
            new_concept_id = change_dataset(new_dataset=new_dataset, conquery_id=concept_id)
            if find_concept_id(concept_id=new_concept_id, concepts=concepts, children_ids=children_ids):
                new_concept_ids.append(new_concept_id)
                concept_ids.append(concept_id)
            else:
                removed_ids.add(ConqueryId(conquery_id=concept_id, id_type="concept"))
        if not concept_ids:
            return None, None

        # translate concept selects
        new_concept_select_ids = list()
        concept_select_ids = list()
        for concept_select_id in self.selects:
            new_concept_select_id = change_dataset(new_dataset=new_dataset, conquery_id=concept_select_id)
            new_root_concept_id = get_root_concept_id(new_concept_select_id)
            if new_concept_select_id in [select[Keys.id] for select in concepts[new_root_concept_id].get(Keys.selects,
                                                                                                         [])]:
                new_concept_select_ids.append(new_concept_select_id)
                concept_select_ids.append(concept_select_id)
            else:
                removed_ids.add(ConqueryId(conquery_id=concept_select_id, id_type="concept_select"))

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
                   exclude_from_time_aggregation=query.get(Keys.exclude_from_time_aggregation),
                   row_prefix=query.get(Keys.row_prefix)
                   )

    def create_tables(self, concept: dict, connector_ids: List[str] = None,
                      selects: List[str] = None, filter_objs: List[dict] = None):

        for table in concept[Keys.tables]:
            table_connector_id = table[Keys.connector_id]
            if connector_ids is not None and not is_in_conquery_ids(table_connector_id, connector_ids):
                continue
            selects = selects or list()
            connector_selects = [select for select in selects
                                 if is_same_conquery_id(table_connector_id, get_connector_id(select))]
            filter_objs = filter_objs or list()
            connector_filters = [filter_obj for filter_obj in filter_objs
                                 if is_same_conquery_id(table_connector_id,
                                                        get_connector_id(filter_obj[Keys.filter]))]

            self.tables.append(ConceptTable(table_connector_id,
                                            select_ids=connector_selects,
                                            filter_objs=connector_filters))

        if not self.tables:
            raise ValueError(f"Could not find any connector for concept element")

    def set_validity_date(self, validity_date_id: str) -> None:
        for table in self.tables:
            table.set_date_column_id(validity_date_id)

    def add_connector_select(self, select_id):
        from cqapi.conquery_ids import get_connector_id
        for table in self.tables:
            if is_same_conquery_id(get_connector_id(select_id), table.connector_id):
                table.add_select(select_id)

    def remove_connector_selects(self, connector_select_ids: List[str] = None):
        for table in self.tables:
            table.remove_selects(select_ids=connector_select_ids)

    def add_concept_select(self, select_id: str):

        if is_same_conquery_id(get_root_concept_id(self.ids[0]),
                               get_root_concept_id(select_id)):
            self.selects.append(select_id)

    def remove_concept_selects(self, concept_select_ids: List[str] = None):
        if concept_select_ids is None:
            self.selects = list()
        else:
            self.selects = [select for select in self.selects if not is_in_conquery_ids(select, concept_select_ids)]

    def add_filter(self, filter_obj: dict) -> None:
        for table in self.tables:
            if is_same_conquery_id(get_connector_id(filter_obj[Keys.filter]), table.connector_id):
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
            Keys.selects: self.selects if self.selects else None,
            Keys.tables: [table.write_table() for table in self.tables]
        }
        return remove_null_values_from_query(query)

    def get_concept_ids(self):
        return set(self.ids)

    def get_concept_elements(self):
        return [self]

    def remove_table(self, connector_id: str):
        self.tables = [table
                       for table in self.tables
                       if not is_same_conquery_id(table.connector_id, connector_id)]

    def remove_all_tables_but(self, connector_id: str):
        self.tables = [table
                       for table in self.tables
                       if is_same_conquery_id(table.connector_id, connector_id)]

    def get_root_concept_id(self):
        return get_root_concept_id(self.ids[0])


class SimpleQuery(QueryObject):

    @classmethod
    def from_query(cls, query: dict) -> QueryObject:
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


class SavedQuery(SimpleQuery):

    def __init__(self, query_id: str, label: str = None, exclude_from_secondary_id: bool = None):
        super().__init__(query_type=obj_to_query_type(SavedQuery), label=label)

        self.query_id = query_id
        self._exclude_from_secondary_id = exclude_from_secondary_id

    def copy(self):
        return SavedQuery(query_id=self.query_id, label=self.label,
                          exclude_from_secondary_id=self._exclude_from_secondary_id)

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[str]):
        raise SavedQueryTranslationError

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


class External(SimpleQuery):

    def __init__(self, format_list: List[str], values: List[List[str]], label: str = None):
        super().__init__(query_type=obj_to_query_type(External), label=label)

        self.format_list = format_list
        self.values = values

    def copy(self):
        return External(label=self.label, format_list=self.format_list, values=self.values)

    def translate(self, concepts: dict, removed_ids: ConqueryIdCollection, children_ids: List[str]):
        raise ExternalQueryTranslationError

    @classmethod
    def from_query(cls, query: dict) -> QueryObject:
        validate_query_type(cls, query)

        return cls(
            format_list=query[Keys.format],
            values=query[Keys.values],
            label=query[Keys.label]
        )

    def write_query(self) -> dict:
        query = {
            **super().write_query(),
            Keys.format: self.format_list,
            Keys.values: self.values
        }
        return remove_null_values_from_query(query)


query_type_to_obj_map = {
    "CONCEPT": ConceptElement,
    "CONCEPT_QUERY": ConceptQuery,
    "AND": AndElement,
    "OR": OrElement,
    "SECONDARY_ID_QUERY": SecondaryIdQuery,
    "DATE_RESTRICTION": DateRestriction,
    "NEGATION": Negation,
    "SAVED_QUERY": SavedQuery,
    "EXPORT_FORM": ExportForm,
    "EXTERNAL": External
}


def convert_query_dict(query: dict) -> QueryObject:
    return query_type_to_obj_map[query[Keys.type]].from_query(query)


def convert_query_dict_list(queries: List[dict]) -> List[QueryObject]:
    return [convert_query_dict(query) for query in queries]


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
