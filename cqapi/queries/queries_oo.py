from copy import deepcopy
from cqapi.util import check_input_list
from cqapi.queries.validate import validate_date
from cqapi.conquery_ids import is_same_conquery_id, is_in_conquery_ids, get_dataset, contains_dataset_id, \
    add_dataset_id_to_conquery_id
from typing import List

cq_element_description = {
    "base_cq_elements": ["CONCEPT", "PERIOD_CONCEPT", "SAVED_QUERY"],
    "cq_element_collections": ["AND", "OR"],
    "cq_element_wrap": ["DATE_RESTRICTION", "NEGATION", "CONCEPT_QUERY"]
}
cq_elements = [__ for _ in cq_element_description.values() for __ in _]


class QueryObject:
    """Base Class of all query elements"""
    def __init__(self, query_type: str, label: str = None):
        self.query_type = query_type
        self.label = label

    def write_query(self) -> dict:
        return {
            "type": self.query_type,
            "label": self.label
        }

    def add_concept_select(self, select_id: str) -> None:
        raise NotImplementedError()

    def add_connector_select(self, select_id: str) -> None:
        raise NotImplementedError()

    def add_filter(self, filter_obj: str) -> None:
        raise NotImplementedError()


class SingleRootQueryObject(QueryObject):
    def __init__(self, root: QueryObject, query_type: str, label: str = None, root_child_key: str = "root"):
        super().__init__(query_type, label=label)
        self.root = root
        self.root_child_key = root_child_key

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


class ConceptQuery(SingleRootQueryObject):
    def __init__(self, root: QueryObject,
                 date_aggregation_mode: str = None, resolved_date_aggregation_mode: str = None):
        super().__init__(root=root, query_type="CONCEPT_QUERY")
        self.query_type = "CONCEPT_QUERY"
        self.date_aggregation_mode = date_aggregation_mode
        self.resolved_date_aggregation_mode = resolved_date_aggregation_mode

    def write_query(self):
        return {
            **super().write_query(),
            "dateAggregationMode": self.date_aggregation_mode,
            "resolvedDateAggregationMode": self.resolved_date_aggregation_mode
        }


class SecondaryIdQuery(SingleRootQueryObject):
    def __init__(self, root: QueryObject, secondary_id: str):
        super().__init__(root=root, query_type="SECONDARY_ID_QUERY")
        self.secondary_id = secondary_id

    def write_query(self):
        return {
            **super().write_query(),
            "secondaryId": self.secondary_id
        }


class ConceptQueryTable:
    selects = list()

    def __init__(self, connector_id: str):
        self.connector_id = connector_id

    def add_select(self, select_id: str):
        self.selects.append(select_id)


class AndOrElement(QueryObject):
    def __init__(self, query_type: str, children: List[QueryObject], create_exist: bool = None,
                 date_action: str = None, label: str = None):
        super().__init__(query_type=query_type)
        self.children = children
        self.date_action = date_action
        self.label = label,
        self.create_exist = create_exist

    def add_concept_select(self, select_id: str):
        for child in self.children:
            child.add_concept_select(select_id)

    def add_connector_select(self, select_id: str):
        for child in self.children:
            child.add_connector_select(select_id)

    def write_query(self):
        return {
            **super().write_query(),
            "children": [child.write_query() for child in self.children],
            "createExists": self.create_exist,
            "dateAction": self.date_action
        }


class AndElement(AndOrElement):
    def __init__(self, children: List[QueryObject], create_exist: bool = None,
                 date_action: str = None, label: str = None):
        super().__init__(query_type="AND", children=children, create_exist=create_exist, date_action=date_action,
                         label=label)


class OrElement(AndOrElement):
    def __init__(self, children: List[QueryObject], create_exist: bool = None,
                 date_action: str = None, label: str = None):
        super().__init__(query_type="OR", children=children, create_exist=create_exist, date_action=date_action,
                         label=label)


class DateRestriction(SingleRootQueryObject):
    def __init__(self, child: ConceptQuery, start_date: str = None, end_date: str = None, label: str = None):
        super().__init__(root=child, query_type="DATE_RESTRICTION", label=label, root_child_key="child")
        self.start_date = start_date
        self.end_date = end_date

    def write_query(self):
        query = {
            **super().write_query(),
            "dateRange": [self.start_date, self.end_date]
        }
        return query


class Negation(SingleRootQueryObject):
    def __init__(self, child: QueryObject, label: str = None, date_action: str = None):
        super().__init__(root=child, query_type="NEGATION", label=label, root_child_key="child")
        self.date_action = date_action

    def write_query(self) -> dict:
        query = {
            **super().write_query(),
            "dateAction": self.date_action
        }
        return query


class ConceptElement(QueryObject):
    tables: List[ConceptQueryTable] = None

    def __init__(self, ids: list, concept: dict, selects: list = None, secondary_id: bool = False,
                 time_aggregation: bool = False, label: str = None,
                 aggregate_event_dates: bool = None):
        super().__init__(query_type="CONCEPT", label=label)
        self.ids = ids
        self.aggregate_event_dates = aggregate_event_dates
        self.secondary_id = secondary_id
        self.time_aggregation = time_aggregation
        self.selects = selects if selects is not None else list()
        self.tables = self.create_tables(concept)

    def create_tables(self, concept: dict, connector_ids: list = None):
        # get connector ids
        table_connector_id_dict_list = list()
        for table in concept['tables']:
            table_connector_id = table['connectorId']
            if connector_ids is not None and not is_in_conquery_ids(table_connector_id, connector_ids):
                continue
            self.tables.append(ConceptQueryTable(table_connector_id))
            table_connector_id_dict_list.append({'id': table_connector_id})

        if not table_connector_id_dict_list:
            raise ValueError(f"Could not find any connector for concept element")

    def add_connector_select(self, select_id):
        from cqapi.conquery_ids import get_connector_id
        for table in self.tables:
            if is_same_conquery_id(get_connector_id(select_id), table.connector_id):
                table.add_select(select_id)

    def add_concept_select(self, select_id: str):
        from cqapi.conquery_ids import get_root_concept_id
        if is_same_conquery_id(get_root_concept_id(self.ids[0]),
                               get_root_concept_id(select_id)):
            self.selects.append(select_id)

    def write_query(self):
        return {
            **super().write_query(),
            "excludeFromSecondaryIdQuery": self.secondary_id,
            "excludeFromTimeAggregation": self.time_aggregation,
            "selects": self.selects,
            "tables": self.tables
        }
