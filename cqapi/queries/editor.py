from __future__ import annotations
from typing import Union, List, Tuple, Optional
from cqapi.queries.base_elements import QueryObject, create_query_obj, SavedQuery, DateRestriction, ConceptQuery, \
    SecondaryIdQuery, Negation, AndElement, OrElement, QueryDescription, ConceptElement, create_query
from cqapi.conquery_ids import SecondaryId
from cqapi.queries.translation import translate_query
from cqapi.api import ConqueryConnection
from cqapi.conquery_ids import *


class QueryEditor:
    """Helper to build and execute queries"""
    query: QueryObject = None

    def __init__(self, query: Union[QueryObject, dict, str] = None):
        if isinstance(query, dict):
            query = create_query_obj(query)
        if isinstance(query, str):
            query = SavedQuery(query_id=query)
        if query is not None and not isinstance(query, QueryObject):
            raise ValueError(f"query must be of type QueryObject or dict. "
                             f"Type of query: {type(query)}")

        self.query = query

    def date_restriction(self, date_range: Union[List[str], dict] = None, start_date: str = None, end_date: str = None,
                         label: str = None):
        self.query = DateRestriction(child=self.query, date_range=date_range, start_date=start_date, end_date=end_date,
                                     label=label)

    def concept_query(self, date_aggregation_mode: str = None):
        self.query = ConceptQuery(root=self.query, date_aggregation_mode=date_aggregation_mode)

    def secondary_id_query(self, secondary_id: Union[str, SecondaryId], date_aggregation_mode: str = None):
        if isinstance(secondary_id, str):
            secondary_id = SecondaryId.from_str(secondary_id)

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
                query = create_query_obj(query)
            if isinstance(query, QueryEditor):
                query = query.query
            if not isinstance(query, QueryObject):
                raise ValueError(f"Query must be of type Union[dict, QueryObject, QueryEditor], not {type(query)}")
            and_or_queries.append(query.copy())

        if query_type == "AND":
            self.query = AndElement(children=and_or_queries, create_exist=create_exist, label=label)
        elif query_type == "OR":
            self.query = OrElement(children=and_or_queries, create_exist=create_exist, label=label)
        else:
            raise ValueError(f"Unknown {query_type=}")

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

    def add_concept_select(self, select_id: SelectId) -> None:
        self.query.add_concept_select(select_id)

    def add_connector_select(self, select_id: SelectId) -> None:
        self.query.add_connector_select(select_id)

    def add_concept_selects(self, select_ids: List[SelectId]) -> None:
        self.query.add_concept_selects(select_ids)

    def set_validity_date(self, validity_date_id: DateId) -> None:
        self.query.set_validity_date(validity_date_id=validity_date_id)

    def remove_concept_selects(self, concept_select_ids: List[SelectId] = None) -> None:
        self.query.remove_concept_selects(concept_select_ids=concept_select_ids)

    def remove_connector_selects(self, connector_select_ids: List[SelectId] = None) -> None:
        self.query.remove_connector_selects(connector_select_ids=connector_select_ids)

    def remove_all_selects(self):
        self.query.remove_all_selects()

    def remove_filters(self, filter_ids: List[FilterId] = None) -> None:
        self.query.remove_filters(filter_ids=filter_ids)

    def add_connector_selects(self, select_ids: List[SelectId]) -> None:
        self.query.add_connector_selects(select_ids)

    def unwrap(self):
        if isinstance(self.query, QueryDescription):
            self.query = self.query.unwrap()

    def add_filter(self, filter_obj: dict) -> None:
        self.query.add_filter(filter_obj=filter_obj)

    def add_filters(self, filter_objs: List[dict]) -> None:
        self.query.add_filters(filter_objs)

    def write_query(self) -> dict:
        return self.query.to_dict()

    def translate(self, concepts: dict, conquery_conn: ConqueryConnection, return_removed_ids: bool = False) -> \
            Union[Tuple[Union[QueryObject, None], Union[QueryObject, None], ConqueryIdCollection],
                  Tuple[Union[QueryObject, None], Union[QueryObject, None]]]:
        return translate_query(
            query=self.query,
            concepts=concepts,
            conquery_conn=conquery_conn,
            return_removed_ids=return_removed_ids
        )

    def execute_query(self, conquery_conn: ConqueryConnection, label: str = None) -> str:
        return conquery_conn.execute_query(self.query, label=label)

    def create_query(self, concept_id: Union[str, ConceptId, ChildId, List[str], List[ConceptId], List[ChildId], list],
                     concepts: dict,
                     concept_query: Optional[bool] = False,
                     connector_ids: Optional[Union[List[ConnectorId], List[str]]] = None,
                     concept_select_ids: Optional[Union[List[SelectId], List[str]]] = None,
                     connector_select_ids: Optional[Union[List[SelectId], List[str]]] = None,
                     filter_objs: Optional[List[dict]] = None,
                     exclude_from_secondary_id: Optional[bool] = None,
                     exclude_from_time_aggregation: Optional[bool] = None,
                     date_aggregation_mode: Optional[str] = None,
                     start_date: Optional[str] = None,
                     end_date: Optional[str] = None):
        self.query = create_query(concept_id=concept_id,
                                  concepts=concepts,
                                  concept_query=concept_query,
                                  connector_ids=connector_ids,
                                  concept_select_ids=concept_select_ids,
                                  connector_select_ids=connector_select_ids,
                                  filter_objs=filter_objs,
                                  exclude_from_secondary_id=exclude_from_secondary_id,
                                  exclude_from_time_aggregation=exclude_from_time_aggregation,
                                  date_aggregation_mode=date_aggregation_mode,
                                  start_date=start_date,
                                  end_date=end_date)

    def get_concept_elements(self) -> List[ConceptElement]:
        return self.query.get_concept_elements()

    def remove_all_tables_but(self, connector_id: ConnectorId) -> None:
        self.query.remove_all_tables_but(connector_ids=[connector_id])
