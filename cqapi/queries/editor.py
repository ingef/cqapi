from __future__ import annotations
from typing import Union, List, Tuple
from cqapi.queries.elements import QueryObject, convert_from_query, SavedQuery, DateRestriction, ConceptQuery, \
    SecondaryIdQuery, Negation, AndOrElement, QueryDescription, ConceptElement
from cqapi.queries.utils import create_query, translate_query
from cqapi.api import ConqueryConnection
from cqapi.conquery_ids import ConqueryIdCollection


class QueryEditor:
    """Helper to build and execute queries"""
    query: QueryObject = None

    def __init__(self, query: Union[QueryObject, dict, str] = None):
        if isinstance(query, dict):
            query = convert_from_query(query)
        if isinstance(query, str):
            query = SavedQuery(query_id=query)
        if query is not None and not isinstance(query, QueryObject):
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

    def write_query(self) -> dict:
        return self.query.write_query()

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

    def create_query(self, concept_id: str, concepts: dict, concept_query: bool = False,
                     connector_ids: List[str] = None,
                     concept_select_ids: List[str] = None, connector_select_ids: List[str] = None,
                     filter_objs: List[dict] = None,
                     exclude_from_secondary_id: bool = None, exclude_from_time_aggregation: bool = None,
                     date_aggregation_mode: str = None,
                     start_date: str = None, end_date: str = None):
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
