from __future__ import annotations
from typing import Union, List, Tuple, Optional
from cqapi.queries.base_elements import QueryObject, create_query_obj, SavedQuery, DateRestriction, ConceptQuery, \
    SecondaryIdQuery, Negation, AndElement, OrElement, QueryDescription, ConceptElement, create_query
from cqapi.queries.translation import translate_query
from cqapi.api import ConqueryConnection
from cqapi.conquery_ids import ConqueryIdCollection, contains_dataset_id, \
    is_concept_select, add_dataset_id_to_conquery_id


class Editor:
    """Helper to build and execute queries"""

    def __init__(self, eva: ConqueryConnection):
        self.conn = eva
        self.query: Optional[QueryObject] = None
        self.secondary_id: Optional[str] = None
        self.query_id: Optional[str] = None

    def date_restriction(self, date_range: Union[List[str], dict] = None, start_date: str = None, end_date: str = None,
                         label: str = None):
        self.query = DateRestriction(child=self.query, date_range=date_range, start_date=start_date, end_date=end_date,
                                     label=label)

    def negate(self, label: str = None):
        self.query = Negation(child=self.query, label=label)

    @staticmethod
    def _to_query(value: Union[dict, QueryObject, Editor, str]) -> QueryObject:
        if isinstance(value, str):
            return SavedQuery(query_id=value)
        if isinstance(value, dict):
            return create_query_obj(value)
        if isinstance(value, Editor):
            return value.query
        if isinstance(value, QueryObject):
            return value

        raise ValueError(f"Query must be of type Union[dict, str, QueryObject, Editor], not {type(value)}")

    def join_and(self, *queries_to_join):
        queries = [self._to_query(query) for query in queries_to_join]
        self.query = AndElement(children=[self.query, *queries])

    def join_or(self, *queries_to_join):
        queries = [self._to_query(query) for query in queries_to_join]
        self.query = OrElement(children=[self.query, *queries])

    def exclude_from_secondary_id(self) -> None:
        self.query and self.query.exclude_from_secondary_id()

    def exclude_from_time_aggregation(self) -> None:
        self.query and self.query.exclude_from_time_aggregation()

    def _add_dataset(self, conquery_id: str) -> str:
        if contains_dataset_id(conquery_id):
            return conquery_id

        return add_dataset_id_to_conquery_id(conquery_id=conquery_id, dataset_id=self.conn.get_daraset())

    def add_select(self, *select_ids: str):
        for select_id in select_ids:
            select_id = self._add_dataset(select_id)
            if is_concept_select(select_id):
                self.query and self.query.add_concept_select(select_id)
            else:
                self.query and self.query.add_connector_select(select_id)

    def remove_selects(self, *select_ids: str):
        select_ids_list = list(select_ids)
        if select_ids_list:
            self.query and self.query.remove_concept_selects(concept_select_ids=select_ids_list)
            self.query and self.query.remove_connector_selects(connector_select_ids=select_ids_list)
        else:
            self.query and self.query.remove_all_selects()

    def set_validity_date(self, validity_date_id: str) -> None:
        self.query and self.query.set_validity_date(validity_date_id=validity_date_id)

    def add_filter(self, *filter_objects: dict) -> None:
        self.query and self.query.add_filters(filter_objs=list(filter_objects))

    def show_json(self):
        self.query and self.query.print()

    def translate(self, concepts: dict, conquery_conn: ConqueryConnection, return_removed_ids: bool = False) -> \
            Union[Tuple[Union[QueryObject, None], Union[QueryObject, None], ConqueryIdCollection],
                  Tuple[Union[QueryObject, None], Union[QueryObject, None]]]:
        return translate_query(
            query=self.query,
            concepts=concepts,
            conquery_conn=conquery_conn,
            return_removed_ids=return_removed_ids
        )

    def execute(self, label: str = None) -> str:
        return self.conn.execute_query(query=self.query, label=label)

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
