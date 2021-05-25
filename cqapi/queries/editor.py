from __future__ import annotations
from typing import Union, List, Tuple
from cqapi.queries.queries_oo import QueryObject, convert_from_query, SavedQuery, DateRestriction, ConceptQuery, \
    SecondaryIdQuery, Negation, AndOrElement, QueryDescription
from cqapi.api import ConqueryConnection
from cqapi.conquery_ids import change_dataset, get_dataset, get_root_concept_id, ConqueryIdCollection
from cqapi.namespace import Keys


class QueryEditor:
    """Helper to build and execute queries"""
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

    def write_query(self) -> dict:
        return self.query.write_query()

    def translate(self, concepts: dict, conquery_conn: ConqueryConnection, return_removed_ids: bool = False) -> \
            Union[Tuple[QueryObject, ConqueryIdCollection], QueryObject]:

        new_dataset = get_dataset(next(iter(concepts)))

        # get children ids that exist
        concept_ids = self.query.get_concept_ids()
        children_ids = check_concept_ids_in_concepts_for_new_dataset(concept_ids=concept_ids,
                                                                     new_dataset=new_dataset,
                                                                     conquery_conn=conquery_conn)
        # translate
        conquery_ids = ConqueryIdCollection()
        new_query = self.query.translate(concepts=concepts, removed_ids=conquery_ids, children_ids=children_ids)
        if return_removed_ids:
            return new_query, conquery_ids
        return new_query

    def execute_query(self, conquery_conn: ConqueryConnection, label: str = None) -> str:
        return conquery_conn.execute_query(self.query, label=label)


def check_concept_ids_in_concepts_for_new_dataset(concept_ids: List[str],
                                                  new_dataset: str, conquery_conn: ConqueryConnection):
    """
    For each concept_id in concept_ids it checks if the concept_id exist in the concept-object of the new dataset.
    This ist needed for translating children concepts that are on level 3 or higher
    :param concept_ids:
    :param new_dataset:
    :param conquery_conn:
    :return:
    """

    # group concept_ids by root_concept_id
    concept_ids_dict = dict()
    for concept_id in concept_ids:
        root_concept_id = get_root_concept_id(concept_id)
        concept_ids_dict[root_concept_id] = [concept_id, *concept_ids_dict.get(root_concept_id, [])]

    # for each root concept_id get the concept and check if concept_ids are in there
    children_ids = []
    for root_concept_id, child_concept_ids in concept_ids_dict.items():
        new_root_concept_id = change_dataset(new_dataset=new_dataset, conquery_id=root_concept_id)
        new_child_concept_ids = [change_dataset(new_dataset=new_dataset,
                                                conquery_id=child_concept_id)
                                 for child_concept_id in child_concept_ids]

        concept = conquery_conn.get_concept(new_root_concept_id)
        concept_ids_in_concept = [child_id for child in concept for child_id in child[Keys.ids]]

        children_ids.extend([child_concept_id for child_concept_id in new_child_concept_ids
                             if child_concept_id in concept_ids_in_concept])

    return children_ids
