from __future__ import annotations
from typing import Union, List, Tuple, Optional
from cqapi.queries.base_elements import QueryObject, create_query_obj, SavedQuery, DateRestriction, ConceptQuery, \
    SecondaryIdQuery, Negation, AndElement, OrElement, QueryDescription, ConceptElement, create_query
from cqapi.queries.translation import translate_query
from cqapi.api import ConqueryConnection
from cqapi.conquery_ids import ConqueryIdCollection, contains_dataset_id, \
    is_concept_select, add_dataset_id_to_conquery_id
import datetime


def convert_date(date: Optional[str]) -> Optional[str]:
    if date is None:
        return None

    try:
        datetime.datetime.strptime(date, '%Y-%m-%d')
        return date
    except ValueError:
        try:
            date_obj = datetime.datetime.strptime(date, '%d.%m.%Y')
            return date_obj.strftime('%Y-%m-%d')
        except ValueError:
            raise ValueError(f"date string must either be of format %Y-%m-%d (e.g. 2020-05-30) "
                             f"or %d.%m.%Y (e.g. 30.05.2020)")


class Query:

    def __init__(self, eva: ConqueryConnection):
        self.query: Optional[QueryObject] = None
        self.conn = eva
        self.concepts: Optional[dict] = None
        self.secondary_id: Optional[str] = None
        self.query_id: Optional[str] = None

    def date_restriction(self, start_date: str, end_date: str,
                         label: str = None):
        """Restrict query with a start and end date.
        E.g. query.date_restriction(start_date="01.01.2020", end_date="31.12.2020)"""
        self.query = DateRestriction(child=self.query, start_date=convert_date(start_date),
                                     end_date=convert_date(end_date), label=label)

    def negate(self, label: str = None):
        """Negate the query to get all people that do not fulfill this characteristic"""
        self.query = Negation(child=self.query, label=label)

    def from_existing_query(self, label: str):
        """Load an already existing query via label"""
        query_id = self.conn.get_query_id(label=label)
        self.query = SavedQuery(query_id=query_id)

    @staticmethod
    def _to_query(value: Union[dict, QueryObject, Query, str]) -> QueryObject:
        if isinstance(value, str):
            return SavedQuery(query_id=value)
        if isinstance(value, dict):
            return create_query_obj(value)
        if isinstance(value, Query):
            return value.query
        if isinstance(value, QueryObject):
            return value

        raise ValueError(f"Query must be of type Union[dict, str, QueryObject, Editor], not {type(value)}")

    def exclude_from_secondary_id(self) -> None:
        self.query and self.query.exclude_from_secondary_id()

    def exclude_from_time_aggregation(self) -> None:
        self.query and self.query.exclude_from_time_aggregation()

    def _add_dataset(self, conquery_id: str) -> str:
        if contains_dataset_id(conquery_id):
            return conquery_id

        return add_dataset_id_to_conquery_id(conquery_id=conquery_id, dataset_id=self.conn.get_dataset())

    def add_select(self, *select_ids: str):
        """Add any select to the query. All query components that can not have this select are ignored"""
        for select_id in select_ids:
            select_id = self._add_dataset(select_id)
            if is_concept_select(select_id):
                self.query and self.query.add_concept_select(select_id)
            else:
                self.query and self.query.add_connector_select(select_id)

    def remove_selects(self, *select_ids: str):
        """Removes all selects given as input parameters. When none is given, all selects are removed.
        Examples of select Ids: "icd.exists", "icd.arzt_diagnose_icd_code.anzahl_arztfaelle"""
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
        return translate_query(query=self.query,
                               concepts=concepts,
                               conquery_conn=conquery_conn,
                               return_removed_ids=return_removed_ids)

    def clear(self):
        """Clears Query"""
        self.query = None

    def join_and(self, *queries_to_join: Union[dict, QueryObject, Query, str]):
        """current query AND all given objects. Given objects can be query_ids, queries as dicts or Query
        E.g.
        query = Query(eva=eva).new(concept_id="alter")
        query.join_and(
        """
        queries = [self._to_query(query) for query in queries_to_join]
        self.query = AndElement(children=[self.query, *queries])

    def join_or(self, *queries_to_join: Union[dict, QueryObject, Query, str]):
        """current query AND all given objects. Given objects can be query_ids, queries as dicts or Query"""
        queries = [self._to_query(query) for query in queries_to_join]
        self.query = OrElement(children=[self.query, *queries])

    def set_secondary_id(self, secondary_id: str):
        self.secondary_id = secondary_id

    def execute(self, label: str = None) -> str:
        if not self.query:
            raise ValueError(f"Can not execute empty query")

        if self.secondary_id:
            query_to_execute = SecondaryIdQuery(root=self.query, secondary_id=self.secondary_id)
        else:
            query_to_execute = ConceptQuery(root=self.query)

        self.query_id = self.conn.execute_query(query=query_to_execute, label=label)

        return "Query was executed. Use the download-Method to download the result"

    def download(self, query_id: str = None, use_pandas: bool = True, use_arrow: bool = True):
        query_id_for_download = query_id if query_id is not None else self.query_id

        if query_id_for_download is None:
            raise ValueError(f"Nothing executed and no query_id given. Can not download any results")

        self.conn.get_query_result(query_id=query_id_for_download,
                                   return_pandas=use_pandas,
                                   download_with_arrow=use_arrow)


class Editor:
    def __init__(self, eva: ConqueryConnection):
        self.conn = eva
        self.concepts = eva.get_concepts()

    def _add_dataset(self, conquery_id: str) -> str:
        if contains_dataset_id(conquery_id):
            return conquery_id

        return add_dataset_id_to_conquery_id(conquery_id=conquery_id, dataset_id=self.conn.get_dataset())

    def new_query(self, concept_id: str,
                  connector_ids: List[str] = None,
                  select_ids: List[str] = None,
                  start_date: str = None, end_date: str = None) -> Query:
        """
        Create query from a eva ids.
        :param concept_id: E.g. "alter", "geschlecht", "icd", "atc"
        :param connector_ids: Optional: E.g. "alter.alter", "geschlecht.geschlecht", "icd.arzt_diagnose_icd_code"
        :param select_ids: Optional: E.g. "alter.alter.alter_select", "geschlecht.geschlecht.geschlecht_text",
                           icd.arzt_diagnose_icd_code.anzahl_arztfaelle"
        :param start_date: Optional: E.g. "01.01.2020"
        :param end_date: Optional: E.g. "31.12.2020"
        """

        connector_ids = connector_ids or []

        query = Query(eva=self.conn)

        query.query = create_query(concept_id=self._add_dataset(concept_id),
                                   concepts=self.concepts,
                                   connector_ids=[self._add_dataset(con_id) for con_id in connector_ids],
                                   start_date=convert_date(start_date),
                                   end_date=convert_date(end_date))

        if select_ids:
            for select_id in select_ids:
                query.add_select(self._add_dataset(select_id))

        return query
