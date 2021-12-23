from __future__ import annotations
import pandas as pd
import datetime
from importlib.resources import open_text
from typing import Union, List, Tuple, Optional

from IPython.display import Markdown, Javascript

from cqapi.api import ConqueryConnection
from cqapi.namespace import Keys
from cqapi.conquery_ids import ConqueryIdCollection, contains_dataset_id, \
    is_concept_select, add_dataset_id_to_conquery_id, remove_dataset_id_from_conquery_id, get_root_concept_id
from cqapi.queries.base_elements import QueryObject, create_query_obj, SavedQuery, DateRestriction, ConceptQuery, \
    SecondaryIdQuery, Negation, AndElement, OrElement, QueryDescription, ConceptElement, create_query
from cqapi.queries.translation import translate_query


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
        self.conn = eva
        self.query: Optional[QueryObject] = None

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
        self.query_id = self.conn.get_query_id(label=label)
        self.query = SavedQuery(query_id=self.query_id)

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

    def check_execution(self, query_id: str = None):
        query_id_to_check = query_id if query_id is not None else self.query_id

        if query_id_to_check is None:
            return Markdown(f"Es wurde keine Anfrage ausgeführt.")

        query_status = self.conn.get_query_info(query_id_to_check)["status"]  # type: ignore

        if query_status == "RUNNING":
            return Markdown("Anfrage ist noch nicht fertig.")

        if query_status in ["NEW", "FAILED"]:
            return Markdown("Die Anfrage ist fehlgeschlagen.")

        if query_status == "DONE":
            return Markdown("Die Anfrage ist fertig und das Ergebnis kann heruntergeladen werden.")

        raise ValueError(f"Unknown {query_status=}")

    def download(self, query_id: str = None, use_pandas: bool = True, use_arrow: bool = True,
                 preprocess_money_columns: bool = True):

        query_id_for_download = query_id if query_id is not None else self.query_id

        if query_id_for_download is None:
            raise ValueError(f"Nothing executed and no query_id given. Can not download any results")

        data: pd.DataFrame = self.conn.get_query_result(query_id=query_id_for_download,
                                                        return_pandas=use_pandas,
                                                        download_with_arrow=use_arrow)

        # preprocess money columns
        if preprocess_money_columns:
            column_descriptions = self.conn.get_column_descriptions(query_id=query_id_for_download)
            money_columns = [col[Keys.label] for col in column_descriptions if col[Keys.type] == Keys.money_type]
            for money_column in money_columns:
                data[money_column] = data[money_column] / 100

        return data

    def get_data(self):
        self.execute()
        return self.download()


class Concepts:
    last_concept: Optional[dict] = None

    def __init__(self, concepts: dict, conn: ConqueryConnection):
        self.struc_elements = {key: value for key, value in concepts.items() if not value.get("active")}
        self.concepts = {key: value for key, value in concepts.items() if value.get("active")}
        self.dataset = next(iter(concepts)).split(".")[0] if concepts else ""
        self.conn = conn

    def get_default_connectors(self, concept_id: str) -> List[str]:
        concept_id = get_root_concept_id(concept_id)

        return [table[Keys.connector_id]
                for table in self.concepts[concept_id][Keys.tables]
                if table.get(Keys.default, False)]

    def get_default_concept_selects(self, concept_id: str) -> List[str]:
        concept_id = get_root_concept_id(concept_id)

        return [select[Keys.id]
                for select in self.concepts[concept_id][Keys.selects]
                if select.get(Keys.default, False)]

    def get_default_connector_selects(self, concept_id: str, connector_ids: List[str] = None) -> List[str]:
        concept_id = get_root_concept_id(concept_id)

        if not connector_ids:
            connector_ids = self.get_default_connectors(concept_id=concept_id)

        return [table_select[Keys.id]
                for table in self.concepts[concept_id][Keys.tables]
                for table_select in table[Keys.selects]
                if table_select.get(Keys.default, False) and table[Keys.connector_id] in connector_ids]

    def is_empty(self):
        return not bool(self.concepts)

    def show_concepts(self):
        concept_labels: List[str] = []
        concept_ids: List[str] = []

        for struc_element in self.struc_elements.values():
            concept_labels.append(struc_element.get("label", ""))
            concept_ids.append("")

            concept_ids.extend(remove_dataset_id_from_conquery_id(concept_id)
                               for concept_id in struc_element["children"])
            concept_labels.extend([self.concepts[concept_id].get("label", "")
                                   for concept_id in struc_element["children"]])

            concept_ids.append("")
            concept_labels.append("")

        return Markdown(pd.DataFrame({"Konzept": concept_labels,
                                      "Konzept-ID": concept_ids}).to_markdown(index=False))

    def _add_dataset_id(self, conquery_id: str):
        if conquery_id.startswith(self.dataset):
            return conquery_id

        return f"{self.dataset}.{conquery_id}"

    def show_connector(self, connector_id: str, show_filters: bool = False, show_selects: bool = True,
                       show_date_columns: bool = True, return_df: bool = True):

        connector_id = self._add_dataset_id(connector_id)
        concept: dict = self.concepts[self._add_dataset_id(get_root_concept_id(connector_id))]

        try:
            connector_obj = next(connector_obj
                                 for connector_obj in concept[Keys.tables]
                                 if connector_obj[Keys.connector_id] == connector_id)
        except StopIteration:
            raise ValueError(f"Could not find any connector with id {remove_dataset_id_from_conquery_id(connector_id)}")

        types: List[str] = []
        labels: List[str] = []
        ids: List[str] = []

        types.append("Quelle (connector_id)")
        labels.append(connector_obj.get("label", ""))
        ids.append(connector_id)

        if connector_obj[Keys.date_column] and show_date_columns:
            for validity_date_obj in connector_obj[Keys.date_column].get(Keys.options, []):
                types.append("Relevanter Zeitraum (date_column_id)")
                labels.append(validity_date_obj[Keys.label])
                ids.append(validity_date_obj[Keys.value])

        if show_selects:
            for select_obj in connector_obj[Keys.selects]:
                types.append("Ausgabewert (select_id)")
                labels.append(select_obj[Keys.label])
                ids.append(select_obj[Keys.id])

        if show_filters:
            for filter_obj in connector_obj[Keys.filters]:
                types.append("Filter (filter_obj)")
                labels.append(filter_obj[Keys.label])
                ids.append(filter_obj[Keys.id])

        ids = [remove_dataset_id_from_conquery_id(conquery_id) for conquery_id in ids]

        if return_df:
            return Markdown(pd.DataFrame({
                "Typ": types,
                "Objekt": labels,
                "EVA-ID": ids
            }).to_markdown(index=False))

        return types, labels, ids

    def show_concept(self, concept_id: str, show_all: bool = True, show_filters: bool = False):

        concept_id = self._add_dataset_id(concept_id)
        concept: dict = self.concepts[concept_id]

        types: List[str] = []
        labels: List[str] = []
        ids: List[str] = []

        types.append("Konzept (concept_id)")
        labels.append(concept["label"])
        ids.append(concept_id)

        # concept selects
        types.append("")
        labels.append("")
        ids.append("")
        for select_obj in concept[Keys.selects]:
            types.append("Ausgabewert (select_id)")
            labels.append(select_obj[Keys.label])
            ids.append(select_obj[Keys.id])

        for connector_obj in concept[Keys.tables]:
            connector_types, connector_labels, connector_ids = \
                self.show_connector(connector_id=connector_obj[Keys.connector_id],
                                    show_selects=show_all, show_date_columns=show_all,
                                    show_filters=show_filters, return_df=False)
            types.extend(["", *connector_types])
            labels.extend(["", *connector_labels])
            ids.extend(["", *connector_ids])

        ids = [remove_dataset_id_from_conquery_id(conquery_id) for conquery_id in ids]

        return Markdown(pd.DataFrame({
            "Typ": types,
            "Objekt": labels,
            "EVA-ID": ids
        }).to_markdown(index=False))

    def search_concept(self, concept_id: str, value: str):
        concept_id = self._add_dataset_id(concept_id)

        # do not request again if same concept
        if not self.last_concept or concept_id != next(iter(self.last_concept)):
            self.last_concept = self.conn.get_concept(concept_id, return_raw_format=True)  # type: ignore

        matches: List[Tuple[str, str, str]] = []
        self.search_concept_recursively(concept_id=concept_id,
                                        concept=next(iter(self.last_concept.values())),  # type: ignore
                                        value=value.lower(), matches=matches)

        if not matches:
            return Markdown("Es konnten keine Konzepte gefunden werden.")

        labels, ids, descriptions = list(map(list, zip(*matches)))

        return Markdown(pd.DataFrame(
            {"Konzept": labels,
             "Konzept-ID": ids,
             "Beschreibung": descriptions}).to_markdown(index=False))

    def search_concept_recursively(self, concept_id: str, concept: dict, value: str,
                                   matches: List[Tuple[str, str, str]]):

        if value in concept[Keys.label].lower() or \
                (concept.get(Keys.description) and value in concept[Keys.description].lower()):
            matches.append((concept[Keys.label],
                            remove_dataset_id_from_conquery_id(concept_id),
                            concept[Keys.description]))

        for child_concept_id in concept[Keys.children]:
            self.search_concept_recursively(concept_id=child_concept_id,
                                            concept=self.last_concept[child_concept_id],  # type: ignore
                                            value=value, matches=matches)


class Editor:
    conn: ConqueryConnection = ConqueryConnection(url="dummy_connection")
    concepts: Concepts = Concepts(concepts=dict(), conn=conn)  # this is only a dummy and has to be overridden

    def login(self,
              url: str = "localhost:8000",
              token: Optional[str] = None,
              dataset: Optional[str] = None,
              auth_url: str = "localhost:8000/auth",
              client_id: str = "conquery-prod",
              token_refresh_rate: int = 300):
        """
        When _user_login is set, this function returns JavaScript-Code that will be executed in the output cell when
        code is run in a jupyter notebook. The Code will initialize a KeyCloak-Object that connects
        to the conquery-auth server. After successful login the token of Editor.conn for all Editor in the
        Notebook-Scope are updated and a second JavaScript-Process will refresh that token in self._token_refresh_rate
        seconds.
        """

        self.conn = ConqueryConnection(url=url, dataset=dataset)

        if token:
            self.conn = ConqueryConnection(url=url, token=token, dataset=dataset)
            return None

        placeholder_dict = {
            "auth_url_placeholder": auth_url,
            "client_id_placeholder": client_id,
            "refresh_rate_placeholder": str(token_refresh_rate * 1000)
        }

        run_keycloak_script: str = open_text("cqapi.auth", "run_keycloak.js").read()

        for ph_name, ph_value in placeholder_dict.items():
            run_keycloak_script = run_keycloak_script.replace(ph_name, ph_value)

        return Javascript(run_keycloak_script)

    def _check_conn_and_concepts(self):
        if not self.conn:
            raise ValueError(f"No connection established, please log in first")
        if self.concepts.is_empty():
            self.concepts = Concepts(concepts=self.conn.get_concepts(remove_structure_elements=False),
                                     conn=self.conn)

    def change_dataset(self, new_dataset: str):
        self.conn.change_dataset(dataset=new_dataset)

    def _add_dataset(self, conquery_id: str) -> str:
        if contains_dataset_id(conquery_id):
            return conquery_id

        return add_dataset_id_to_conquery_id(conquery_id=conquery_id, dataset_id=self.conn.get_dataset())

    def from_existing_query(self, label: str):
        self._check_conn_and_concepts()

        query = Query(eva=self.conn)
        query.from_existing_query(label=label)

        return query

    def new_query(self, concept_id: str,
                  connector_ids: List[str] = None,
                  select_ids: List[str] = None,
                  start_date: str = None, end_date: str = None) -> Query:
        """
        Create query from a eva ids.
        :param concept_id: E.g. "alter", "geschlecht", "icd", "atc"
        :param connector_ids: Optional: E.g. "alter.alter", "geschlecht.geschlecht", "icd.arzt_diagnose_icd_code".
                              When none is given, defaults are selected
        :param select_ids: Optional: E.g. "alter.alter.alter_select", "geschlecht.geschlecht.geschlecht_text",
                           icd.arzt_diagnose_icd_code.anzahl_arztfaelle"
                           When none is given, defaults are selected
        :param start_date: Optional: E.g. "01.01.2020"
        :param end_date: Optional: E.g. "31.12.2020"
        """

        self._check_conn_and_concepts()

        concept_id = self._add_dataset(concept_id)

        # get default values from concepts
        if not connector_ids:
            connector_ids = self.concepts.get_default_connectors(concept_id=concept_id)

        if not select_ids:
            select_ids = self.concepts.get_default_concept_selects(concept_id=concept_id)
            select_ids.extend(self.concepts.get_default_connector_selects(concept_id=concept_id,
                                                                          connector_ids=connector_ids))

        query = Query(eva=self.conn)

        query.query = create_query(concept_id=self._add_dataset(concept_id),
                                   concepts=self.concepts.concepts,
                                   connector_ids=[self._add_dataset(con_id) for con_id in connector_ids],
                                   start_date=convert_date(start_date),
                                   end_date=convert_date(end_date))

        if select_ids:
            for select_id in select_ids:
                query.add_select(self._add_dataset(select_id))

        return query

    def show_concepts(self):
        self._check_conn_and_concepts()
        return self.concepts.show_concepts()

    def show_connector(self, connector_id: str, show_selects: bool = True,
                       show_filters: bool = True, show_date_column: bool = True):
        self._check_conn_and_concepts()
        return self.concepts.show_connector(connector_id=connector_id, show_selects=show_selects,
                                            show_date_columns=show_date_column, show_filters=show_filters)

    def show_concept(self, concept_id: str, show_all: bool = True, show_filters: bool = False):
        self._check_conn_and_concepts()
        return self.concepts.show_concept(concept_id=concept_id, show_all=show_all, show_filters=show_filters)

    def search_concept(self, concept_id: str, value: str):
        self._check_conn_and_concepts()
        return self.concepts.search_concept(concept_id=concept_id, value=value)
