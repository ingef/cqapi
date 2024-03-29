from __future__ import annotations
import pandas as pd
import datetime
from importlib.resources import open_text
from typing import Union, List, Tuple, Optional
import attr
from IPython.display import Markdown, Javascript

from cqapi.api import ConqueryConnection
from cqapi.namespace import Keys
from cqapi.conquery_ids import ConqueryIdCollection, contains_dataset_id, add_dataset_id_to_conquery_id, \
    get_concept_id_from_id_string, remove_dataset_id_from_conquery_id_string, SelectId, DateId
from cqapi.queries.base_elements import QueryObject, create_query_obj, SavedQuery, DateRestriction, ConceptQuery, \
    SecondaryIdQuery, Negation, AndElement, OrElement, create_query, QueryDescription
from cqapi.queries.form_elements import AbsoluteExportForm, RelativeExportForm
from cqapi.queries.translation import translate_query


def show_error(text: str):
    wrapper = f"Es ist Fehler aufgetreten: \n{text}"

    return Markdown(wrapper)


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


def convert_resolution(resolution: str):
    mapping = {
        "Gesamt": "COMPLETE",
        "Jahre": "YEARS",
        "Quartale": "QUARTERS",
        "Tage": "DAYS"
    }

    if resolution in mapping.values():
        return resolution
    if resolution.title() in mapping.keys():
        return mapping[resolution.title()]

    raise ValueError(f"Unknown {resolution=}. Must be in {mapping.keys()}")


@attr.s(kw_only=True, auto_attribs=True)
class Query:
    dataset: str
    query: QueryObject
    concepts: Optional[dict] = attr.ib(default=None, init=False)
    query_id: Optional[str] = attr.ib(default=None, init=False)

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

    def _add_dataset(self, conquery_id: str) -> str:
        if contains_dataset_id(conquery_id):
            return conquery_id

        return add_dataset_id_to_conquery_id(conquery_id=conquery_id, dataset_id=self.dataset)

    def show_json(self):
        self.query.print()

    def translate(self, concepts: dict, conquery_conn: ConqueryConnection, return_removed_ids: bool = False) -> \
            Union[Tuple[Union[QueryObject, None], Union[QueryObject, None], ConqueryIdCollection],
                  Tuple[Union[QueryObject, None], Union[QueryObject, None]]]:
        return translate_query(query=self.query,
                               concepts=concepts,
                               conquery_conn=conquery_conn,
                               return_removed_ids=return_removed_ids)

    def finalize(self) -> QueryObject:
        if not isinstance(self.query, QueryDescription):
            return ConceptQuery(root=self.query)

        return self.query


@attr.s(kw_only=True, auto_attribs=True)
class AbsoluteExportFormQuery(Query):
    query: AbsoluteExportForm


@attr.s(kw_only=True, auto_attribs=True)
class RelativeExportFormQuery(Query):
    query: RelativeExportForm


@attr.s(kw_only=True, auto_attribs=True)
class EditorQuery(Query):
    secondary_id: Optional[str] = attr.ib(default=None, init=False)

    def date_restriction(self, start_date: str, end_date: str,
                         label: str = None):
        """Restrict query with a start and end date.
        E.g. query.date_restriction(start_date="01.01.2020", end_date="31.12.2020)"""
        self.query = DateRestriction(child=self.query, start_date=convert_date(start_date),
                                     end_date=convert_date(end_date), label=label)

    def negate(self, label: str = None):
        """Negate the query to get all people that do not fulfill this characteristic"""
        self.query = Negation(child=self.query, label=label)

    def exclude_from_secondary_id(self) -> None:
        self.query and self.query.exclude_from_secondary_id()

    def exclude_from_time_aggregation(self) -> None:
        self.query and self.query.exclude_from_time_aggregation()

    def add_select(self, *select_ids: str):
        """Add any select to the query. All query components that can not have this select are ignored"""
        from cqapi.conquery_ids import SelectId
        for select_id in select_ids:
            select_id = self._add_dataset(select_id)
            select_id_obj: SelectId = SelectId.from_str(select_id)

            if select_id_obj.is_concept_select():
                self.query and self.query.add_concept_select(select_id_obj)
            else:
                self.query and self.query.add_connector_select(select_id_obj)

    def remove_selects(self, *select_ids: str):
        """Removes all selects given as input parameters. When none is given, all selects are removed.
        Examples of select Ids: "icd.exists", "icd.arzt_diagnose_icd_code.anzahl_arztfaelle"""
        select_ids_list = [SelectId.from_str(select_id) for select_id in select_ids]
        if select_ids_list:
            self.query and self.query.remove_concept_selects(concept_select_ids=select_ids_list)
            self.query and self.query.remove_connector_selects(connector_select_ids=select_ids_list)
        else:
            self.query and self.query.remove_all_selects()

    def set_validity_date(self, validity_date_id: str) -> None:
        self.query and self.query.set_validity_date(validity_date_id=DateId.from_str(validity_date_id))

    def add_filter(self, *filter_objects: dict) -> None:
        self.query and self.query.add_filters(filter_objs=list(filter_objects))

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

    def finalize(self) -> QueryObject:
        if not isinstance(self.query, QueryDescription):
            if self.secondary_id:
                return SecondaryIdQuery(root=self.query, secondary_id=self.secondary_id)
            else:
                return ConceptQuery(root=self.query)
        return self.query


class Concepts:
    last_concept: Optional[dict] = None

    def __init__(self, concepts: dict):
        self.struc_elements = {key: value for key, value in concepts.items() if not value.get("active")}
        self.concepts = {key: value for key, value in concepts.items() if value.get("active")}
        self.dataset = next(iter(concepts)).split(".")[0] if concepts else ""

    def get_default_connectors(self, concept_id: str) -> List[str]:
        concept_id = get_concept_id_from_id_string(concept_id)

        return [table[Keys.connector_id]
                for table in self.concepts[concept_id][Keys.tables]
                if table.get(Keys.default, False)]

    def get_default_concept_selects(self, concept_id: str) -> List[str]:
        concept_id = get_concept_id_from_id_string(concept_id)

        return [select[Keys.id]
                for select in self.concepts[concept_id][Keys.selects]
                if select.get(Keys.default, False)]

    def get_default_connector_selects(self, concept_id: str, connector_ids: List[str] = None) -> List[str]:
        concept_id = get_concept_id_from_id_string(concept_id)

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

            concept_ids.extend(remove_dataset_id_from_conquery_id_string(concept_id)
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
        concept: dict = self.concepts[self._add_dataset_id(get_concept_id_from_id_string(connector_id))]

        try:
            connector_obj = next(connector_obj
                                 for connector_obj in concept[Keys.tables]
                                 if connector_obj[Keys.connector_id] == connector_id)
        except StopIteration:
            raise ValueError(f"Could not find any connector "
                             f"with id {remove_dataset_id_from_conquery_id_string(connector_id)}")

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

        ids = [remove_dataset_id_from_conquery_id_string(conquery_id) for conquery_id in ids]

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

        ids = [remove_dataset_id_from_conquery_id_string(conquery_id) for conquery_id in ids]

        return Markdown(pd.DataFrame({
            "Typ": types,
            "Objekt": labels,
            "EVA-ID": ids
        }).to_markdown(index=False))

    def search_concept(self, concept_id: str, value: str, conn: ConqueryConnection):
        concept_id = self._add_dataset_id(concept_id)

        # do not request again if same concept
        if not self.last_concept or concept_id != next(iter(self.last_concept)):
            self.last_concept = conn.get_concept(concept_id, return_raw_format=True)  # type: ignore

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
                            remove_dataset_id_from_conquery_id_string(concept_id),
                            concept[Keys.description]))

        for child_concept_id in concept[Keys.children]:
            self.search_concept_recursively(concept_id=child_concept_id,
                                            concept=self.last_concept[child_concept_id],  # type: ignore
                                            value=value, matches=matches)


class Conquery:
    conn: ConqueryConnection = ConqueryConnection(url="dummy_connection")
    concepts: Concepts = Concepts(concepts=dict())  # this is only a dummy and has to be overridden
    executed_query_id: Optional[str] = None

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
            self.concepts = Concepts(concepts=self.conn.get_concepts(remove_structure_elements=False))

    def change_dataset(self, new_dataset: str):
        self.conn.change_dataset(dataset=new_dataset)

    def _add_dataset(self, conquery_id: str) -> str:
        if contains_dataset_id(conquery_id):
            return conquery_id

        return add_dataset_id_to_conquery_id(conquery_id=conquery_id, dataset_id=self.conn.get_dataset())

    def from_existing_query(self, label: str, get_original: bool = False):
        self._check_conn_and_concepts()

        query_id = self.conn.get_query_id(label=label)
        if get_original:
            query = create_query_obj(self.conn.get_query(query_id=query_id))
        else:
            query = SavedQuery(query_id=query_id)

        query_wrapper = Query(query=query, dataset=self.conn.get_dataset())
        query_wrapper.query = query

        return query_wrapper

    def new_query(self, concept_id: str,
                  connector_ids: List[str] = None,
                  select_ids: List[str] = None,
                  start_date: str = None, end_date: str = None) -> EditorQuery:
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

        query = create_query(concept_id=self._add_dataset(concept_id),
                             concepts=self.concepts.concepts,
                             connector_ids=[self._add_dataset(con_id) for con_id in connector_ids],
                             start_date=convert_date(start_date),
                             end_date=convert_date(end_date))

        query_wrapper = EditorQuery(query=query, dataset=self.conn.get_dataset())
        if select_ids:
            for select_id in select_ids:
                query_wrapper.add_select(self._add_dataset(select_id))

        return query_wrapper

    def new_absolute_export_query(self, editor_query: Union[EditorQuery, str],
                                  start_date: str, end_date: str,
                                  resolution: str = "Gesamt",
                                  features: Optional[List[str]] = None):
        if isinstance(editor_query, EditorQuery):
            editor_query_id = self.conn.execute_query(query=editor_query.finalize())
        else:
            editor_query_id = editor_query

        query = AbsoluteExportForm(query_id=editor_query_id, features=features or [],
                                   resolutions=[convert_resolution(resolution)],
                                   start_date=convert_date(start_date), end_date=convert_date(end_date))

        return AbsoluteExportFormQuery(query=query, dataset=self.conn.get_dataset())

    def add_feature_to_absolute_export_form_query(self, query: AbsoluteExportFormQuery,
                                                  concept_id: str, connector_id: str = None,
                                                  select_ids: Union[List[str]] = None):

        concept_id = self._add_dataset(concept_id)
        if not connector_id:
            connector_ids = self.concepts.get_default_connectors(concept_id=concept_id)
        else:
            connector_ids = [self._add_dataset(connector_id)]

        if not select_ids:
            select_ids = self.concepts.get_default_concept_selects(concept_id=concept_id)
            select_ids.extend(self.concepts.get_default_connector_selects(concept_id=concept_id,
                                                                          connector_ids=connector_ids))

        else:
            select_ids = [self._add_dataset(select_id) for select_id in select_ids]

        select_id_objs = [SelectId.from_str(select_id) for select_id in select_ids]
        concept_select_ids = [select_id for select_id, select_id_obj in zip(select_ids, select_id_objs)
                              if select_id_obj.is_concept_select()]
        connector_select_ids = [select_id for select_id, select_id_obj in zip(select_ids, select_id_objs)
                                if not select_id_obj.is_concept_select()]
        concept = create_query(concept_id=concept_id, connector_ids=connector_ids,
                               concept_select_ids=concept_select_ids, connector_select_ids=connector_select_ids,
                               concepts=self.concepts.concepts)

        query.query.features.append(concept)

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
        return self.concepts.search_concept(concept_id=concept_id, value=value, conn=self.conn)

    def execute(self, query: Query, label: str = None):
        self.executed_query_id = self.conn.execute_query(query=query.finalize(), label=label)

        return Markdown("Anfrage wurde ausgeführt. Mit der Methode `download()` "
                        "kann das Anfrageergebnis heruntergeladen werden.")

    def check_execution(self, query_id: str = None):
        query_id_to_check = query_id if query_id is not None else self.executed_query_id

        if query_id_to_check is None:
            return Markdown(f"Es wurde keine Anfrage ausgeführt.")

        query_status = self.conn.get_query_info(query_id_to_check)["status"]  # type: ignore

        if query_status == "RUNNING":
            return Markdown("Anfrage ist noch nicht fertig.")

        if query_status in ["NEW", "FAILED"]:
            return Markdown("Die Anfrage ist fehlgeschlagen.")

        if query_status == "DONE":
            return Markdown("Die Anfrage ist fertig und das Ergebnis kann heruntergeladen werden.")

        return show_error(f"Anfrage hat unbekannten Status: {query_status}.")

    def download(self, query_id: str = None, label: str = None, use_pandas: bool = True, use_arrow: bool = True,
                 preprocess_money_columns: bool = True):

        query_id_for_download = query_id if query_id is not None else self.executed_query_id

        if label is not None:
            query_id_for_download = self.conn.get_query_id(label=label)

        if query_id_for_download is None:
            return show_error("Es wurde weder eine Anfrage ausgeführt, noch eine query_id übergeben.")

        data: pd.DataFrame = self.conn.get_query_result(query_id=query_id_for_download,
                                                        return_pandas=use_pandas,
                                                        file_format="arrow")

        # preprocess money columns
        if preprocess_money_columns:
            column_descriptions = self.conn.get_column_descriptions(query_id=query_id_for_download)
            money_columns = [col[Keys.label] for col in column_descriptions if col[Keys.type] == Keys.money_type]
            for money_column in money_columns:
                data[money_column] = data[money_column] / 100

        return data

    def get_data(self, query: Query, label: str = None):
        self.execute(query=query, label=label)
        return self.download()
