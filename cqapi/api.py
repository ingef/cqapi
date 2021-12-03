from __future__ import annotations
import csv
from io import StringIO
from time import sleep
import requests
import cqapi.datasets
from cqapi.conquery_ids import get_dataset as get_dataset_from_id
from cqapi.exceptions import QueryNotFoundError
from cqapi.queries.utils import get_dataset_from_query
from cqapi.queries.base_elements import QueryObject
from typing import Union, List, Dict, NoReturn
from requests import Response
from requests.exceptions import HTTPError
from importlib.resources import open_text


def raise_for_status(response: Response) -> Union[None, NoReturn]:
    if response.status_code < 400:
        return None

    if response.status_code < 500:
        http_error_msg = f'{response.status_code} Client Error: {response.reason} for url: {response.url}\n' \
                         f'Response: {response.text}'

    else:
        http_error_msg = f'{response.status_code} Server Error: {response.reason} for url: {response.url}\n' \
                         f'Response: {response.text}'

    raise HTTPError(http_error_msg, response=response)


class ConqueryConnectionSession:
    """Session object to communicate with conquery.
    This could be a bundle of static methods if it wasn't for the token update in the header"""

    def __init__(self, token: str):
        self.token: str = token
        self._session = requests.session()
        self._session.headers.update(self.header)

    def open(self):
        self.close()
        self._session = requests.session()

    def close(self):
        self._session.close()

    @property
    def header(self):
        return {'Authorization': f'Bearer {self.token}',
                'Accept-Language': 'de-DE,de;q=0.9',
                'pretty': 'false'}

    def update_token(self, new_token: str):
        self.token = new_token
        self._session.headers.update(self.header)

    # Basic Request-Methods
    def get(self, url, params: dict = None):
        with self._session.get(url, params=params) as response:
            raise_for_status(response)
            return response

    def post(self, url, data):
        with self._session.post(url=url, json=data) as response:
            raise_for_status(response)
            return response.json()

    def patch(self, url, data):
        with self._session.patch(url, json=data) as response:
            raise_for_status(response)
            return response.json()

    def delete(self, url):
        with self._session.delete(url) as response:
            raise_for_status(response)
            return response.text

    # Methods on top of basic methods
    def get_json(self, url):
        return self.get(url=url).json()

    def get_text(self, url, params: dict = None):
        return self.get(url=url, params=params).text


class ConqueryConnection(object):
    _dataset = None

    def __init__(self, url: str, auth_url: str = "http://auth.localhost/auth", client_name: str = "conquery-release",
                 token: str = "", requests_timout: int = 5, dataset: str = None, user_login: bool = True,
                 token_refresh_rate: int = 300000):
        self._url: str = url.strip('/')
        self._token = token
        self._session: ConqueryConnectionSession = ConqueryConnectionSession(token=token)

        self._user_login: bool = user_login
        # settings when user login is enabled:
        self._auth_url: str = auth_url
        self._client_id: str = client_name
        self._token_refresh_rate = token_refresh_rate

        self._timeout: int = requests_timout
        self._datasets_with_permission: List[str] = []

        if not user_login:
            self._set_up_datasets(dataset=dataset)

    def _set_up_datasets(self, dataset: str = None):
        """Create new session and set datasets with permission globally"""

        self._datasets_with_permission = self.get_datasets()
        self.store_dataset_list_globally()

        self._dataset = self._get_dataset(dataset)

    def update_token(self, new_token: str):
        """Updates session token and own token (in case we make a new session)"""
        self._token = new_token
        self._session.update_token(new_token=new_token)

        # with user login this is the first time we have a token, so we set up the datasets
        if not self._datasets_with_permission:
            self._set_up_datasets()

    def login(self):
        """
        When _user_login is set, this function returns JavaScript-Code that will be executed in the output cell when
        code is run in a jupyter notebook. The Code will initialize a KeyCloak-Object that connects
        to the conquery-auth server. After successful login the token of all ConqueryConnection-Instanzes in the
        Notebook-Scope are updated and a second JavaScript-Process will refresh that token in self._token_refresh_rate
        seconds.
        """
        # the import is only done here because login is only used in specific use cases
        from IPython.display import Javascript  # type: ignore

        if not self._user_login:
            return None

        placeholder_dict = {
            "auth_url_placeholder": self._auth_url,
            "client_id_placeholder": self._client_id,
            "refresh_rate_placeholder": str(self._token_refresh_rate)
        }

        run_keycloak_script: str = open_text("cqapi.auth", "run_keycloak.js").read()

        for ph_name, ph_value in placeholder_dict.items():
            run_keycloak_script = run_keycloak_script.replace(ph_name, ph_value)

        return Javascript(run_keycloak_script)

    def change_dataset(self, dataset: str):
        self._dataset = self._get_dataset(dataset)

    def store_dataset_list_globally(self):
        cqapi.datasets.set_dataset_list(self._datasets_with_permission)

    def _get_dataset(self, dataset: str = None) -> str:

        if dataset is None:
            if self._dataset is not None:
                return self._dataset

            try:
                return self._datasets_with_permission[0]
            except IndexError:
                raise PermissionError("No Permission on any dataset")

        if dataset not in self._datasets_with_permission:
            raise ValueError(f"No permission on {dataset=}. \n"
                             f"Datasets with permission: {self._datasets_with_permission}")
        return dataset

    def get_dataset(self) -> str:
        if not self._dataset:
            self._set_up_datasets()

        if self._dataset is None:
            raise ValueError(f"No permission on any dataset")

        return self._dataset

    def get_datasets(self) -> list:
        response_list = self._session.get_json(f"{self._url}/api/datasets")
        return [d['id'] for d in response_list]

    def get_datasets_label_dict(self) -> Dict[str, str]:
        response_list = self._session.get_json(f"{self._url}/api/datasets")
        return {dataset_info.get('id'): dataset_info.get('label') for dataset_info in response_list}

    def get_dataset_label(self, dataset: str = None) -> str:
        dataset = self._get_dataset(dataset)
        dataset_label_dict = self.get_datasets_label_dict()
        if dataset not in dataset_label_dict.keys():
            raise ValueError(f"There is no permission on {dataset=}")
        return dataset_label_dict[dataset]

    def get_user_info(self) -> dict:
        return self._session.get_json(f"{self._url}/api/me")

    def get_concepts(self, dataset: str = None, remove_structure_elements: bool = True) -> dict:
        dataset = self._get_dataset(dataset)
        response = self._session.get_json(f"{self._url}/api/datasets/{dataset}/concepts")

        if remove_structure_elements:
            return {concept_id: concept for (
                concept_id, concept) in response['concepts'].items() if concept.get('active')}

        return response['concepts']

    def get_secondary_ids(self, dataset: str = None) -> list:
        dataset = self._get_dataset(dataset)
        response = self._session.get_json(f"{self._url}/api/datasets/{dataset}/concepts")
        return response['secondaryIds']

    def secondary_id_exists(self, secondary_id: str) -> bool:
        dataset = get_dataset_from_id(secondary_id)
        secondary_ids = self.get_secondary_ids(dataset)
        return secondary_id in [_.get("id") for _ in secondary_ids]

    def get_concept(self, concept_id: str, return_raw_format: bool = False) -> Union[List[dict], dict]:
        dataset = get_dataset_from_id(concept_id)
        response_dict = self._session.get_json(f"{self._url}/api/datasets/{dataset}/concepts/{concept_id}")

        if return_raw_format:
            return response_dict

        response_list = [dict(attrs, **{"ids": [c_id]}) for c_id, attrs in response_dict.items()]
        return response_list

    def get_stored_queries(self, dataset: str = None) -> list:
        dataset = self._get_dataset(dataset)
        response_list = self._session.get_json(f"{self._url}/api/datasets/{dataset}/queries")
        return response_list

    def get_query_id(self, label: str, dataset: str = None) -> str:
        queries = self.get_stored_queries(dataset=dataset)
        queries_with_label = [query["id"] for query in queries if query["label"] == label]
        if not queries_with_label:
            raise QueryNotFoundError

        return queries_with_label[0]

    def get_column_descriptions(self, query_id: str) -> list:
        dataset = get_dataset_from_id(query_id)

        self.wait_for_query_to_finish(query_id=query_id)

        result = self._session.get_json(f"{self._url}/api/datasets/{dataset}/queries/{query_id}")

        return result['columnDescriptions']

    def get_form_configs(self, dataset: str = None) -> list:
        dataset = self._get_dataset(dataset)
        result = self._session.get_json(f"{self._url}/api/datasets/{dataset}/form-configs")
        return result

    def get_form_config(self, form_config_id: str) -> dict:
        dataset = get_dataset_from_id(form_config_id)
        result = self._session.get_json(f"{self._url}/api/datasets/{dataset}/form-configs/{form_config_id}")
        return result

    def get_query(self, query_id: str) -> dict:
        dataset = get_dataset_from_id(query_id)
        result = self._session.get_json(f"{self._url}/api/datasets/{dataset}/queries/{query_id}")
        return result.get('query')

    def get_stored_query_info(self, query_id: str, label: str = None) -> dict:
        dataset = get_dataset_from_id(query_id)
        stored_queries = self.get_stored_queries(dataset)
        if query_id is not None:
            query_info = [query_info for query_info in stored_queries if query_info["id"] == query_id]
            if not query_info:
                raise ValueError(f"Could not find query with id {query_id}")
        elif label is not None:
            query_info = [query_info for query_info in stored_queries if query_info["label"] == label]
            if not query_info:
                raise ValueError(f"Could not find query with label {label}")
        else:
            raise ValueError(f"Neither query_id nor label is specified.")

        return query_info[0]

    def delete_stored_query(self, query_id: str) -> None:
        dataset = get_dataset_from_id(query_id)
        self._session.delete(f"{self._url}/api/datasets/{dataset}/queries/{query_id}")

    def delete_stored_queries(self, query_ids: List[str]):
        for query_id in query_ids:
            self.delete_stored_query(query_id=query_id)

    def get_number_of_results(self, query_id: str) -> int:

        self.wait_for_query_to_finish(query_id=query_id)

        response = self.get_query_info(query_id)

        n_results = response.get('numberOfResults')
        if n_results is None:
            return -1

        return int(n_results)

    def wait_for_query_to_finish(self, query_id: str, requests_per_sec: int = None):
        """Waits until query i finished and checks for NEW/FAILED"""
        query_info = self.get_query_info(query_id)

        while query_info['status'] == 'RUNNING':
            query_info = self.get_query_info(query_id)
            if requests_per_sec is None:
                continue
            sleep(1 / requests_per_sec)

        if query_info['status'] in ["NEW", "FAILED"]:
            raise Exception(f"Query Status: {query_info['status']} for query {query_info['id']}")

    def get_query_info(self, query_id: str):
        dataset = get_dataset_from_id(query_id)
        result = self._session.get_json(f"{self._url}/api/datasets/{dataset}/queries/{query_id}")
        return result

    def query_succeeded(self, query_id: str) -> bool:
        while True:
            response = self.get_query_info(query_id)
            if response["status"] == "DONE":
                return True
            if response["status"] in ["FAILED", "NEW"]:
                return False

    def get_query_label(self, query_id: str) -> str:
        query_info = self.get_query_info(query_id)
        return query_info.get("label")

    def query_id_exists(self, query_id: str) -> Union[bool, NoReturn]:
        dataset = get_dataset_from_id(query_id)

        if self._session is None:
            raise ValueError("No Session running")

        with self._session.get(f"{self._url}/api/datasets/{dataset}/queries/{query_id}") as response:
            if response.status_code == 404:
                return False

            raise_for_status(response=response)

        return True

    def execute_query(self, query: Union[dict, QueryObject], dataset: str = None,
                      label: str = None) -> str:
        try:
            query = query.to_dict()  # type:ignore
        except AttributeError:
            if not isinstance(query, dict):
                raise TypeError(f"{query=} must be of type dict or QueryObject with method write_query")

        if dataset is None:
            dataset = self._dataset if self._dataset is not None else get_dataset_from_query(query)

        result = self._session.post(f"{self._url}/api/datasets/{dataset}/queries", query)

        try:
            if label is not None:
                self._session.patch(f"{self._url}/api/datasets/{dataset}/queries/{result['id']}",
                                    {"label": label})
            return result['id']

        except KeyError:
            raise ValueError("Error encountered when executing query", result.get('message'), result.get('details'))

    def reexecute_query(self, query_id: str) -> None:
        dataset = get_dataset_from_id(query_id)
        self._session.post(f"{self._url}/api/datasets/{dataset}/queries/{query_id}/reexecute", data="")

    def get_query_result(self, query_id: str, return_pandas: bool = True, download_with_arrow: bool = True,
                         already_reexecuted: bool = False, delete_query: bool = False):
        """ Returns results for given query.
        Blocks until the query is DONE.

        :param download_with_arrow: Use apache arrow for transferring data from backend to cqapi
        :param query_id:
        :param already_reexecuted: only needed when reexecuting query, to know when trapped in an endless loop
        e.g. requests_per_sec = 2 -> sleep 0.5 seconds between requests
        :param return_pandas: when true, returns data as pandas.DataFrame
        :param delete_query: deletes query after getting result
        :return: str containing the returned csv's
        """

        self.wait_for_query_to_finish(query_id=query_id)

        response = self.get_query_info(query_id)

        response_status = response["status"]

        if response_status == "FAILED":
            raise Exception(f"Query with {query_id=} failed.")
        elif response_status == "NEW":
            if already_reexecuted:
                raise Exception(f"Query {query_id} still in state NEW after reexecuting..")
            self.reexecute_query(query_id)
            sleep(0.5)
            data = self.get_query_result(query_id, already_reexecuted=True)
        elif response_status == "DONE":
            if return_pandas:
                import pandas as pd  # type: ignore
                if download_with_arrow:
                    import pyarrow as pa  # type: ignore
                    result_url_arrow = self._get_result_url(response=response, file_type="arrf")
                    # if date_as_object=False, date columns will be in numpy Int64 / pd.Timestamp format
                    data = \
                        pa.ipc.open_file(self._session.get(result_url_arrow).content).read_pandas(date_as_object=False)
                else:
                    result_url_csv = self._get_result_url(response=response, file_type="csv")
                    result_string = self._download_query_results(result_url_csv)
                    data = pd.read_csv(StringIO(result_string), sep=";", dtype=str, keep_default_na=False)

            else:
                result_url_csv = self._get_result_url(response=response, file_type="csv")
                result_string = self._download_query_results(result_url_csv)
                data = list(csv.reader(result_string.splitlines(), delimiter=';'))
        else:
            raise ValueError(f"Unknown response status {response_status}")

        if delete_query:
            self.delete_stored_query(query_id=query_id)

        return data

    def _download_query_results(self, url):
        return self._session.get_text(url, params={"pretty": "false"})

    @staticmethod
    def _get_result_url(response, file_type: str = "csv"):
        result_urls = response["resultUrls"]

        if file_type not in ["csv", "xlsx"]:
            result_url_base = ".".join(response["resultUrls"][0].split(".")[:-1])
            return f'{result_url_base}.{file_type}'

        for result_url in result_urls:
            if result_url.split(".")[-1] == file_type:
                return result_url

        result_url_message = '\n'.join(result_urls)
        raise ValueError(f"Could not find result url for {file_type=}. \n"
                         f"Result Urls: \n {result_url_message}")
