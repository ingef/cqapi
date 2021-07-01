import csv
from io import StringIO
from time import sleep
import requests
from cqapi.conquery_ids import get_dataset as get_dataset_from_id
from cqapi.exceptions import ConqueryClientConnectionError, QueryNotFoundError
from cqapi.queries import get_dataset_from_query
from cqapi.queries.elements import QueryObject
from typing import Union, List


def get_json(session, url):
    return get(session, url).json()


def get(session, url, params: dict = None):
    with session.get(url, params=params) as response:
        response.raise_for_status()
        return response


def get_text(session, url, params: dict = None):
    return get(session, url, params=params).text


def post(session, url, data):
    with session.post(url, json=data) as response:
        response.raise_for_status()
        return response.json()


def patch(session, url, data):
    with session.patch(url, json=data) as response:
        response.raise_for_status()
        return response.json()


def delete(session, url):
    with session.delete(url) as response:
        response.raise_for_status()
        return response.text


def check_query_status(query_info):
    query_status = query_info["status"]
    if query_status in ["NEW", "FAILED"]:
        raise Exception(f"Query Status: {query_status} for query {query_info['id']}")


class ConqueryConnection(object):
    _session = None
    _dataset = None

    def __enter__(self):
        # open session and set header
        self._session = requests.Session()
        self._session.headers.update(self._header)

        # try to fail early if conquery is not available at self._url
        if self._check_connection:
            try:
                get_json(self._session, f"{self._url}/api/datasets")
            except ConnectionError:
                error_msg = f"Could not connect to Conquery, are you sure {self._url} is the right address?"
                raise ConqueryClientConnectionError(error_msg)

        # Check if token is known to conquery and if it has access to any dataset
        if self._check_permission:
            with self._session.get(f"{self._url}/api/datasets") as response:
                if response.status_code == 401 or not response.json():
                    error_msg = f"There is no permission for accessing any dataset."
                    raise ConqueryClientConnectionError(error_msg)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._session.close()

    def __init__(self, url: str, token: str = "", requests_timout: int = 5,
                 check_connection: bool = True, check_permission: bool = True, dataset: str = None):
        self._url = url.strip('/')
        self._token = token
        self._check_connection = check_connection
        self._check_permission = check_permission
        self._timeout = requests_timout
        self._header = {'Authorization': f'Bearer {self._token}',
                        'Accept-Language': 'de-DE,de;q=0.9',
                        'pretty': 'false'}
        self.open_session()
        self._datasets_with_permission = self.get_datasets()
        if dataset is not None:
            self.set_dataset(dataset)

    def set_dataset(self, dataset: str = None):
        self._dataset = self._get_dataset(dataset)

    def _get_dataset(self, dataset: str = None):

        if dataset is None:
            if self._dataset is not None:
                return self._dataset

            return self._datasets_with_permission[0]

        if dataset not in self._datasets_with_permission:
            raise ValueError(f"No permission on {dataset=}. \n"
                             f"Datasets with permission: {self._datasets_with_permission}")
        return dataset

    def open_session(self):
        self.close_session()
        # open session and set header
        self._session = requests.Session()
        self._session.headers.update(self._header)

    def close_session(self):
        if self.has_open_session():
            self._session.close()

    def has_open_session(self):
        return self._session is not None

    def get_user_info(self) -> dict:
        response = get_json(self._session, f"{self._url}/api/me")
        return response

    def get_datasets(self) -> list:
        response_list = get_json(self._session, f"{self._url}/api/datasets")
        return [d['id'] for d in response_list]

    def get_datasets_label_dict(self) -> dict:
        response_list = get_json(self._session, f"{self._url}/api/datasets")
        return {dataset_info.get('id'): dataset_info.get('label') for dataset_info in response_list}

    def get_dataset_label(self, dataset: str = None) -> str:
        dataset = self._get_dataset(dataset)
        dataset_label_dict = self.get_datasets_label_dict()
        if dataset not in dataset_label_dict.keys():
            raise ValueError(f"There is no permission on {dataset=}")
        return dataset_label_dict.get(dataset)

    def get_concepts(self, dataset: str = None, remove_structure_elements: bool = True) -> dict:
        dataset = self._get_dataset(dataset)
        response = get_json(self._session, f"{self._url}/api/datasets/{dataset}/concepts")

        if remove_structure_elements:
            return {concept_id: concept for (
                concept_id, concept) in response['concepts'].items() if concept.get('active')}

        return response['concepts']

    def get_secondary_ids(self, dataset: str = None) -> list:
        dataset = self._get_dataset(dataset)
        response = get_json(self._session, f"{self._url}/api/datasets/{dataset}/concepts")
        return response['secondaryIds']

    def secondary_id_exists(self, secondary_id: str) -> bool:
        dataset = get_dataset_from_id(secondary_id)
        secondary_ids = self.get_secondary_ids(dataset)
        return secondary_id in [_.get("id") for _ in secondary_ids]

    def get_concept(self, concept_id: str, return_raw_format: bool = False) -> Union[List[dict], dict]:
        dataset = get_dataset_from_id(concept_id)
        response_dict = get_json(self._session, f"{self._url}/api/datasets/{dataset}/concepts/{concept_id}")

        if return_raw_format:
            return response_dict

        response_list = [dict(attrs, **{"ids": [c_id]}) for c_id, attrs in response_dict.items()]
        return response_list

    def get_stored_queries(self, dataset: str = None) -> list:
        dataset = self._get_dataset(dataset)
        response_list = get_json(self._session, f"{self._url}/api/datasets/{dataset}/stored-queries")
        return response_list

    def get_query_id(self, label: str, dataset: str = None) -> str:
        queries = self.get_stored_queries()
        queries_with_label = [query["id"] for query in queries if query["label"] == label]
        if not queries_with_label:
            raise QueryNotFoundError

        return queries_with_label[0]

    def get_column_descriptions(self, query_id: str) -> list:
        dataset = get_dataset_from_id(query_id)
        result = get_json(self._session, f"{self._url}/api/datasets/{dataset}/stored-queries/{query_id}")
        return result['columnDescriptions']

    def get_form_configs(self, dataset: str = None) -> list:
        dataset = self._get_dataset(dataset)
        result = get_json(self._session, f"{self._url}/api/datasets/{dataset}/form-configs")
        return result

    def get_form_config(self, form_config_id: str) -> dict:
        dataset = get_dataset_from_id(form_config_id)
        result = get_json(self._session, f"{self._url}/api/datasets/{dataset}/form-configs/{form_config_id}")
        return result

    def get_query(self, query_id: str) -> dict:
        dataset = get_dataset_from_id(query_id)
        result = get_json(self._session, f"{self._url}/api/datasets/{dataset}/stored-queries/{query_id}")
        return result.get('query')

    def get_stored_query_info(self, query_id: str = None, label: str = None) -> dict:
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

    def delete_stored_query(self, query_id: str):
        dataset = get_dataset_from_id(query_id)
        result = delete(self._session, f"{self._url}/api/datasets/{dataset}/stored-queries/{query_id}")
        return result

    def delete_stored_queries(self, query_ids: List[str]):
        for query_id in query_ids:
            self.delete_stored_query(query_id=query_id)

    def get_number_of_results(self, query_id: str) -> int:
        response = self.get_query_info(query_id)
        check_query_status(response)
        while response['status'] == 'RUNNING':
            response = self.get_query_info(query_id)
            check_query_status(response)

        n_results = response.get('numberOfResults')
        if n_results is None:
            return -1

        return int(n_results)

    def get_query_info(self, query_id: str):
        dataset = get_dataset_from_id(query_id)
        result = get_json(self._session, f"{self._url}/api/datasets/{dataset}/queries/{query_id}")
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

    def execute_query(self, query: Union[dict, QueryObject], dataset: str = None,
                      label: str = None) -> str:
        try:
            query = query.write_query()
        except AttributeError:
            if not isinstance(query, dict):
                raise TypeError(f"{query=} must be of type dict or QueryObject with method write_query")

        if dataset is None:
            dataset = get_dataset_from_query(query)

        result = post(self._session, f"{self._url}/api/datasets/{dataset}/queries", query)

        try:
            if label is not None:
                patch(self._session, f"{self._url}/api/datasets/{dataset}/stored-queries/{result['id']}",
                      {"label": label})
            return result['id']
        
        except KeyError:
            raise ValueError("Error encountered when executing query", result.get('message'), result.get('details'))

    def reexecute_query(self, query_id: str) -> None:
        dataset = get_dataset_from_id(query_id)
        post(self._session, f"{self._url}/api/datasets/{dataset}/stored-queries/{query_id}/reexecute", data="")

    def get_query_result(self, query_id: str, return_pandas: bool = False, requests_per_sec=None,
                         already_reexecuted: bool = False, delete_query: bool = False):
        """ Returns results for given query.
        Blocks until the query is DONE.

        :param query_id:
        :param already_reexecuted: only needed when reexecuting query, to know when trapped in an endless loop
        :param requests_per_sec: Number of request to do per second (default None -> as many as possible)
        e.g. requests_per_sec = 2 -> sleep 0.5 seconds between requests
        :param return_pandas: when true, returns data as pandas.DataFrame
        :param delete_query: deletes query after getting result
        :return: str containing the returned csv's
        """

        response = self.get_query_info(query_id)

        while response['status'] == 'RUNNING':
            response = self.get_query_info(query_id)
            if requests_per_sec is None:
                continue
            sleep(1 / requests_per_sec)

        response_status = response["status"]

        if response_status == "FAILED":
            raise Exception(f"Query with {query_id=} failed with code. {response.status_code}")
        elif response_status == "NEW":
            if already_reexecuted:
                raise Exception(f"Query {query_id} still in state NEW after reexecuting..")
            self.reexecute_query(query_id)
            sleep(0.5)
            data = self.get_query_result(query_id, already_reexecuted=True)
        elif response_status == "DONE":
            result_url_csv = self.get_result_url(response=response, file_type="csv")
            result_string = self._download_query_results(result_url_csv)
            if return_pandas:
                import pandas as pd
                data = pd.read_csv(StringIO(result_string), sep=";", dtype=str, keep_default_na=False)
            else:
                data = list(csv.reader(result_string.splitlines(), delimiter=';'))
        else:
            raise ValueError(f"Unknown response status {response_status}")

        if delete_query:
            self.delete_stored_query(query_id=query_id)

        return data

    def get_data(self, query_id: str):
        """ Returns results for given query.
        Blocks until the query is DONE.

        :param query_id:
        :return: str containing the returned csv's
        """

        response = self.get_query_info(query_id)

        while response['status'] == 'RUNNING':
            response = self.get_query_info(query_id)
            sleep(1 / 100)

        response_status = response["status"]

        if response_status == "FAILED":
            raise Exception(f"Query with {query_id=} failed.")
        elif response_status == "NEW":
            raise ValueError(f"query stats NEW - query has to be reexecuted")
        elif response_status == "DONE":
            import pyarrow as pa
            result_url_arrow = self.get_result_url(response=response, file_type="arrf")
            return pa.ipc.open_file(get(self._session, result_url_arrow).content).read_pandas()
        else:
            raise ValueError(f"Unknown response status {response_status}")

    def _download_query_results(self, url):
        return get_text(self._session, url, params={"pretty": "false"})

    def get_result_url(self, response, file_type: str = "csv"):
        result_urls = response["resultUrls"]

        if file_type not in ["csv", "xlsx"]:
            result_url_base = ".".join(response["resultUrl"].split(".")[:-1])
            return f'{result_url_base}.{file_type}'

        for result_url in result_urls:
            if result_url.split(".")[-1] == file_type:
                return result_url

        result_url_message = '\n'.join(result_urls)
        raise ValueError(f"Could not find result url for {file_type=}. \n"
                         f"Result Urls: \n {result_url_message}")
