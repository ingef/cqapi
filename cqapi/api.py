import csv
from io import StringIO
from time import sleep
from typing import Union, List, Dict, NoReturn, Optional

import requests
from requests import Response
from requests.exceptions import HTTPError
import pandas as pd
import pyarrow as pa

import cqapi.datasets
from cqapi.conquery.api import ConqueryApiUrls, Url
from cqapi.conquery_ids import get_dataset_from_id_string, ConqueryId
from cqapi.exceptions import QueryNotFoundError, RetryCountExceeded, ConqueryTimeout
from cqapi.queries.utils import get_dataset_from_query
from cqapi.queries.base_elements import QueryObject


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

    def __init__(self, token: str, timeout_s: int, retry_count_get: int):
        self.token: str = token
        self._session = requests.session()
        self._session.headers.update(self.header)
        self._timeout_s = timeout_s
        self._retry_count_get = retry_count_get

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
        retry_count = 0

        while retry_count <= self._retry_count_get:
            try:
                response = self._session.get(url, params=params, timeout=self._timeout_s)
                # exit loop if no exception rises
                break
            except requests.exceptions.Timeout as e:
                retry_count = retry_count + 1

        if retry_count > self._retry_count_get:
            raise RetryCountExceeded(f"GET-Request {url=} exceeded limit: {self._retry_count_get=}")

        raise_for_status(response)
        return response


    def post(self, url, data):
        try:
            with self._session.post(url=url, json=data, timeout=self._timeout_s) as response:
                raise_for_status(response)
                return response.json()
        except requests.exceptions.Timeout:
            #do nothing here, raise ConqueryTimeout to get a less verbose message
            pass
        if isinstance(url, Url):
            url = url.parse()
        raise ConqueryTimeout(f"POST-Request {url=} timeout: {self._timeout_s=}")

    def patch(self, url, data):
        try:
            with self._session.patch(url, json=data, timeout=self._timeout_s) as response:
                raise_for_status(response)
                return response.json()
        except requests.exceptions.Timeout:
            # do nothing here, raise ConqueryTimeout to get a less verbose message
            pass
        if isinstance(url, Url):
            url = url.parse()
        raise ConqueryTimeout(f"PATCH-Request {url=} timeout: {self._timeout_s=}")

    def delete(self, url):
        try:
            with self._session.delete(url, timeout=self._timeout_s) as response:
                raise_for_status(response)
                return response.text
        except requests.exceptions.Timeout:
            # do nothing here, raise ConqueryTimeout to get a less verbose message
            pass
        if isinstance(url, Url):
            url = url.parse()
        raise ConqueryTimeout(f"DELETE-Request {url=} timeout: {self._timeout_s=}")

    # Methods on top of basic methods
    def get_json(self, url):
        return self.get(url=url).json()

    def get_text(self, url, params: dict = None):
        return self.get(url=url, params=params).text


class ConqueryConnection(object):
    _dataset = None

    def __init__(self, url: str,
                 token: str = "",
                 requests_timout_s: int = 10,
                 requests_retry_count: int = 2,
                 dataset: str = None):

        self.conquery_api_urls = ConqueryApiUrls(conquery_url=url.strip("/"))
        self._token = token
        self._datasets_with_permission: List[str] = []

        self._session: ConqueryConnectionSession = ConqueryConnectionSession(token=token,
                                                                             timeout_s=requests_timout_s,
                                                                             retry_count_get=requests_retry_count)

        if token:
            self._set_up_datasets(dataset=dataset)


    def close(self):
        self._session.close()

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
        dataset = self.conquery_api_urls.datasets()
        dataset_parsed = dataset.parse()
        response_list = self._session.get_json(dataset_parsed)
        return [d['id'] for d in response_list]

    def get_datasets_label_dict(self) -> Dict[str, str]:
        response_list = self._session.get_json(self.conquery_api_urls.datasets().parse())
        return {dataset_info.get('id'): dataset_info.get('label') for dataset_info in response_list}

    def get_dataset_label(self, dataset: str = None) -> str:
        dataset = self._get_dataset(dataset)
        dataset_label_dict = self.get_datasets_label_dict()
        if dataset not in dataset_label_dict.keys():
            raise ValueError(f"There is no permission on {dataset=}")
        return dataset_label_dict[dataset]

    def get_user_info(self) -> dict:
        return self._session.get_json(self.conquery_api_urls.me().parse())

    def user_has_group(self, group_id: str) -> bool:
        """Checks if user has group with id group_id"""
        groups = self.get_user_info().get('groups', [])

        group_ids = [group.get('id') for group in groups]

        return group_id in group_ids

    def get_concepts(self, dataset: str = None, remove_structure_elements: bool = True) -> dict:
        dataset = self._get_dataset(dataset)
        obj = self.conquery_api_urls.concepts(dataset=dataset).parse()
        response = self._session.get_json(obj)

        if remove_structure_elements:
            return {concept_id: concept for (
                concept_id, concept) in response['concepts'].items() if concept.get('active')}

        return response['concepts']

    def get_secondary_ids(self, dataset: str = None) -> list:
        dataset = self._get_dataset(dataset)
        response = self._session.get_json(self.conquery_api_urls.concepts(dataset=dataset))
        return response['secondaryIds']

    def secondary_id_exists(self, secondary_id: str) -> bool:
        dataset = get_dataset_from_id_string(secondary_id)
        secondary_ids = self.get_secondary_ids(dataset)
        return secondary_id in [_.get("id") for _ in secondary_ids]

    def get_concept(self, concept_id: Union[str, ConqueryId],
                    return_raw_format: bool = False) -> dict:
        if isinstance(concept_id, ConqueryId):
            concept_id = concept_id.id

        response_dict = self._session.get_json(self.conquery_api_urls.concept_id(concept_id=concept_id).parse())

        return response_dict

    def get_stored_queries(self, dataset: str = None) -> list:
        dataset = self._get_dataset(dataset)
        response_list = self._session.get_json(self.conquery_api_urls.queries(dataset=dataset).parse())
        return response_list

    def get_query_id(self, label: str, dataset: str = None) -> str:
        queries = self.get_stored_queries(dataset=dataset)
        queries_with_label = [query["id"] for query in queries if query["label"] == label]
        if not queries_with_label:
            raise QueryNotFoundError

        return queries_with_label[0]

    def get_column_descriptions(self, query_id: str) -> list:
        self.wait_for_query_to_finish(query_id=query_id)

        result = self._session.get_json(self.conquery_api_urls.query_id(query_id=query_id).parse())

        return result['columnDescriptions']

    def get_form_configs(self, dataset: str = None) -> list:
        dataset = self._get_dataset(dataset)
        result = self._session.get_json(self.conquery_api_urls.form_configs(dataset=dataset).parse())
        return result

    def get_form_config(self, form_config_id: str) -> dict:
        result = self._session.get_json(self.conquery_api_urls.form_config_id(form_config_id=form_config_id).parse())
        return result

    def get_query(self, query_id: str) -> Optional[dict]:
        if not query_id:
            return None
        result = self._session.get_json(self.conquery_api_urls.query_id(query_id=query_id).parse())
        return result.get('query')

    def explode_query(self, query: dict) -> dict:
        if "root" in query:
            query["root"] = self.explode_query(query=query["root"])
        elif "child" in query:
            query["child"] = self.explode_query(query=query["child"])
        elif "children" in query:
            for i, child_query in enumerate(query["children"]):
                query["children"][i] = self.explode_query(query=child_query)
        elif query["type"] == "SAVED_QUERY":
            return self.get_query(query_id=query["query"])["root"]
        return query

    def get_stored_query_info(self, query_id: str, label: str = None) -> dict:
        dataset = get_dataset_from_id_string(query_id)
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
        self._session.delete(self.conquery_api_urls.query_id(query_id=query_id).parse())

    def delete_stored_queries(self, query_ids: List[str]):
        for query_id in query_ids:
            self.delete_stored_query(query_id=query_id)

    def get_number_of_results(self, query_id: str) -> int:

        self.reexecute_when_status_new(query_id=query_id)

        self.wait_for_query_to_finish(query_id=query_id)

        response = self.get_query_info(query_id)

        n_results = response.get('numberOfResults')
        if n_results is None:
            return -1

        return int(n_results)

    def reexecute_when_status_new(self, query_id: str):
        """On restart of java backend, query_ids have status "NEW" and need to be rexecuted to retrieve data"""
        query_info = self.get_query_info(query_id)

        if query_info["status"] == "NEW":
            self.reexecute_query(query_id)
            sleep(0.5)

    def wait_for_query_ignore_final_status(self, query_id: str, requests_per_sec: int = None):
        """Waits until query i finished, no additional checks"""
        query_info = self.get_query_info(query_id)

        while query_info['status'] == 'RUNNING':
            query_info = self.get_query_info(query_id)
            if requests_per_sec is None:
                continue
            sleep(1 / requests_per_sec)

        return query_info


    def wait_for_query_to_finish(self, query_id: str, requests_per_sec: int = None):
        """Waits until query i finished and checks for NEW/FAILED"""

        query_info = self.wait_for_query_ignore_final_status(query_id=query_id, requests_per_sec=requests_per_sec)

        if query_info['status'] in ["NEW", "FAILED"]:
            raise Exception(f"Query Status: {query_info['status']} for query {query_info['id']}")

    def get_query_info(self, query_id: str):
        result = self._session.get_json(self.conquery_api_urls.query_id(query_id=query_id).parse())
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

        if self._session is None:
            raise ValueError("No Session running")

        try:
            with self._session.get(self.conquery_api_urls.query_id(query_id=query_id).parse()) as response:
                if response.status_code == 404:
                    return False

                raise_for_status(response=response)
        except HTTPError:
            return False

        return True

    def execute_query(self, query: Union[dict, QueryObject], dataset: str = None,
                      label: str = None, wait: bool= True) -> str:
        try:
            query = query.to_dict()  # type:ignore
        except AttributeError:
            if not isinstance(query, dict):
                raise TypeError(f"{query=} must be of type dict or QueryObject with method write_query")

        if dataset is None:
            dataset = self._dataset if self._dataset is not None else get_dataset_from_query(query)

        result = self._session.post(self.conquery_api_urls.queries(dataset=dataset), query)

        try:
            if label is not None:
                self._session.patch(self.conquery_api_urls.query_id(query_id=result['id']).parse(), {"label": label})

            query_id = result['id']
            if wait:
                self.wait_for_query_ignore_final_status(query_id=query_id, requests_per_sec=10)
            return query_id

        except KeyError:
            raise ValueError("Error encountered when executing query", result.get('message'), result.get('details'))

    def reexecute_query(self, query_id: str) -> None:
        self._session.post(self.conquery_api_urls.query_reexecute(query_id=query_id), data="")

    def get_query_result(self, query_id: str, return_pandas: bool = True, file_format: str = "arrow",
                         delete_query: bool = False):
        """ Returns results for given query.
        Blocks until the query is DONE.

        :param file_format: Options are arrow, csv
        :param query_id:
        :param return_pandas: when true, returns data as pandas.DataFrame
        :param delete_query: deletes query after getting result
        :return: str containing the returned csv's
        """

        self.reexecute_when_status_new(query_id=query_id)

        self.wait_for_query_to_finish(query_id=query_id)

        response = self.get_query_info(query_id)

        response_status = response["status"]

        if response_status == "FAILED":
            raise Exception(f"Query with {query_id=} failed.")
        elif response_status == "NEW":
            raise Exception(f"Query {query_id} still in state NEW after reexecuting..")
        elif response_status == "DONE":
            result_url = self.conquery_api_urls.query_result(query_id=query_id, file_format=file_format).parse()

            if file_format == "arrow":
                data = pa.ipc.open_file(self._session.get(result_url).content).read_pandas(date_as_object=False)

            else:
                result_string = self._download_query_results(result_url)
                if return_pandas:
                    data = pd.read_csv(StringIO(result_string), sep=";", dtype=str, keep_default_na=False)
                else:
                    data = list(csv.reader(result_string.splitlines(), delimiter=';'))

        else:
            raise ValueError(f"Unknown response status {response_status}")

        if delete_query:
            self.delete_stored_query(query_id=query_id)

        return data

    def _download_query_results(self, url):
        return self._session.get_text(url, params={"pretty": "false"})


    def send_custom_get(self, resource: str, params: dict = None):
        url = self.conquery_api_urls.conquery_url + resource
        return self._session.get(url=url, params=params)

    def send_custom_post(self, resource: str, data):
        url = self.conquery_api_urls.conquery_url + resource
        return self._session.post(url=url, data=data)

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
