import requests
import csv
from time import sleep
from cqapi.queries.queries import wrap_saved_query

class CqApiError(BaseException):
    pass


class ConqueryClientConnectionError(CqApiError):
    def __init__(self, msg):
        self.message = msg


def get_json(session, url):
    return get(session, url).json()


def get(session, url):
    with session.get(url) as response:
        response.raise_for_status()
        return response


def get_text(session, url):
    return get(session, url).text


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
        raise (f"Query Status: {query_status} for query {query_info['id']}")


class ConqueryConnection(object):
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

    def __init__(self, url: str, token: str = "", requests_timout: int = 5, open_session: bool = False,
                 check_connection: bool = True, check_permission: bool = True):
        self._url = url.strip('/')
        self._token = token
        self._check_connection = check_connection
        self._check_permission = check_permission
        self._timeout = requests_timout
        self._header = {'Authorization': f'Bearer {self._token}',
                        'Accept-Language': 'en-GB;q=0.8,en;q=0.7,en-US;q=0.6'}
        self._session = None
        if open_session:
            self.open_session()

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

    def get_user_info(self):
        response = get_json(self._session, f"{self._url}/api/me")
        return response

    def get_datasets(self):
        response_list = get_json(self._session, f"{self._url}/api/datasets")
        return [d['id'] for d in response_list]

    def get_datasets_label_dict(self):
        response_list = get_json(self._session, f"{self._url}/api/datasets")
        return {dataset_info.get('id'): dataset_info.get('label') for dataset_info in response_list}


    def get_dataset_label(self, dataset):
        dataset_label_dict = self.get_datasets_label_dict()
        if dataset not in dataset_label_dict.keys():
            raise ValueError(f"There is no permission on {dataset=}")
        return dataset_label_dict.get(dataset)

    def get_concepts(self, dataset, remove_structure_elements=True):
        response = get_json(self._session, f"{self._url}/api/datasets/{dataset}/concepts")

        if remove_structure_elements:
            return {concept_id: concept for (
                concept_id, concept) in response['concepts'].items() if concept.get('active')}

        return response['concepts']

    def get_secondary_ids(self, dataset):
        response = get_json(self._session, f"{self._url}/api/datasets/{dataset}/concepts")
        return response['secondaryIds']

    def secondary_id_exists(self, dataset: str, secondary_id: str) -> bool:
        secondary_ids = self.get_secondary_ids(dataset)
        return secondary_id in [_.get("id") for _ in secondary_ids]

    def get_concept(self, dataset, concept_id):
        response_dict = get_json(self._session, f"{self._url}/api/datasets/{dataset}/concepts/{concept_id}")
        response_list = [dict(attrs, **{"ids": [c_id]}) for c_id, attrs in response_dict.items()]
        return response_list

    def get_stored_queries(self, dataset):
        response_list = get_json(self._session, f"{self._url}/api/datasets/{dataset}/stored-queries")
        return response_list

    def get_column_descriptions(self, dataset, query_id):
        result = get_json(self._session, f"{self._url}/api/datasets/{dataset}/stored-queries/{query_id}")
        return result.get('columnDescriptions')

    def get_form_configs(self, dataset):
        result = get_json(self._session, f"{self._url}/api/datasets/{dataset}/form-configs")
        return result

    def get_form_config(self, dataset, form_config_id):
        result = get_json(self._session, f"{self._url}/api/datasets/{dataset}/form-configs/{form_config_id}")
        return result

    def get_query(self, dataset, query_id):
        result = get_json(self._session, f"{self._url}/api/datasets/{dataset}/stored-queries/{query_id}")
        return result.get('query')

    def get_stored_query(self, dataset, query_id):
        result = get_json(self._session, f"{self._url}/api/datasets/{dataset}/stored-queries/{query_id}")
        return result.get('query')

    def delete_stored_query(self, dataset, query_id):
        result = delete(self._session, f"{self._url}/api/datasets/{dataset}/stored-queries/{query_id}")
        return result

    def get_number_of_results(self, dataset, query_id):
        response = self.get_query_info(dataset, query_id)
        check_query_status(response)
        while response['status'] == 'RUNNING':
            response = self.get_query_info(dataset, query_id)
            check_query_status(response)

        n_results = response.get('numberOfResults')
        if n_results is None:
            return -1

        return int(n_results)

    def get_query_info(self, dataset, query_id):
        result = get_json(self._session, f"{self._url}/api/datasets/{dataset}/queries/{query_id}")
        return result

    def query_succeeded(self, dataset, query_id):
        while True:
            response = self.get_query_info(dataset, query_id)
            if response["status"] == "DONE":
                return True
            if response["status"] in ["FAILED", "NEW"]:
                return False

    def get_query_label(self, dataset, query_id):
        query_info = self.get_query_info(dataset, query_id)
        return query_info.get("label")

    def execute_query(self, dataset, query, label=None):
        result = post(self._session, f"{self._url}/api/datasets/{dataset}/queries", query)
        try:
            if label is not None:
                patch(self._session, f"{self._url}/api/datasets/{dataset}/stored-queries/{result['id']}",
                      {"label": label})
            return result['id']
        except KeyError:
            raise ValueError("Error encountered when executing query", result.get('message'), result.get('details'))

    def execute_form_query(self, dataset, form_query):
        result = post(self._session, f"{self._url}/api/datasets/{dataset}/queries", form_query)
        try:
            return result['id']
        except KeyError:
            raise ValueError("Error encountered when executing query", result.get('message'), result.get('details'))

    def get_query_result(self, dataset: str, query_id: str, requests_per_sec=None):
        """ Returns results for given query.
        Blocks until the query is DONE.

        :param dataset:
        :param query_id:
        :param requests_per_sec: Number of request to do per second (default None -> as many as possible)
        e.g. requests_per_sec = 2 -> sleep 0.5 seconds between requests
        :return: str containing the returned csv's
        """
        response = self.get_query_info(dataset, query_id)

        while not response['status'] == 'RUNNING':
            response = self.get_query_info(dataset, query_id)
            if requests_per_sec is None:
                continue
            sleep(1 / requests_per_sec)

        response_status = response["status"]

        if response_status == "FAILED":
            raise Exception(f"Query with {query_id=} failed with code. {response.status_code}")
        elif response_status == "NEW":
            self.execute_query(dataset, wrap_saved_query(query_id))
            return self.get_query_result(dataset, query_id)
        elif response_status == "DONE":
            result_string = self._download_query_results(response["resultUrl"])
            return list(csv.reader(result_string.splitlines(), delimiter=';'))
        else:
            raise ValueError(f"Unknown response status {response_status}")

    def _download_query_results(self, url):
        return get_text(self._session, url)
