from aiohttp import ClientSession
from aiohttp import ClientConnectorError
from cqapi import util
import csv
from time import sleep


class CqApiError(BaseException):
    pass


class ConqueryClientConnectionError(CqApiError):
    def __init__(self, msg):
        self.message = msg


async def get(session, url):
    async with session.get(url) as response:
        response.raise_for_status()
        return await response.json()


async def get_text(session, url):
    async with session.get(url) as response:
        response.raise_for_status()
        return await response.text()


async def post(session, url, data):
    async with session.post(url, json=data) as response:
        response.raise_for_status()
        return await response.json()


async def patch(session, url, data):
    async with session.patch(url, json=data) as response:
        response.raise_for_status()
        return await response.json()


async def delete(session, url):
    async with session.delete(url) as response:
        response.raise_for_status()
        return await response.text()


class ConqueryConnection(object):
    async def __aenter__(self):
        self._session = ClientSession(headers=self._header)
        # try to fail early if conquery is not available at self._url
        if self._check_connection:
            try:
                await get(self._session, f"{self._url}/api/datasets")
            except ClientConnectorError:
                error_msg = f"Could not connect to Conquery, are you sure {self._url} is the right address?"
                raise ConqueryClientConnectionError(error_msg)
        # Check if token is known to conquery and if it has access to any dataset
        if self._check_permission:
            async with self._session.get(f"{self._url}/api/datasets") as response:
                if response.status == 401 or not await response.json():
                    error_msg = f"There is no permission for accessing any dataset."
                    raise ConqueryClientConnectionError(error_msg)

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._session.close()

    def __init__(self, url, token="", requests_timout=5, check_connection=True, check_permission=True):
        self._url = url.strip('/')
        self._token = token
        self._check_connection = check_connection
        self._check_permission = check_permission
        self._timeout = requests_timout
        self._header = {'Authorization': f'Bearer {self._token}',
                        'Accept-Language': 'en-GB;q=0.8,en;q=0.7,en-US;q=0.6'}

    async def get_user_info(self):
        response = await get(self._session, f"{self._url}/api/me")
        return response

    async def get_datasets(self):
        response_list = await get(self._session, f"{self._url}/api/datasets")
        return [d['id'] for d in response_list]

    async def get_datasets_label_dict(self):
        response_list = await get(self._session, f"{self._url}/api/datasets")
        return {dataset_info.get('id'): dataset_info.get('label') for dataset_info in response_list}

    async def get_concepts(self, dataset):
        response = await get(self._session, f"{self._url}/api/datasets/{dataset}/concepts")
        return response['concepts']

    async def get_concept(self, dataset, concept_id):
        response_dict = await get(self._session, f"{self._url}/api/datasets/{dataset}/concepts/{concept_id}")
        response_list = [dict(attrs, **{"ids": [c_id]}) for c_id, attrs in response_dict.items()]
        return response_list

    async def get_stored_queries(self, dataset):
        response_list = await get(self._session, f"{self._url}/api/datasets/{dataset}/stored-queries")
        return response_list

    async def get_column_descriptions(self, dataset, query_id):
        result = await get(self._session, f"{self._url}/api/datasets/{dataset}/stored-queries/{query_id}")
        return result.get('columnDescriptions')

    async def get_query(self, dataset, query_id):
        result = await get(self._session, f"{self._url}/api/datasets/{dataset}/stored-queries/{query_id}")
        return result.get('query')

    async def get_stored_query(self, dataset, query_id):
        result = await get(self._session, f"{self._url}/api/datasets/{dataset}/stored-queries/{query_id}")
        return result.get('query')

    async def delete_stored_query(self, dataset, query_id):
        result = await delete(self._session, f"{self._url}/api/datasets/{dataset}/stored-queries/{query_id}")
        return result

    async def get_number_of_results(self, dataset, query_id):
        response = await self.get_query_info(dataset, query_id)
        while not response['status'] == 'DONE':
            response = await self.get_query_info(dataset, query_id)

        n_results = response.get('numberOfResults')
        if n_results is None:
            return -1

        return int(n_results)

    async def get_query_info(self, dataset, query_id):
        result = await get(self._session, f"{self._url}/api/datasets/{dataset}/queries/{query_id}")
        return result

    async def execute_query(self, dataset, query, label=None):
        result = await post(self._session, f"{self._url}/api/datasets/{dataset}/queries", query)

        try:
            if label is not None:
                await patch(self._session, f"{self._url}/api/datasets/{dataset}/stored-queries/{result['id']}",
                            {"label": label})
            return result['id']
        except KeyError:
            raise ValueError("Error encountered when executing query", result.get('message'), result.get('details'))

    async def execute_form_query(self, dataset, form_query):
        result = await post(self._session, f"{self._url}/api/datasets/{dataset}/queries", form_query)
        try:
            return result['id']
        except KeyError:
            raise ValueError("Error encountered when executing query", result.get('message'), result.get('details'))

    async def get_query_result(self, dataset, query_id, requests_per_sec=None):
        """ Returns results for given query.
        Blocks until the query is DONE.

        :param dataset:
        :param query_id:
        :param requests_per_sec: Number of request to do per second (default None -> as many as possible)
        e.g. requests_per_sec = 2 -> sleep 0.5 seconds between requests
        :return: str containing the returned csv's
        """
        response = await self.get_query_info(dataset, query_id)
        while not response['status'] == 'DONE':
            if response['status'] == "FAILED":
                raise Exception(f"Query with {query_id=} failed. Response: \n"
                                f"{response=}")
            response = await self.get_query_info(dataset, query_id)
            if requests_per_sec is None:
                continue
            sleep(1 / requests_per_sec)

        result_string = await self._download_query_results(response["resultUrl"])
        return list(csv.reader(result_string.splitlines(), delimiter=';'))

    async def _download_query_results(self, url):
        return await get_text(self._session, url)

    async def create_concept_query_with_selects(self, dataset: str, concept_id: str, selects: list = None):
        concepts = await self.get_concepts(dataset)

        if selects is None:
            selects = util.selects_per_concept(concepts).get(concept_id)

        concept_query = util.concept_query_from_concept(concept_id, concepts.get(concept_id))
        return util.add_selects_to_concept_query(concept_query, concept_id, selects)
