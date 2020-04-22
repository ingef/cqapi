from aiohttp import ClientSession
from aiohttp import ClientConnectorError
from cqapi import util
import csv


class CqApiError(BaseException):
    pass


class ConqueryClientConnectionError(CqApiError):
    def __init__(self, msg):
        self.message = msg


async def get(session, url, token):
    headers = {'Authorization': f'Bearer {token}'}
    async with session.get(url, headers=headers) as response:
        return await response.json()


async def get_text(session, url, token):
    headers = {'Authorization': f'Bearer {token}'}
    async with session.get(url, headers=headers) as response:
        return await response.text()


async def post(session, url, data, token):
    headers = {'Authorization': f'Bearer {token}'}
    async with session.post(url, headers=headers, json=data) as response:
        return await response.json()


async def patch(session, url, data, token):
    headers = {'Authorization': f'Bearer {token}'}
    async with session.patch(url, headers=headers, json=data) as response:
        return await response.json()


async def delete(session, url, token):
    headers = {'Authorization': f'Bearer {token}'}
    async with session.delete(url, headers=headers) as response:
        return await response.text()


class ConqueryConnection(object):
    async def __aenter__(self):
        self._session = ClientSession()
        # try to fail early if conquery is not available at self._url
        if self._check_connection:
            try:
                await get(self._session, f"{self._url}/api/datasets", self._token)
            except ClientConnectorError:
                error_msg = f"Could not connect to Conquery, are you sure {self._url} is the right address?"
                raise ConqueryClientConnectionError(error_msg)
        # Check if token is known to conquery and if it has access to any dataset
        if self._check_permission:
            headers = {'Authorization': f'Bearer {self._token}'}
            async with self._session.get(f"{self._url}/api/datasets", headers=headers) as response:
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

    async def get_datasets(self):
        response_list = await get(self._session, f"{self._url}/api/datasets", self._token)
        return [d['id'] for d in response_list]

    async def get_concepts(self, dataset):
        response = await get(self._session, f"{self._url}/api/datasets/{dataset}/concepts", self._token)
        return response['concepts']

    async def get_concept(self, dataset, concept_id):
        response_dict = await get(self._session, f"{self._url}/api/datasets/{dataset}/concepts/{concept_id}",
                                  self._token)
        response_list = [dict(attrs, **{"ids": [c_id]}) for c_id, attrs in response_dict.items()]
        return response_list

    async def get_stored_queries(self, dataset):
        response_list = await get(self._session, f"{self._url}/api/datasets/{dataset}/stored-queries", self._token)
        return response_list

    async def get_column_descriptions(self, dataset, query_id):
        result = await get(self._session, f"{self._url}/api/datasets/{dataset}/stored-queries/{query_id}", self._token)
        return result.get('columnDescriptions')

    async def get_stored_query(self, dataset, query_id):
        result = await get(self._session, f"{self._url}/api/datasets/{dataset}/stored-queries/{query_id}", self._token)
        return result.get('query')

    async def delete_stored_query(self, dataset, query_id):
        result = await delete(self._session, f"{self._url}/api/datasets/{dataset}/stored-queries/{query_id}",
                              self._token)
        return result

    async def get_query(self, dataset, query_id):
        result = await get(self._session, f"{self._url}/api/datasets/{dataset}/queries/{query_id}", self._token)
        return result

    async def execute_query(self, dataset, query, label=None):
        result = await post(self._session, f"{self._url}/api/datasets/{dataset}/queries", query, self._token)
        try:
            if label is not None:
                await patch(self._session, f"{self._url}/api/datasets/{dataset}/stored-queries/{result['id']}",
                            {"label": label}, self._token)
            return result['id']
        except KeyError:
            raise ValueError("Error encountered when executing query", result.get('message'), result.get('details'))

    async def execute_form_query(self, dataset, form_query):
        result = await post(self._session, f"{self._url}/api/datasets/{dataset}/queries", form_query, self._token)
        try:
            return result['id']
        except KeyError:
            raise ValueError("Error encountered when executing query", result.get('message'), result.get('details'))

    async def get_query_result(self, dataset, query_id):
        """ Returns results for given query.
        Blocks until the query is DONE.

        :param dataset:
        :param query_id:
        :return: str containing the returned csv's
        """
        response = await self.get_query(dataset, query_id)
        while not response['status'] == 'DONE':
            response = await self.get_query(dataset, query_id)

        result_string = await self._download_query_results(response["resultUrl"])
        return list(csv.reader(result_string.splitlines(), delimiter=';'))

    async def _download_query_results(self, url):
        return await get_text(self._session, url, self._token)

    async def create_concept_query_with_selects(self, dataset: str, concept_id: str, selects: list = None):
        concepts = await self.get_concepts(dataset)

        if selects is None:
            selects = util.selects_per_concept(concepts).get(concept_id)

        concept_query = util.concept_query_from_concept(concept_id, concepts.get(concept_id))
        return util.add_selects_to_concept_query(concept_query, concept_id, selects)
