import requests
import threading
from concurrent.futures import ThreadPoolExecutor

from services.logger.Logger import _log

class SimpleBatchRequester:
    def __init__(self, max_workers=5):
        """
        Initializes the SimpleBatchRequester.

        Args:
            max_workers (int): The maximum number of threads to use.
        """
        self.max_workers = max_workers

    def _worker(self, request_data, request_index):
        """
        A worker function for a single HTTP request.
        Args:
            request_data (dict): A dictionary containing request details (url, method, params, etc.).
            request_index (int): The original index of the request in the input list.

        Returns:
            dict: A dictionary containing the request index and the result.
        """
        request_id = request_data.get('id') or request_index
        try:
            method = request_data.get('method', 'GET').lower()
            url = request_data.get('url')
            params = request_data.get('params')
            data = request_data.get('data')
            headers = request_data.get('headers')
            timeout = request_data.get('timeout', 10)

            response = requests.request(
                method,
                url,
                params=params,
                data=data,
                headers=headers,
                timeout=timeout
            )
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            result = {
                'status_code': response.status_code,
                'content': response.json() if 'application/json' in response.headers.get('Content-Type', '') else response.text
            }
        except requests.exceptions.RequestException as e:
            _log(f"Request failed for {request_data.get('url')}: {e}", level="ERROR")
            result = {'error': str(e)}

        return {'id': request_id, 'index': request_index, 'result': result}

    def run(self, requests_list):
        """
        Executes a list of requests in parallel.
        
        Example:
            requests_to_make = [
                {'id': 1, 'url': 'https://httpbin.org/get', 'params': {'id': 1}},
                {'id': 2, 'url': 'https://httpbin.org/delay/3'},  # This will take 3 seconds
                {'id': 3, 'url': 'https://httpbin.org/get', 'params': {'id': 3}},
                {'id': 4, 'url': 'https://httpbin.org/status/404'} # This will fail
            ]
            responses = requester.run(requests_to_make)

        Args:
            requests_list (list): A list of dictionaries, where each dict contains request details.

        Returns:
            list: A list of dictionaries, with each dict containing the request index and its result.
                  Example: [{'index': 0, 'id': 1, 'result': {...}}, ...]
        """
        results = [None] * len(requests_list)
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_index = {
                executor.submit(self._worker, request_data, i): i
                for i, request_data in enumerate(requests_list)
            }
            for future in future_to_index:
                result = future.result()
                results[result['index']] = result

        return results