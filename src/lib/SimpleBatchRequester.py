import time
import requests
import threading
from concurrent.futures import ThreadPoolExecutor

from services.logger.Logger import _log
from lib.LocalCache import cache_handler
from lib.Utils import Utils

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
        cache_time = request_data.get('cache_time', None)
        cache_hash = Utils.hash(request_data)
        request_ok = False
        
        try:
            method = request_data.get('method', 'GET').lower()
            url = request_data.get('url')
            params = request_data.get('params')
            data = request_data.get('data')
            headers = request_data.get('headers')
            timeout = request_data.get('timeout', 10)
            
            if cache_time != None:
                cached_result = cache_handler.get(cache_hash)
                if cached_result is not None:
                    print(f"[BatchRequest] Cache hit for {request_data.get('url')}")
                    return cached_result

            response = requests.request(
                method,
                url,
                params=params,
                data=data,
                headers=headers,
                timeout=timeout
            )
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            request_result = {
                'status_code': response.status_code,
                'content': response.json() if 'application/json' in response.headers.get('Content-Type', '') else response.text
            }
            request_ok = True
        except requests.exceptions.RequestException as e:
            _log(f"Request failed for {request_data.get('url')}: {e}", level="ERROR")
            request_result = {'error': str(e)}

        request_result = {'id': request_id, 'index': request_index, 'result': request_result}
        if cache_time is not None and request_ok:
            cache_handler.set(cache_hash, request_result, ttl_s=cache_time)
        return request_result

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
        failed_requests = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_index = {
                executor.submit(self._worker, request_data, i): i
                for i, request_data in enumerate(requests_list)
            }
            for future in future_to_index:
                result = future.result()
                try:
                    results[result['index']] = result
                except Exception as e:
                    print(f"[BatchRequest] Error processing result for index {result['index']}: {e}")
                    failed_requests.append(result['index'])

        # Retry failed requests
        if failed_requests:
            print(f"[BatchRequest] Retrying failed requests: {failed_requests}")
            time.sleep(0.2)  # Wait for a moment before retrying
            retry_results = self.run([requests_list[i] for i in failed_requests])
            for retry_result in retry_results:
                results[retry_result['index']] = retry_result

        return results