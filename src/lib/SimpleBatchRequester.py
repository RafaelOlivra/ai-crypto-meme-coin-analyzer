import time
import requests
import threading
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

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
            print(f"[BatchRequest] Request failed for {request_data.get('url')}: {e}")
            request_result = {'error': str(e)}

        request_result = {'id': request_id, 'index': request_index, 'result': request_result}
        if cache_time is not None and request_ok:
            cache_handler.set(cache_hash, request_result, ttl_s=cache_time)
        return request_result

    def run(self, requests_list):
        """
        Executes a list of requests in parallel.
        
        Args:
            requests_list (list): A list of dictionaries, where each dict contains request details.

        Returns:
            list: A list of dictionaries, with each dict containing the request index and its result.
        """
        results = [None] * len(requests_list)
        failed_indices = set()
        
        # Run initial batch
        print(f"[BatchRequest] Starting initial batch of {len(requests_list)} requests.")
        processed_results, newly_failed_indices = self._process_batch(requests_list)
        for res in processed_results:
            results[res['index']] = res
        failed_indices.update(newly_failed_indices)

        # Retry failed requests
        if failed_indices:
            failed_requests = [requests_list[i] for i in failed_indices]
            print(f"[BatchRequest] Retrying {len(failed_indices)} failed requests with indices: {sorted(list(failed_indices))}")
            
            # We must re-map the indices for the retry.
            # Create a mapping from the temporary retry index to the original index.
            original_index_map = {i: original_idx for i, original_idx in enumerate(failed_indices)}
            
            retry_results = self._process_batch(failed_requests, original_index_map)
            for res in retry_results[0]:
                results[res['index']] = res

        return results

    def _process_batch(self, requests_list, original_index_map=None):
        """Helper method to process a batch of requests."""
        processed_results = []
        failed_indices = set()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_index = {
                executor.submit(self._worker, request_data, i): i
                for i, request_data in enumerate(requests_list)
            }
            for future in concurrent.futures.as_completed(future_to_index):
                temp_index = future_to_index[future]
                try:
                    result = future.result()
                    # If we're in a retry, use the original index map
                    original_index = original_index_map.get(temp_index, temp_index) if original_index_map else temp_index
                    result['index'] = original_index # Update the index in the result dict
                    processed_results.append(result)
                except Exception as e:
                    # Log the error and add the original index to the failed set
                    original_index = original_index_map.get(temp_index, temp_index) if original_index_map else temp_index
                    print(f"[BatchRequest] Error processing request for original index {original_index}: {e}")
                    failed_indices.add(original_index)
                    
        return processed_results, failed_indices