import os
import hashlib
import time
import requests
import json
import tempfile
from functools import wraps
from typing import Callable, Any
import pickle
import pandas as pd

# --- Configuration ---
DEFAULT_CACHE_DIR = os.path.join(tempfile.gettempdir(), ".cache.temp")
DEFAULT_TTL_SECONDS = 6000

class LocalCache:
    """
    A class to handle local file caching for URLs or function return values.

    This class provides a decorator to easily cache the result of a function
    and a separate method to cache the content of a URL. It uses a hybrid
    approach, first attempting JSON serialization and falling back to Python's
    pickle module for complex or unknown data types. It also incorporates
    an integrity check by including a hash in the cache file name to detect
    tampering.
    """
    
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        """
        Overrides the __new__ method to ensure only one instance is created.
        """
        if cls._instance is None:
            cls._instance = super(LocalCache, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self,
            cache_dir: str = DEFAULT_CACHE_DIR,
            print: list[str] = ['misses', 'errors']
        ):
        """
        Initializes the cache handler and ensures the cache directory exists.
        This will only run on the first instance creation.

        Args:
            cache_dir (str): The directory where cache files will be stored.
            print (list[str]): A list of cache event types to print (e.g., ['hits', 'misses', 'errors']).
        """
        if self._initialized:
            return
        
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)
        self._initialized = True
        self.print_hits = 'hits' in print
        self.print_misses = 'misses' in print
        self.print_errors = 'errors' in print

        self.cache_disabled_flag = os.path.join(self.cache_dir, ".py-local-cache-disabled")
        
    def _is_cache_disabled(self) -> bool:
        """
        Checks if caching is disabled for all instances of the LocalCache class.
        """
        return os.path.exists(self.cache_disabled_flag)

    def _print(self, msg: str, type: str = "hit") -> None:
        """
        Prints a message to the console with a specific type.

        Args:
            msg (str): The message to print.
            type (str): The type of message (e.g., "info", "warning", "error").
        """
        if self.print_hits and type == "hit":
            print(f"[Cache {type.upper()}] {msg}")
        elif self.print_misses and type == "miss":
            print(f"[Cache {type.upper()}] {msg}")
        elif self.print_errors and type == "error":
            print(f"[Cache {type.upper()}] {msg}")

    def _get_file_path(self, key: str, ext: str = "") -> str:
        """
        Generates a full file path for a given cache key with an integrity hash.

        The hash is included as a subfolder to organize the cache, preventing a
        single directory from holding too many files.

        Args:
            key (str): The unique hash key for the cached item.
            ext (str): The file extension (e.g., '.json' or '.pkl').

        Returns:
            str: The full, absolute file path for the cached item.
        """
        subfolder = key[:2]
        file_name = key + ext
        path = os.path.join(self.cache_dir, subfolder)
        os.makedirs(path, exist_ok=True)
        return os.path.join(path, file_name)

    def _is_expired(self, file_path: str, ttl_ms: int) -> bool:
        """
        Checks if a file exists and if it has expired based on its modification time.

        Args:
            file_path (str): The path to the cache file.
            ttl_ms (int): The time-to-live in milliseconds.

        Returns:
            bool: True if the file does not exist or has expired, False otherwise.
        """
        try:
            mod_time_ms = os.path.getmtime(file_path) * 1000
            now_ms = time.time() * 1000
            return (now_ms - mod_time_ms) >= ttl_ms
        except FileNotFoundError:
            return True
        except Exception as e:
            self._print(f"Error checking file expiration for {file_path}: {e}", type="error")
            return True

    def _cache_handler(self, key: str, ttl_s: int, func: Callable, args: tuple, kwargs: dict, invalidate_if_return: Any = '__INVALIDATE__') -> Any:
        """
        Internal method to handle the core caching logic (read, write, expire).

        This method is the central engine for all caching operations, used by
        both the `@cache` decorator and the `cache_url` method. It first
        attempts to load from a JSON cache file, then falls back to a Pickle
        file if it exists, and finally runs the provided function if no valid
        cache is found.

        Args:
            key (str): The unique hash key for the cached item.
            ttl_s (int): The time-to-live in seconds.
            func (Callable): The function to execute on a cache miss.
            args (tuple): The positional arguments for the function.
            kwargs (dict): The keyword arguments for the function.
            invalidate_if_return (Any): If the result matches this value, the cache
                                        is considered invalid.

        Returns:
            Any: The cached or newly computed result of the function.
        """
        ttl_ms = ttl_s * 1000
        json_cache_path = self._get_file_path(key, ext=".json")
        pickle_cache_path = self._get_file_path(key, ext=".pkl")

        # Check for and load from JSON cache
        if os.path.exists(json_cache_path):
            if not self._is_expired(json_cache_path, ttl_ms):
                try:
                    file_hash = os.path.basename(json_cache_path).split('.')[0]
                    if file_hash != key:
                        raise ValueError("File name hash mismatch.")
                    
                    with open(json_cache_path, "r") as f:
                        cache_value = json.load(f)
                        if invalidate_if_return is not '__INVALIDATE__' and cache_value == invalidate_if_return:
                            self._print(f"Cache invalidated for key '{key}'.", type="miss")
                        else:
                            self._print(f"Cache hit (JSON) for key '{key}'", type="hit")
                            return cache_value
                except (IOError, json.JSONDecodeError, ValueError) as e:
                    self._print(f"JSON cache file corrupt or tampered with for key '{key}', re-running: {e}", type="error")
                    os.remove(json_cache_path)

        # Check for and load from Pickle cache
        if os.path.exists(pickle_cache_path):
            if not self._is_expired(pickle_cache_path, ttl_ms):
                try:
                    file_hash = os.path.basename(pickle_cache_path).split('.')[0]
                    if file_hash != key:
                        raise ValueError("File name hash mismatch.")
                    
                    with open(pickle_cache_path, "rb") as f:
                        cache_value = pickle.load(f)
                        if invalidate_if_return is not '__INVALIDATE__' and cache_value == invalidate_if_return:
                            self._print(f"Cache invalidated for key '{key}'.", type="miss")
                        else:
                            self._print(f"Cache hit (Pickle) for key '{key}'", type="hit")
                            return cache_value
                except (IOError, pickle.PickleError, ValueError) as e:
                    self._print(f"Pickle cache file corrupt or tampered with for key '{key}', re-running: {e}", type="error")
                    os.remove(pickle_cache_path)

        self._print(f"Cache miss for key '{key}', running original function: '{func.__name__}'", type="miss")
        result = func(*args, **kwargs)

        try:
            with open(json_cache_path, "w") as f:
                json.dump(result, f)
            self._print(f"Result for key '{key}' cached to {json_cache_path}")
            if os.path.exists(pickle_cache_path):
                os.remove(pickle_cache_path)
        except TypeError:
            try:
                with open(pickle_cache_path, "wb") as f:
                    pickle.dump(result, f)
                if os.path.exists(json_cache_path):
                    os.remove(json_cache_path)
                self._print(f"Result for key '{key}' cached (Pickle) to {pickle_cache_path}")
            except Exception as e:
                self._print(f"Failed to save to cache for key '{key}': {e}", type="error")
        except Exception as e:
            self._print(f"Failed to save to cache for key '{key}': {e}", type="error")

        return result

    def cache_url(self, url: str, ttl_s: int = DEFAULT_TTL_SECONDS) -> str:
        """
        Fetches a URL resource and caches it locally with TTL support.

        This method is a direct analog of the JavaScript 'localCache' function.
        It stores the raw response content to a file. The core caching logic is
        delegated to `_cache_handler`.

        Args:
            url (str): The URL to fetch.
            ttl_s (int): Time-to-live in seconds.

        Returns:
            str: The local file path to the cached resource, or the original URL
                 if caching fails.
        """
        if self._is_cache_disabled():
            return url

        if not url or url.startswith("file://") or url.startswith("http://localhost"):
            return url

        url_hash = hashlib.md5(url.encode()).hexdigest()

        def fetch_url_content():
            """Wrapper function to fetch URL content."""
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                return response.content
            except requests.exceptions.RequestException as e:
                self._print(f"Failed to fetch {url}: {e}", type="error")
                return url
            except Exception as e:
                self._print(f"An unexpected error occurred while caching {url}: {e}", type="error")
                return url

        cached_data = self._cache_handler(url_hash, ttl_s, fetch_url_content, (), {})
        
        # This part is still needed to save the file
        if cached_data != url:
            content_type = ""
            try:
                response = requests.head(url, timeout=5)
                content_type = response.headers.get("Content-Type", "")
            except requests.exceptions.RequestException:
                pass 
                
            ext = ""
            if "image/jpeg" in content_type: ext = ".jpg"
            elif "image/png" in content_type: ext = ".png"
            elif "application/json" in content_type: ext = ".json"
            elif "text/html" in content_type: ext = ".html"
            
            final_cache_path = self._get_file_path(url_hash, ext=ext)
            with open(final_cache_path, "wb") as f:
                f.write(cached_data)
            return final_cache_path
        
        return url

    def cache(self, ttl_s: int = DEFAULT_TTL_SECONDS, invalidate_if_return: Any = '__INVALIDATE__'):
        """
        Decorator to cache the result of a function call.

        Use this by decorating your function with `@cache(ttl_s=TIME_IN_SECONDS)`.

        Args:
            ttl_s (int): Time-to-live for the cache in seconds.
            invalidate_if_return (Any): If the returned cached result matches this value,
                                        the cache will be invalidated.
        """
        def decorator(func: Callable):
            @wraps(func)
            def wrapper(*args, **kwargs):
                if self._is_cache_disabled():
                    return func(*args, **kwargs)

                is_method = '.' in func.__qualname__
                instance_id = None
                cache_args = args
                
                if is_method and args:
                    instance = args[0]
                    if hasattr(instance, 'instance_id'):
                        instance_id = str(instance.instance_id)
                    else:
                        instance_id = "__DEFAULT__"
                    cache_args = args[1:]
                
                key_components = [func.__name__, cache_args, kwargs]
                if instance_id:
                    key_components.append(instance_id)

                args_str = json.dumps(key_components, sort_keys=True)
                key = hashlib.md5(args_str.encode()).hexdigest()

                return self._cache_handler(
                    key=key,
                    ttl_s=ttl_s,
                    func=func,
                    args=args,
                    kwargs=kwargs,
                    invalidate_if_return=invalidate_if_return
                )
            return wrapper
        return decorator

    def disable_cache(self):
        """
        Disables caching for all instances of the LocalCache class.
        This is done by creating a marker file in the cache directory.
        """
        open(self.cache_disabled_flag, "a").close()

    def enable_cache(self):
        """
        Enables caching for all instances of the LocalCache class.
        This is done by removing the marker file.
        """
        try:
            os.remove(self.cache_disabled_flag)
        except FileNotFoundError:
            pass

# Initialize the cache if not already initialized globally
cache_handler = LocalCache()