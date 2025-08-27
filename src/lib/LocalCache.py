import os
import hashlib
import time
import requests
import json
import tempfile
from functools import wraps
from typing import Callable


# --- Configuration ---
DEFAULT_CACHE_DIR = os.path.join(tempfile.gettempdir(), ".cache.temp")
DEFAULT_TTL_SECONDS = 6000

class LocalCache:
    """
    A class to handle local file caching for URLs or function return values.

    This class provides a decorator to easily cache the result of a function
    and a separate method to cache the content of a URL.
    """
    
    # This class variable will hold the single instance
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        """
        Overrides the __new__ method to ensure only one instance is created.
        """
        if cls._instance is None:
            cls._instance = super(LocalCache, cls).__new__(cls)
            # We must initialize the instance here as well
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, cache_dir: str = DEFAULT_CACHE_DIR):
        """
        Initializes the cache handler and ensures the cache directory exists.
        This will only run on the first instance creation.
        """
        if self._initialized:
            return
        
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)
        self._initialized = True

        # Cache disabled flag
        self.cache_disabled_flag = os.path.join(self.cache_dir, ".py-local-cache-disabled")
        
    def _is_cache_disabled(self) -> bool:
        """
        Checks if caching is disabled for all instances of the LocalCache class.
        """
        return os.path.exists(self.cache_disabled_flag)

    def _get_file_path(self, key: str, ext: str = "") -> str:
        """
        Generates a full file path for a given cache key.
        """
        return os.path.join(self.cache_dir, f"{key}{ext}")

    def _is_expired(self, file_path: str, ttl_ms: int) -> bool:
        """
        Checks if a file exists and if it has expired based on its modification time.
        """
        try:
            mod_time_ms = os.path.getmtime(file_path) * 1000
            now_ms = time.time() * 1000
            return (now_ms - mod_time_ms) >= ttl_ms
        except FileNotFoundError:
            return True
        except Exception as e:
            print(f"Error checking file expiration for {file_path}: {e}")
            return True

    def cache_url(self, url: str, ttl_s: int = DEFAULT_TTL_SECONDS) -> str:
        """
        Fetches a URL resource and caches it locally with TTL support.

        This method is a direct analog of the JavaScript 'localCache' function.
        It stores the raw response content to a file.

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

        ttl_ms = ttl_s * 1000
        url_hash = hashlib.md5(url.encode()).hexdigest()
        cache_file_path = self._get_file_path(url_hash)

        if not self._is_expired(cache_file_path, ttl_ms):
            return cache_file_path
        
        if os.path.exists(cache_file_path):
            try:
                os.remove(cache_file_path)
            except Exception as e:
                print(f"Failed to delete expired cache file: {e}")

        try:
            print(f"Fetching and caching URL: {url}")
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            content_type = response.headers.get("Content-Type", "")
            ext = ""
            if "image/jpeg" in content_type:
                ext = ".jpg"
            elif "image/png" in content_type:
                ext = ".png"
            elif "application/json" in content_type:
                ext = ".json"
            elif "text/html" in content_type:
                ext = ".html"
            
            final_cache_path = cache_file_path + ext
            with open(final_cache_path, "wb") as f:
                f.write(response.content)

            return final_cache_path
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch {url}: {e}")
            return url
        except Exception as e:
            print(f"An unexpected error occurred while caching {url}: {e}")
            return url
            
    def cache(self, ttl_s: int = DEFAULT_TTL_SECONDS):
        """
        Wrapper function that caches the result of a function call.
        Use this by decorating your function with @cache(ttl_s=TIME_IN_SECONDS).
        Example:
            @cache(ttl_s=60)
            def fetch_data():
                # Expensive operation
                return {"data": 123}

        Args:
            ttl_s (int): Time-to-live for the cache in seconds.
        """
        ttl_ms = ttl_s * 1000
        
        def decorator(func: Callable):
            @wraps(func)
            def wrapper(*args, **kwargs):
                
                if self._is_cache_disabled():
                    return
        
                is_method = '.' in func.__qualname__
                instance_id = None
                cache_args = args
                
                if is_method and args:
                    instance = args[0]
                    # Check for a persistent instance ID first
                    if hasattr(instance, 'instance_id'):
                        instance_id = str(instance.instance_id)
                    # Fallback to a non-persistent ID
                    else:
                        instance_id = "__DEFAULT__"

                    cache_args = args[1:]
                
                # The rest of the logic remains unchanged.
                key_components = [func.__name__, cache_args, kwargs]
                if instance_id:
                    key_components.append(instance_id)

                args_str = json.dumps(key_components, sort_keys=True)
                key = hashlib.md5(args_str.encode()).hexdigest()
                cache_file_path = self._get_file_path(key, ext=".json")

                if not self._is_expired(cache_file_path, ttl_ms):
                    try:
                        print(f"Cache hit for function '{func.__name__}' on instance {instance_id}")
                        with open(cache_file_path, "r") as f:
                            return json.load(f)
                    except (IOError, json.JSONDecodeError) as e:
                        print(f"Error reading from cache, re-running function: {e}")

                print(f"Cache miss for function '{func.__name__}' on instance {instance_id}, running original function...")
                result = func(*args, **kwargs)

                try:
                    with open(cache_file_path, "w") as f:
                        json.dump(result, f)
                    print(f"Function result cached to {cache_file_path}")
                except Exception as e:
                    print(f"Failed to save to cache: {e}")

                return result
            return wrapper
        return decorator

    def disable_cache(self):
        """
        Disables caching for all instances of the LocalCache class.
        We add a .py-local-cache-disabled in the temp cache dir to mark it as disabled.
        """
        open(self.cache_disabled_flag, "a").close()

    def enable_cache(self):
        """
        Enables caching for all instances of the LocalCache class.
        We remove the .py-local-cache-disabled marker from the temp cache dir.
        """
        try:
            os.remove(self.cache_disabled_flag)
        except FileNotFoundError:
            pass

# Initialize the cache if not already initialized globally
cache_handler = LocalCache()