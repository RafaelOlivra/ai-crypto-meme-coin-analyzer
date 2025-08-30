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
    A class to handle local file caching for URLs, function return values,
    and direct set/get calls.

    TTL is assigned on `set` and checked automatically on `get`.
    Supports JSON and Pickle storage with integrity checks.
    """

    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(LocalCache, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self,
            cache_dir: str = DEFAULT_CACHE_DIR,
            print: list[str] = ['misses', 'errors']
        ):
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
        return os.path.exists(self.cache_disabled_flag)

    def _print(self, msg: str, type: str = "hit") -> None:
        if self.print_hits and type == "hit":
            print(f"[Cache {type.upper()}] {msg}")
        elif self.print_misses and type == "miss":
            print(f"[Cache {type.upper()}] {msg}")
        elif self.print_errors and type == "error":
            print(f"[Cache {type.upper()}] {msg}")

    def _get_file_path(self, key: str, ext: str = "") -> str:
        subfolder = key[:2]
        file_name = key + ext
        path = os.path.join(self.cache_dir, subfolder)
        os.makedirs(path, exist_ok=True)
        return os.path.join(path, file_name)

    def _is_expired(self, expire_at: float) -> bool:
        """Check if given expiration timestamp has passed."""
        return time.time() > expire_at

    def _save_to_cache(self, key: str, result: Any, ttl_s: int) -> None:
        """
        Saves a result with TTL using JSON if possible, otherwise Pickle.

        We wrap the data in {"expire_at": timestamp, "value": actual_data}
        """
        expire_at = time.time() + ttl_s
        wrapped = {"expire_at": expire_at, "value": result}

        json_cache_path = self._get_file_path(key, ext=".json")
        pickle_cache_path = self._get_file_path(key, ext=".pkl")

        try:
            with open(json_cache_path, "w") as f:
                json.dump(wrapped, f)
            self._print(f"Result for key '{key}' cached to {json_cache_path}")
            if os.path.exists(pickle_cache_path):
                os.remove(pickle_cache_path)
        except TypeError:
            try:
                with open(pickle_cache_path, "wb") as f:
                    pickle.dump(wrapped, f)
                if os.path.exists(json_cache_path):
                    os.remove(json_cache_path)
                self._print(f"Result for key '{key}' cached (Pickle) to {pickle_cache_path}")
            except Exception as e:
                self._print(f"Failed to save to cache for key '{key}': {e}", type="error")
        except Exception as e:
            self._print(f"Failed to save to cache for key '{key}': {e}", type="error")

    def _load_from_cache(self, key: str, invalidate_if_return: Any = '__INVALIDATE__') -> Any:
        """
        Attempts to load a cached result (JSON first, then Pickle).
        TTL is checked automatically.
        """
        json_cache_path = self._get_file_path(key, ext=".json")
        pickle_cache_path = self._get_file_path(key, ext=".pkl")

        # Try JSON cache
        if os.path.exists(json_cache_path):
            try:
                with open(json_cache_path, "r") as f:
                    wrapped = json.load(f)
                expire_at = wrapped.get("expire_at", 0)
                if self._is_expired(expire_at):
                    self._print(f"Cache expired for key '{key}' (JSON)", type="miss")
                    os.remove(json_cache_path)
                    return None
                cache_value = wrapped.get("value")
                if invalidate_if_return is not '__INVALIDATE__' and cache_value == invalidate_if_return:
                    self._print(f"Cache invalidated for key '{key}'.", type="miss")
                    os.remove(json_cache_path)
                else:
                    self._print(f"Cache hit (JSON) for key '{key}'", type="hit")
                    return cache_value
            except Exception as e:
                self._print(f"JSON cache file corrupt for key '{key}': {e}", type="error")
                os.remove(json_cache_path)

        # Try Pickle cache
        if os.path.exists(pickle_cache_path):
            try:
                with open(pickle_cache_path, "rb") as f:
                    wrapped = pickle.load(f)
                expire_at = wrapped.get("expire_at", 0)
                if self._is_expired(expire_at):
                    self._print(f"Cache expired for key '{key}' (Pickle)", type="miss")
                    os.remove(pickle_cache_path)
                    return None
                cache_value = wrapped.get("value")
                if invalidate_if_return is not '__INVALIDATE__' and cache_value == invalidate_if_return:
                    self._print(f"Cache invalidated for key '{key}'.", type="miss")
                    os.remove(pickle_cache_path)
                else:
                    self._print(f"Cache hit (Pickle) for key '{key}'", type="hit")
                    return cache_value
            except Exception as e:
                self._print(f"Pickle cache file corrupt for key '{key}': {e}", type="error")
                os.remove(pickle_cache_path)

        return None

    def _cache_handler(self, key: str, ttl_s: int, func: Callable, args: tuple, kwargs: dict, invalidate_if_return: Any = '__INVALIDATE__') -> Any:
        cached_value = self._load_from_cache(key, invalidate_if_return)
        if cached_value is not None:
            return cached_value

        self._print(f"Cache miss for key '{key}', running original function: '{func.__name__}'", type="miss")
        result = func(*args, **kwargs)
        self._save_to_cache(key, result, ttl_s)
        return result

    # ---------------------
    # Direct call interface
    # ---------------------
    def set(self, key: str, value: Any, ttl_s: int = DEFAULT_TTL_SECONDS) -> None:
        """
        Stores a value in the cache with TTL.
        """
        self._save_to_cache(key, value, ttl_s)

    def get(self, key: str) -> Any:
        """
        Retrieves a value from the cache if not expired.
        """
        return self._load_from_cache(key)

    # ---------------------
    # URL interface
    # ---------------------
    def cache_url(self, url: str, ttl_s: int = DEFAULT_TTL_SECONDS) -> str:
        if self._is_cache_disabled():
            return url
        if not url or url.startswith("file://") or url.startswith("http://localhost"):
            return url

        url_hash = hashlib.md5(url.encode()).hexdigest()

        def fetch_url_content():
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                return response.content
            except requests.exceptions.RequestException as e:
                self._print(f"Failed to fetch {url}: {e}", type="error")
                return url
            except Exception as e:
                self._print(f"Unexpected error caching {url}: {e}", type="error")
                return url

        cached_data = self._cache_handler(url_hash, ttl_s, fetch_url_content, (), {})

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

    # ---------------------
    # Decorator interface
    # ---------------------
    def cache(self, ttl_s: int = DEFAULT_TTL_SECONDS, invalidate_if_return: Any = '__INVALIDATE__'):
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

    # ---------------------
    # Cache control
    # ---------------------
    def disable_cache(self):
        open(self.cache_disabled_flag, "a").close()

    def enable_cache(self):
        try:
            os.remove(self.cache_disabled_flag)
        except FileNotFoundError:
            pass

# Initialize global cache handler
cache_handler = LocalCache()
