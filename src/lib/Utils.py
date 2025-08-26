import re
import requests
import json
import hashlib

from datetime import datetime, date, timedelta

from services.AppData import AppData
from services.logger.Logger import _log

# --- Configuration ---
DATETIME_DISPLAY_FORMAT = AppData().get_config("datetime_display_format")
TIME_DISPLAY_FORMAT = AppData().get_config("time_display_format")

class Utils:
    
    # --------------------------
    # URL Utils
    # --------------------------
    
    @staticmethod
    def slugify(string: str) -> str:
        """
        Convert a given string to a URL-friendly 'slug'.

        Steps:
        - Convert to lowercase and replace spaces with hyphens.
        - Replace accented characters with ASCII equivalents.
        - Remove non-alphanumeric characters, keeping only letters, numbers, and hyphens.
        - Remove leading/trailing hyphens and reduce multiple hyphens to one.

        Args:
            string (str): Input string to be slugified.

        Returns:
            str: Slugified version of the input string.
        """
        # Convert to lowercase and replace spaces with hyphens
        slug = string.lower().replace(" ", "-")

        # Replace accented characters with their ASCII equivalents
        accents_mapping = {
            r"[àáâãäå]": "a",
            r"[èéêë]": "e",
            r"[ìíîï]": "i",
            r"[òóôõö]": "o",
            r"[ùúûü]": "u",
            r"[ñ]": "n",
            r"[ç]": "c",
        }
        for pattern, replacement in accents_mapping.items():
            slug = re.sub(pattern, replacement, slug)

        # Remove any characters that are not alphanumeric or hyphens
        slug = re.sub(r"[^a-z0-9-]", "", slug)

        # Reduce consecutive hyphens to a single hyphen
        slug = re.sub(r"-+", "-", slug)

        # Strip hyphens from the beginning and end of the string
        return slug.strip("-")

    @staticmethod
    def time_slugify(time) -> str:
        """
        Create a URL-friendly slug from a datetime object.

        Args:
            dt (datetime): The datetime object to slugify.

        Returns:
            str: The slugified datetime string.
        """
        formatted = Utils.formatted_date(time)
        return "T" + formatted.replace(":", "").replace("-", "").replace("T", "").replace("Z", "").lower()

    @staticmethod
    def url_encode(text: str) -> str:
        """
        Encode a text string for use in a URL.

        Args:
            text (str): The text to encode.

        Returns:
            str: The URL-encoded text.

        Raises:
            ValueError: If the input is not a string.
        """
        if not isinstance(text, str):
            raise ValueError("Input must be a string")
        return requests.utils.quote(text)
    
    # --------------------------
    # JSON Utils
    # --------------------------
        
    @staticmethod
    def is_json(data: str) -> bool:
        """
        Check if a given string is valid JSON.

        Args:
            data (str): The string to check.

        Returns:
            bool: True if the string is valid JSON, False otherwise.
        """
        try:
            json.loads(data)
            return True
        except ValueError:
            return False

    @staticmethod
    def flatten_json_to_string(cell, parent_key: str = '', sep: str = '.') -> str:
        """
        Flatten a nested JSON-like cell into grouped 'key: value' strings.

        Args:
            cell (dict | list | any): The JSON object or list to flatten.
            parent_key (str, optional): Prefix key (used for nested dicts). Defaults to ''.
            sep (str, optional): Separator between nested keys. Defaults to '.'.

        Returns:
            str: Flattened string representation.
        """
        items = []

        if isinstance(cell, dict):
            for k, v in cell.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, (dict, list)):
                    items.append(Utils.flatten_json_to_string(v, new_key, sep))
                else:
                    items.append(f"{new_key}: {v}")

        elif isinstance(cell, list):
            groups = []
            for item in cell:
                if isinstance(item, dict):
                    groups.append(flatten_json_to_string(item, parent_key, sep))
                else:
                    groups.append(str(item))
            return ", ".join(groups)

        else:
            return f"{parent_key}: {cell}" if parent_key else str(cell)

        return ", ".join(items)


    # --------------------------
    # Date Utils
    # --------------------------
    
    @staticmethod
    def formatted_date(
            _date: date | datetime | str | int | None = None,
            format: str = "",
            delta_seconds: int = 0
        ) -> str:
        """
        Get the current date in a specific format.

        Args:
            _date (date | datetime | str | int | None): The date to format.
            format (str, optional): The format to use. Defaults to "".
            delta_seconds (int, optional): A time delta in seconds to add to the date. Defaults to 0.

        Returns:
            str: The current date as a formatted string.
        """
        if not _date:
            _date = datetime.now()
            
        _date = Utils.to_datetime(_date, format=format)

        if delta_seconds:
            _date += timedelta(seconds=delta_seconds)

        return Utils.to_date_string(_date, format=format)

    @staticmethod
    def to_date_string(_date: date | datetime | str | int, format="") -> str:
        """
        Convert a date object to a string.

        Args:
            _date (date | datetime | str): The date object to convert.
            format (str, optional): The format to convert to. Defaults to "".
        Returns:
            str: The date string in the specified format.
        """ 
        # Convert string to datetime object from isoformat
        if isinstance(_date, str):
            _date = datetime.fromisoformat(_date)

        # If it's an int, convert to datetime (assuming it's a timestamp)
        if isinstance(_date, int):
            _date = datetime.fromtimestamp(_date)

        # If it's a date (not datetime), convert to datetime at midnight
        if isinstance(_date, date) and not isinstance(_date, datetime):
            _date = datetime.combine(_date, datetime.min.time())

        if format == "display":
            return _date.strftime(DATETIME_DISPLAY_FORMAT)
        elif format == "iso_date_only":
            return _date.strftime("%Y-%m-%d")
        elif format != "":
            return _date.strftime(format)
        else: # iso
            return _date.strftime("%Y-%m-%dT%H:%M:%SZ")

    @staticmethod
    def to_date_string_recursive(items: list | dict, format="") -> list | dict:
        """
        Convert all date or time objects in a list or dictionary to strings.

        Args:
            items (list | dict): The list or dictionary to convert.
            format (str, optional): The format to convert to. Defaults to "".

        Returns:
            (list | dict): The list or dictionary with date or datetime objects converted to strings.
        """
        # Ignore objects that are not lists or dictionaries
        if not (isinstance(items, list) or isinstance(items, dict)):
            return items

        for item in items:
            if isinstance(item, dict) or isinstance(item, list):
                item = Utils.to_date_string_recursive(items=item, format=format)
            else:
                # Convert date objects to strings
                if isinstance(items[item], datetime) or isinstance(items[item], date):
                    items[item] = Utils.to_date_string(items[item], format=format)

                # Convert time objects to strings
                if "datetime.time" in str(type(items[item])):
                    items[item] = Utils.to_time_string(items[item])
        return items

    @staticmethod
    def to_time_string(_time: datetime | str) -> str:
        """
        Convert a time object to a string.
        If the input is a string, it is assumed to be in iso format.

        Args:
            _time (datetime | str): The time object to convert.
                    The format can be set with 'time_display_format' the config file.

        Returns:
            str: The formatted time string.
        """
        # Convert string to datetime object from isoformat
        if isinstance(_time, str):
            _time = datetime.fromisoformat(_time)

        return str(_time.strftime(TIME_DISPLAY_FORMAT))

    @staticmethod
    def to_datetime(_date: str | date | datetime | int, format: str = "") -> datetime:
        """
        Convert a date string to a datetime object.
        By default, the date is assumed to be in iso format.
        If the conversion fails, the date is assumed to be in the 'datetime_display_format' config format.

        Args:
            _date (str | date | datetime | int): The date to convert.
            format (str, optional): The format to use for parsing the date. Defaults to "".

        Returns:
            datetime: The datetime object.
        """
        # If already a datetime object, return as is
        if isinstance(_date, datetime):
            return _date
        
        # If it's an integer, convert it to a datetime object
        if isinstance(_date, int):
            return datetime.fromtimestamp(_date)

        # Attempt to convert from isoformat
        try:
            if isinstance(_date, str):
                if format == "":
                    _date = datetime.fromisoformat(_date)
                else:
                    _date = datetime.strptime(_date, format)

            elif isinstance(_date, date):
                _date = datetime.combine(_date, datetime.min.time())

        # Attempt to convert from display format
        except ValueError:
            _date = datetime.strptime(
                str(_date), DATETIME_DISPLAY_FORMAT
            )

        return _date
    
    @staticmethod
    def get_days_since(_date: str | date | datetime | int, format: str = "") -> int:
        """
        Get the number of days since a given date.

        Args:
            _date (str | date): The date string to compare.
            format (str, optional): The format to use for parsing the date. Defaults to "".

        Returns:
            int: The number of days since the given date.
        """
        date = Utils.to_datetime(_date, format=format)
        if not date:
            return 0
        return (datetime.now() - date).days

    # --------------------------
    # Hash Utils
    # --------------------------
    
    @staticmethod
    def hash(input):
        """
        Hashes the input using SHA-256, supporting various data types.
        
        The function serializes the input (string, object, list, or dict)
        into a consistent JSON string before hashing.
        
        Args:
            input (any): The input to hash. Can be a string, list, dictionary,
                        or other JSON-serializable object.
        
        Returns:
            str: The hexadecimal representation of the hash.
        """
        if isinstance(input, str):
            # Already a string, just encode it
            data_to_hash = input.encode('utf-8')
        elif isinstance(input, (list, dict)):
            # For lists and dictionaries, serialize them to a canonical JSON string
            # using sort_keys=True to ensure consistent hashing regardless of order.
            data_to_hash = json.dumps(input, sort_keys=True).encode('utf-8')
        else:
            # For other JSON-serializable objects (like ints, floats, booleans)
            # or for simple objects, convert to a string and encode.
            try:
                data_to_hash = json.dumps(input).encode('utf-8')
            except TypeError:
                # Fallback for non-JSON-serializable objects. This might not
                # produce consistent hashes for complex, un-serializable objects.
                data_to_hash = str(input).encode('utf-8')

        # Create the SHA-256 hash object and return the hexdigest
        return hashlib.sha256(data_to_hash).hexdigest()
