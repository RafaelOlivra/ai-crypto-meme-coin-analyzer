import re
import requests
import json

from datetime import datetime, date

from services.AppData import AppData


class Utils:
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

    @staticmethod
    def to_date_string(_date: date | datetime | str, format="") -> str:
        """
        Convert a date object to a string.

        Args:
            _date (date | datetime | str): The date object to convert.
            format (str, optional): The format to convert to. Defaults to "".
                (Currently supports 'display' and 'iso_date_only')
        Returns:
            str: The date string in the specified format.
        """
        # Convert string to datetime object from isoformat
        if isinstance(_date, str):
            _date = datetime.fromisoformat(_date)

        # Convert date to datetime object
        if isinstance(_date, date):
            _date = datetime.combine(_date, datetime.min.time())

        if format == "display":
            return str(_date.strftime(AppData().get_config("datetime_display_format")))
        elif format == "iso_date_only":
            return _date.strftime("%Y-%m-%d")
        else:
            # Return isoformat by default
            return _date.isoformat()

    @staticmethod
    def to_date_string_recursive(items: list | dict, format="") -> list | dict:
        """
        Convert all date or time objects in a list or dictionary to strings.

        Args:
            items (list | dict): The list or dictionary to convert.
            format (str, optional): The format to convert to. Defaults to "".
                (Currently supports 'display' and 'iso_date_only')

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

        return str(_time.strftime(AppData().get_config("time_display_format")))

    @staticmethod
    def to_datetime(_date: str | date) -> datetime:
        """
        Convert a date string to a datetime object.
        By default, the date is assumed to be in iso format.
        If the conversion fails, the date is assumed to be in the 'datetime_display_format' config format.

        Args:
            _date (str | date): The date string to convert.

        Returns:
            datetime: The datetime object.
        """
        # Attempt to convert from isoformat
        try:
            # If we have a string we convert it to a datetime object
            if isinstance(_date, str):
                _date = datetime.fromisoformat(_date)

            # If we have a date object we convert it to a datetime object
            if isinstance(_date, date):
                _date = datetime.combine(_date, datetime.min.time())

        # Attempt to convert from display format
        except ValueError:
            _date = datetime.strptime(
                _date, AppData().get_config(f"datetime_display_format")
            )

        return _date
