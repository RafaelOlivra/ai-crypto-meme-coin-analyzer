import pytest
import time

from lib.Utils import Utils

def test_formatted_date():
    timestamp = 1633072801  # Example timestamp
    formatted_date = Utils.formatted_date(timestamp)
    assert formatted_date == "2021-10-01T04:20:01Z"