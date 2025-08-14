from datetime import datetime
import pytest
import time

from lib.Utils import Utils
from services.log.Logger import _log

def test_formatted_date():
    # Test with a timestamp
    timestamp = 1633072801
    formatted_date = Utils.formatted_date(timestamp)
    assert formatted_date == "2021-10-01T04:20:01Z"

    # Test with an already formatted date string
    expected_date = '2025-08-14T15:30:39Z'
    formatted_date = Utils.formatted_date('2025-08-14T15:30:39Z')
    assert formatted_date == expected_date

    # Test with a datetime object
    dt = datetime(2025, 8, 14, 15, 30, 39)
    formatted_date = Utils.formatted_date(dt)
    assert formatted_date == expected_date
    
    # Test with time delta
    delta_seconds = -3600  # 1 hour
    formatted_date = Utils.formatted_date(dt, delta_seconds=delta_seconds)
    assert formatted_date == "2025-08-14T14:30:39Z"