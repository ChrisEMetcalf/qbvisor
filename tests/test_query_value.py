import pytest
from datetime import date, datetime
from qbvisor.query_value import format_query_value

def test_format_string():
    assert format_query_value("hello") == "'hello'"

def test_format_bool():
    assert format_query_value(True) == "'true'"
    assert format_query_value(False) == "'false'"

def test_format_number():
    assert format_query_value(123) == "123"
    assert format_query_value(123.456) == "123.456"

def test_format_date():
    d = date(2023, 10, 1)
    assert format_query_value(d) == "'2023-10-01'"

def test_format_datetime():
    dt = datetime(2023, 10, 1, 12, 30)
    assert format_query_value(dt).endswith("AM'") or format_query_value(dt).endswith("PM'")  # Check for AM/PM

def test_format_list_of_mixed():
    v = ["a", 1, True]
    out = format_query_value(v)
    # strip outer quotes, split on '; '
    inner = out.strip("'").split("; ")
    assert inner == ["a", "1", "true"]