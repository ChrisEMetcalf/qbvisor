from datetime import date, datetime

# Private formatting constants
_DATE_PATTERN = "%Y-%m-%d"
_DATETIME_PATTERN = "%Y-%m-%d %I:%M%p"

def format_query_value(value) -> str:
    """
    Format a Python value into a Quickbase-safe query string.
    
    Automatically handles quoting, dates, booleans, numbers, and lists.
    """

    if isinstance(value, datetime):
        return f"'{value.strftime(_DATETIME_PATTERN)}'"
    if isinstance(value, date):
        return f"'{value.strftime(_DATE_PATTERN)}'"
    if isinstance(value, bool):
        return "'true'" if value else "'false'"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        elements = "; ".join(_strip_quotes(format_query_value(v)) for v in value)
        return f"'{elements}'"
    
    # Default: treat as string
    return f"'{str(value)}'"

def _strip_quotes(text: str) -> str:
    """
    Helper to remove outer single quotes if they exist.
    """
    if text.startswith("'") and text.endswith("'"):
        return text[1:-1]
    return text