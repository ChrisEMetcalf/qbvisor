from typing import Any

from .query_value import format_query_value

SUPPORTED_OPERATORS = {
    "CT", "XCT", "HAS", "XHAS", "EX", "XEX",
    "TV", "XTV", "SW", "XSW",
    "BF", "OBF", "AF", "OAF",
    "IR", "XIR", "LT", "LTE", "GT", "GTE"
}

class QueryHelper:
    """
    Build Quickbase formula queries using field labels, not field IDs.
    Ensures correct serialization of values and operator validation.

    Supported Operators:
    CT, XCT, HAS, XHAS, EX, XEX, TV, XTV,
    SW, XSW, BF, OBF, AF, OAF, IR, XIR, LT, LTE, GT, GTE
    """

    def __init__(self, client: Any, app_name: str, table_name: str):
        self.client = client
        self.app_name = app_name
        self.table_name = table_name
        self.field_map = client.meta.get_field_map(app_name, table_name)

    def fid(self, label: str) -> str:
        """
        Resolve a field label to its Quickbase field ID (fid).
        """
        if label not in self.field_map:
            raise ValueError(f"Field label '{label}' not found in table '{self.table_name}'.")
        return str(self.field_map[label]['id'])

    def expr(self, field_label: str, operator: str, value: Any) -> str:
        """
        Build a formula query expression.

        Example:
            {6.EX.'true'}

        Args:
            field_label: Friendly field name
            operator: Quickbase operator code (e.g., EX, XEX, CT, etc.)
            value: The value to match against (date, string, boolean, list, etc.)

        Returns:
            A Quickbase formula query expression.
        """
        if operator not in SUPPORTED_OPERATORS:
            raise ValueError(f"Operator '{operator}' is not supported. Must be one of: {sorted(SUPPORTED_OPERATORS)}")

        field_id = self.fid(field_label)
        serialized_val = format_query_value(value)
        return f"{{{field_id}.{operator}.{serialized_val}}}"

    def and_(self, *expressions: str) -> str:
        """
        Combine multiple expressions with logical AND.
        """
        return "AND".join(expressions)

    def or_(self, *expressions: str) -> str:
        """
        Combine multiple expressions with logical OR.
        """
        return "OR".join(expressions)

    def not_(self, *expressions: str) -> str:
        """
        Logical NOT.
        """
        return "NOT".join(expressions)
    
    # -------------------------------
    # Convenience Methods
    # -------------------------------
    def eq(self, field_label: str, value: Any) -> str:
        """
        Field equals value.
        """
        return self.expr(field_label, "EX", value)
    
    def neq(self, field_label: str, value: Any) -> str:
        """
        Field not equals value.
        """
        return self.expr(field_label, "XEX", value)
    
    def contains(self, field_label: str, value: Any) -> str:
        """
        Field contains value.
        """
        return self.expr(field_label, "CT", value)
    
    def not_contains(self, field_label: str, value: Any) -> str:
        """
        Field does not contain value.
        """
        return self.expr(field_label, "XCT", value)
    
    def has(self, field_label: str, value: Any) -> str:
        """
        Field has value (for list-user or multi-select fields).
        """
        return self.expr(field_label, "HAS", value)
    
    def not_has(self, field_label: str, value: Any) -> str:
        """
        Field does not have value (for list-user or multi-select fields).
        """
        return self.expr(field_label, "XHAS", value)
    
    def starts_with(self, field_label: str, value: Any) -> str:
        """
        Field starts with value.
        """
        return self.expr(field_label, "SW", value)
    
    def not_starts_with(self, field_label: str, value: Any) -> str:
        """
        Field does not start with value.
        """
        return self.expr(field_label, "XSW", value)
    
    def less_than(self, field_label: str, value: Any) -> str:
        """
        Field less than value.
        """
        return self.expr(field_label, "LT", value)
    
    def less_than_or_equal(self, field_label: str, value: Any) -> str:
        """
        Field less than or equal to value.
        """
        return self.expr(field_label, "LTE", value)
    
    def greater_than(self, field_label: str, value: Any) -> str:
        """
        Field greater than value.
        """
        return self.expr(field_label, "GT", value)
    
    def greater_than_or_equal(self, field_label: str, value: Any) -> str:
        """
        Field greater than or equal to value.
        """
        return self.expr(field_label, "GTE", value)
    
    def before(self, field_label: str, value: Any) -> str:
        """
        Field date is before value.
        """
        return self.expr(field_label, "BF", value)
    
    def on_or_before(self, field_label: str, value: Any) -> str:
        """
        Field date is on or before value.
        """
        return self.expr(field_label, "OBF", value)
    
    def after(self, field_label: str, value: Any) -> str:
        """
        Field date is after value.
        """
        return self.expr(field_label, "AF", value)
    
    def on_or_after(self, field_label: str, value: Any) -> str:
        """
        Field date is on or after value.
        """
        return self.expr(field_label, "OAF", value)