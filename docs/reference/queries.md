# Queries

`QueryHelper` builds Quickbase query expressions from field labels and safely serializes common
Python values. Pass the resulting expression to `query_records()`, `query_dataframe()`, report or
attachment filters, and other methods that accept `where`.

::: qbvisor.query_helper.QueryHelper
    options:
      members:
        - fid
        - expr
        - and_
        - or_
        - not_
        - eq
        - neq
        - contains
        - not_contains
        - has
        - not_has
        - starts_with
        - not_starts_with
        - less_than
        - less_than_or_equal
        - greater_than
        - greater_than_or_equal
        - before
        - on_or_before
        - after
        - on_or_after
