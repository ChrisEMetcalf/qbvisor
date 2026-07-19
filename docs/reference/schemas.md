# Declarative schema models

These immutable models describe desired application state and the plan produced from it. Start
with the [declarative schema guide](../declarative-schemas.md) before using the lower-level state
and plan models directly.

::: qbvisor.schema
    options:
      members:
        - AppSpec
        - TableSpec
        - FieldSpec
        - FormulaSpec
        - RelationshipSpec
        - SummaryFieldSpec
        - SchemaState
        - StateResource
        - SchemaPlan
        - SchemaChange
        - SchemaAttributeChange
        - SchemaApplyResult
        - SchemaAction
        - SchemaStateAction
        - SchemaResourceKind
        - FormulaFieldType
