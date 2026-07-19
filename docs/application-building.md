# Building Quickbase applications

qbvisor supports direct resource methods and a declarative plan-and-apply workflow. Both use the
same transport, metadata resolution, structured exceptions, and mutation retry rules.

## Choose a workflow

| Situation | Recommended workflow |
| --- | --- |
| Add one known table or field to an existing app | Direct client method |
| Inspect or extend one existing relationship | Direct client method |
| Create a complete application or maintain desired state | Declarative schema |
| Review exact effects before changing shared infrastructure | Declarative schema |
| Delete resources | Explicit direct method after independent review |

Declarative apply is intentionally non-destructive. It creates and updates managed resources but
does not delete objects omitted from the specification.

## Direct application and table methods

Create a table and fields in an application already configured in `QB_APP_IDS`:

```python
from qbvisor import QuickBaseClient

with QuickBaseClient() as qb:
    qb.create_table(
        "Development Sandbox",
        "Invoices",
        description="Invoices received from the billing integration",
        singular_record_name="Invoice",
        plural_record_name="Invoices",
    )
    qb.create_field("Development Sandbox", "Invoices", "Invoice Number", "text")
    qb.create_field("Development Sandbox", "Invoices", "Amount", "currency")
    qb.create_field("Development Sandbox", "Invoices", "Status", "text")
```

Successful table and field mutations invalidate affected metadata. Later calls on the same client
observe the new schema.

`create_app()` returns the new application metadata, but the client's `QB_APP_IDS` mapping is
static. Add the returned ID to configuration and create a new client before calling name-resolved
methods against that app. Use declarative creation when the complete app must be built in one
workflow.

Direct deletion methods are available for applications, tables, fields, relationships, records,
and attachment versions. They are irreversible Quickbase mutations and are not part of declarative
apply.

## Declarative plan and apply

An `AppSpec` gives every resource a stable key separate from its Quickbase display name:

```python
from qbvisor import AppSpec, FieldSpec, FormulaSpec, TableSpec

spec = AppSpec(
    key="billing",
    name="Billing",
    tables=[
        TableSpec(
            key="invoices",
            name="Invoices",
            fields=[
                FieldSpec(key="number", label="Invoice Number", field_type="text"),
                FieldSpec(key="quantity", label="Quantity", field_type="numeric"),
                FieldSpec(key="rate", label="Rate", field_type="currency"),
                FieldSpec(
                    key="amount",
                    label="Amount",
                    field_type="currency",
                    formula=FormulaSpec(
                        expression="[Quantity] * [Rate]",
                        depends_on=(
                            "tables.invoices.fields.quantity",
                            "tables.invoices.fields.rate",
                        ),
                    ),
                ),
            ],
        )
    ],
)
```

Planning observes Quickbase but does not mutate Quickbase or local state:

```python
with QuickBaseClient() as qb:
    plan = qb.plan_app(spec, state_path=".qbvisor/state.json")
    print(plan)

    if not plan.can_apply:
        raise RuntimeError("Resolve schema conflicts before apply")

    result = qb.apply_app(plan)
```

Apply locks the state path, observes Quickbase again, and rejects a stale plan before mutation. It
then applies the reviewed changes, verifies convergence, and atomically publishes state. A second
plan should show no Quickbase changes or pending bindings.

The state file contains stable resource addresses and Quickbase IDs. It does not contain tokens or
record data, but it must be persisted and protected because it distinguishes renames from resource
replacement.

## Formula fields

`FormulaSpec.expression` contains native Quickbase formula syntax. qbvisor does not parse or
reimplement the formula language; Quickbase validates references, return types, and evaluation.

`depends_on` declares stable schema addresses that must exist before the formula field is created.
Declare formula-to-formula dependencies and dependencies on relationship-generated fields.
qbvisor does not infer them from bracketed formula text because brackets can also occur in strings,
comments, HTML, and embedded queries.

Formula queries receive a plan warning because filtering, sorting, or grouping on them can affect
application-wide performance.

## Relationships

Add lookup and summary fields to an existing relationship with labels or numeric IDs:

```python
from qbvisor import RelationshipSummary

with QuickBaseClient() as qb:
    relationship = qb.update_relationship(
        "Billing",
        "Invoice Lines",
        "Related Invoice",
        lookup_fields=["Invoice Number", "Customer"],
        summary_fields=[
            RelationshipSummary("SUM", "Line Amount", label="Invoice Total"),
            RelationshipSummary("COUNT", label="Line Count"),
        ],
    )
```

`COUNT` omits a source field. Other accumulation types require a child-table field. Declarative
relationships add stable keys and dependency ordering for these generated fields.

## Current boundaries

- Schema planning reads app, table, field, and relationship metadata but not records.
- Missing bound IDs, ambiguous imports, field-type changes, and dependency cycles block apply.
- Filtered summaries cannot always be read back reliably and may require explicit conflict
  resolution.
- Removing a declaration stops managing that resource; it does not delete the Quickbase object.
- Independent state files must not apply concurrently to the same application.

See [Declarative schemas](declarative-schemas.md) for complete identity, managed-property, formula,
conflict, and state-publication semantics.
