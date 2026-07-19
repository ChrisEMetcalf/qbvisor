# Declarative schemas

qbvisor can build and maintain a Quickbase application from Python-defined desired state. The
workflow is intentionally explicit:

1. Define stable resource keys and desired Quickbase settings.
2. Run a read-only plan and review every proposed effect.
3. Apply that specific plan.
4. Let qbvisor verify the resulting schema before publishing state.
5. Run another plan and expect no changes.

This is similar to Terraform's plan-and-apply model, but the current scope is deliberately
non-destructive. qbvisor creates and updates declared resources. It does not delete remote resources
that are absent from the specification.

## Resource identity

Every managed resource has a lowercase key that is independent from its Quickbase display name.
For example:

```text
apps.operations
apps.operations.tables.projects
apps.operations.tables.projects.fields.status
apps.operations.relationships.project_details
apps.operations.relationships.project_details.lookups.name
apps.operations.relationships.project_details.summaries.total_hours
```

On the first plan, qbvisor uses a unique case-insensitive display-name match to find existing
tables and fields. The configured `QB_APP_IDS` alias may match either `AppSpec.key` or
`AppSpec.name`. An exact match is shown with `[bind state]`; it does not mutate Quickbase.

After apply publishes state, Quickbase IDs are authoritative. Changing `TableSpec.name` or
`FieldSpec.label` therefore produces an in-place rename against the stored ID. If a stored ID is no
longer returned, the plan reports drift and blocks apply instead of creating a replacement.

Relationship-created fields also receive stable identities. Lookup addresses derive from their
parent field key. Every summary requires its own key because multiple summaries may use the same
source and accumulation type.

## Managed attributes

Required names, labels, field types, keys, and relationship references are always managed.
Optional settings use `None` to mean "do not manage this attribute." Supplying a value, including
an empty string or `False`, takes ownership of that setting.

`FieldSpec.properties` manages only the keys it contains. Other field-type properties returned by
Quickbase are ignored. `AppSpec.security_properties` behaves the same way. Application variables
are compared as a complete name-to-value mapping when `variables` is supplied.

JSON property values are copied and frozen when the specification is constructed. A list or
dictionary passed by the caller cannot change underneath a reviewed plan.

## Planning

```python
plan = qb.plan_app(spec, state_path=".qbvisor/state.json")
print(plan)
```

Plan markers are:

- `+` create the resource;
- `~` update managed attributes;
- `=` the remote resource already matches;
- `!` a conflict that must be resolved before apply.

Plan output includes individual before-and-after values and whether an existing remote ID will be
bound into state. `plan.can_apply` is false when any conflict is present. `plan.to_dict()` provides a
deterministic machine-readable representation for tooling or review output.

The plan also prints the resource execution order. Scalar fields are created first. Formula fields,
relationships, lookups, and summaries then follow their declared dependencies. A dependency cycle
is reported as a conflict before Quickbase is changed.

Planning is read-only but not offline. It calls Quickbase to observe the current schema. It does not
query table records or attachments. The planner fetches the app, one table collection, fields for
each declared existing table, and relationships for declared existing child tables.

## Formula fields

`FormulaSpec` manages raw Quickbase formula syntax. qbvisor preserves the expression and sends it to
Quickbase for parsing, reference validation, return-type checking, and evaluation. It does not
maintain a second implementation of the Quickbase formula language.

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
                    properties={"decimalPlaces": 2},
                ),
            ],
        )
    ],
)
```

Quickbase formulas refer to fields by label. `depends_on` does not rewrite that formula text. It
declares the stable resources that must exist first. Supported dependency addresses are:

```text
tables.<table_key>.fields.<field_key>
relationships.<relationship_key>
relationships.<relationship_key>.lookups.<parent_field_key>
relationships.<relationship_key>.summaries.<summary_key>
```

Declare formula-to-formula dependencies and any dependency on a relationship-generated field.
Formula queries that use newly managed fields in another table should declare those table-field
dependencies as well. qbvisor does not infer dependencies from arbitrary formula text because
bracketed content can also appear in literals, escaped text, HTML, comments, and embedded query
strings.

The JSON API currently supports formula mode for `text`, `rich-text`, `numeric`, `currency`,
`rating`, `percent`, `date`, `datetime`, `timeofday`, `duration`, `checkbox`, `phone`, `email`,
`url`, `user`, and `multitext`. Quickbase returns a requested `datetime` formula as the canonical
`timestamp` response type; qbvisor handles that difference during planning.

The JSON field endpoint rejects formula properties for text multiple-choice, text multi-line,
List-User, address, and file fields. It also does not accept Work Date as a create-field type. These
are rejected while constructing the specification instead of failing partway through apply. This
scope reflects observed JSON API behavior and is narrower than the formula types available through
some Quickbase UI workflows.

A formula field must be created with a non-empty, currently valid expression. Quickbase does not
allow qbvisor to create a scalar or empty placeholder and convert it later. The dependency order is
therefore part of the reviewed plan and is enforced during apply. Field mode is also part of
identity: an existing scalar, lookup, or summary field is a conflict when the desired resource is a
formula, and the reverse is also true.

Quickbase removes trailing formula whitespace when reading the field. `FormulaSpec` applies the
same normalization while preserving internal spacing, line breaks, comments, and escaping. A
formula query produces a plan warning because filtering, sorting, or grouping on formula-query
fields can have application-wide performance effects.

## Apply and state publication

```python
reviewed_plan = qb.plan_app(spec)

# Present or store the plan for review before this point.
result = qb.apply_app(reviewed_plan)
```

Apply acquires a non-blocking lock for the selected state path and re-runs the plan. If remote or
local managed state changed, `QuickbaseSchemaStalePlanError` is raised before any mutation. Create
and update requests are not automatically retried after an uncertain connection failure.

After all planned requests finish, qbvisor writes a candidate state file beside the destination and
plans again through the new bindings. The result must contain no Quickbase changes, state bindings,
or conflicts. Only then is the candidate atomically moved to the configured state path. If
verification fails, the previous state file remains in place.

State has a UUID lineage and a monotonically increasing serial. A fully unchanged apply does not
rewrite the file or increment its serial.

The default `.qbvisor/state.json` path is ignored by this repository template. The file has no API
token, request headers, or record values. It still needs durable handling because it is the source
of stable identity for renames and for settings Quickbase cannot read back. Back it up or persist it
through an access-controlled CI artifact or state service when more than one environment performs
applies. Do not run concurrent applies against independent copies of the same state.

## Conflicts and current boundaries

Apply is blocked when:

- a state-bound Quickbase ID is missing;
- more than one remote resource could satisfy a first import;
- a field's desired type differs from its existing immutable type;
- a field's scalar or derived mode differs from the desired formula mode;
- declared schema dependencies contain a cycle;
- a relationship is bound to different parent or child tables;
- a generated field no longer matches its stable relationship identity;
- a filtered summary exists without state, because Quickbase does not expose the filter for
  verification;
- a reviewed plan is stale or another process holds the state lock.

Changing the filter of an existing summary is not treated as an in-place update because Quickbase
does not provide a reliable read-back value. It remains an explicit conflict until safe replacement
support is added.

The current workflow does not delete applications, tables, fields, relationships, lookups, or
summaries. Removing an item from the specification stops managing it; it does not remove the remote
resource or its existing state entry. This prevents a missing declaration from becoming an
accidental destructive operation.
