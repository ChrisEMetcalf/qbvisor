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

Planning is read-only but not offline. It calls Quickbase to observe the current schema. It does not
query table records or attachments. The planner fetches the app, one table collection, fields for
each declared existing table, and relationships for declared existing child tables.

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
