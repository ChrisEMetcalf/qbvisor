"""Order fields and generated relationship resources by declared dependencies."""

from __future__ import annotations

import heapq
from collections.abc import Mapping
from dataclasses import dataclass

from ..schema import AppSpec


@dataclass(frozen=True, slots=True)
class SchemaDependencyOrder:
    """Deterministic resource order and any nodes blocked by a dependency cycle."""

    ordered: tuple[str, ...]
    blocked: tuple[str, ...] = ()
    cycle: tuple[str, ...] = ()

    @property
    def review_order(self) -> tuple[str, ...]:
        return self.ordered + self.blocked


def _find_cycle(
    nodes: set[str],
    dependencies: Mapping[str, set[str]],
) -> tuple[str, ...]:
    visited: set[str] = set()
    active: list[str] = []
    active_positions: dict[str, int] = {}

    def visit(node: str) -> tuple[str, ...]:
        visited.add(node)
        active_positions[node] = len(active)
        active.append(node)
        for dependency in sorted(dependencies[node] & nodes):
            position = active_positions.get(dependency)
            if position is not None:
                return tuple(active[position:] + [dependency])
            if dependency not in visited:
                cycle = visit(dependency)
                if cycle:
                    return cycle
        active.pop()
        active_positions.pop(node)
        return ()

    for node in sorted(nodes):
        if node not in visited:
            cycle = visit(node)
            if cycle:
                return cycle
    return ()


def plan_schema_dependencies(spec: AppSpec) -> SchemaDependencyOrder:
    """Return the safe execution order for one fully validated application spec."""
    prefix = f"apps.{spec.key}."
    dependencies: dict[str, set[str]] = {}
    priorities: dict[str, int] = {}
    field_addresses: dict[tuple[str, str], str] = {}

    for table in spec.tables:
        for field_spec in table.fields:
            address = field_spec.address(spec.key, table.key)
            field_addresses[(table.key, field_spec.key)] = address
            dependencies[address] = set()
            priorities[address] = 2 if field_spec.formula is not None else 0

    for relationship in spec.relationships:
        relationship_address = relationship.address(spec.key)
        dependencies[relationship_address] = set()
        priorities[relationship_address] = 1
        for field_key in relationship.lookup_fields:
            address = relationship.lookup_address(spec.key, field_key)
            dependencies[address] = {
                relationship_address,
                field_addresses[(relationship.parent_table, field_key)],
            }
            priorities[address] = 3
        for summary in relationship.summary_fields:
            address = summary.address(spec.key, relationship.key)
            dependencies[address] = {relationship_address}
            if summary.field is not None:
                dependencies[address].add(
                    field_addresses[(relationship.child_table, summary.field)]
                )
            priorities[address] = 3

    for table in spec.tables:
        for field_spec in table.fields:
            if field_spec.formula is None:
                continue
            address = field_spec.address(spec.key, table.key)
            dependencies[address].update(
                f"{prefix}{dependency}" for dependency in field_spec.formula.depends_on
            )

    dependents: dict[str, set[str]] = {address: set() for address in dependencies}
    indegree = {address: len(required) for address, required in dependencies.items()}
    for address, required in dependencies.items():
        for dependency in required:
            dependents[dependency].add(address)

    ready = [(priorities[address], address) for address, count in indegree.items() if count == 0]
    heapq.heapify(ready)
    ordered: list[str] = []
    while ready:
        _, address = heapq.heappop(ready)
        ordered.append(address)
        for dependent in sorted(dependents[address]):
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                heapq.heappush(ready, (priorities[dependent], dependent))

    blocked = set(dependencies) - set(ordered)
    return SchemaDependencyOrder(
        ordered=tuple(ordered),
        blocked=tuple(sorted(blocked)),
        cycle=_find_cycle(blocked, dependencies) if blocked else (),
    )
