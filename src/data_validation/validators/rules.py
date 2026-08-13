"""Rule definitions and the logic that turns a rule config dict into a row-level predicate."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyspark.sql import Column
from pyspark.sql import functions as F

from data_validation.settings import DATASET_LEVEL_RULE_TYPES, DEFAULT_SEVERITY

_ROW_LEVEL_BUILDERS: dict[str, Any] = {}


def row_level_rule(rule_type: str):
    def register(fn):
        _ROW_LEVEL_BUILDERS[rule_type] = fn
        return fn

    return register


@dataclass(frozen=True)
class ValidationRule:
    id: str
    type: str
    column: str | None = None
    severity: str = DEFAULT_SEVERITY
    description: str = ""
    params: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ValidationRule":
        known = {"id", "type", "column", "severity", "description"}
        params = {k: v for k, v in raw.items() if k not in known}
        return cls(
            id=raw["id"],
            type=raw["type"],
            column=raw.get("column"),
            severity=raw.get("severity", DEFAULT_SEVERITY),
            description=raw.get("description", ""),
            params=params or None,
        )

    def is_dataset_level(self) -> bool:
        return self.type in DATASET_LEVEL_RULE_TYPES

    def build_predicate(self) -> Column:
        """Return a non-null boolean Column that is True for VALID rows.

        Only valid for row-level rule types; raises for dataset-level types
        such as ``unique``, which the engine evaluates separately.
        """
        if self.is_dataset_level():
            raise ValueError(f"Rule type '{self.type}' is dataset-level, not row-level")
        try:
            builder = _ROW_LEVEL_BUILDERS[self.type]
        except KeyError as exc:
            raise ValueError(f"Unknown rule type: {self.type!r}") from exc
        predicate = builder(self)
        # Null-safe: a predicate that evaluates to NULL (e.g. rlike/expr on a
        # NULL column) is treated as a validation failure, not silently
        # excluded, so `filter(~predicate)` always captures failing rows.
        return F.coalesce(predicate, F.lit(False))


@row_level_rule("not_null")
def _not_null(rule: ValidationRule) -> Column:
    return F.col(rule.column).isNotNull()


@row_level_rule("min_max")
def _min_max(rule: ValidationRule) -> Column:
    params = rule.params or {}
    col = F.col(rule.column)
    conditions = [col.isNotNull()]
    if "min" in params:
        conditions.append(col >= F.lit(params["min"]))
    if "max" in params:
        conditions.append(col <= F.lit(params["max"]))
    predicate = conditions[0]
    for cond in conditions[1:]:
        predicate = predicate & cond
    return predicate


@row_level_rule("allowed_values")
def _allowed_values(rule: ValidationRule) -> Column:
    params = rule.params or {}
    values = params["values"]
    return F.col(rule.column).isin(values)


@row_level_rule("regex")
def _regex(rule: ValidationRule) -> Column:
    params = rule.params or {}
    pattern = params["pattern"]
    return F.col(rule.column).rlike(pattern)


@row_level_rule("sql_expression")
def _sql_expression(rule: ValidationRule) -> Column:
    params = rule.params or {}
    expression = params["expression"]
    return F.expr(expression)
