"""Runs a set of ValidationRules against a DataFrame and produces a results DataFrame."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_validation.io.readers import read_input
from data_validation.validators.rules import ValidationRule

RESULT_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("run_timestamp", TimestampType(), False),
        StructField("run_date", StringType(), False),
        StructField("dataset", StringType(), False),
        StructField("rule_id", StringType(), False),
        StructField("rule_type", StringType(), False),
        StructField("column_name", StringType(), True),
        StructField("severity", StringType(), False),
        StructField("description", StringType(), True),
        StructField("total_records", LongType(), False),
        StructField("failed_records", LongType(), False),
        StructField("passed_records", LongType(), False),
        StructField("pass_rate", DoubleType(), False),
        StructField("status", StringType(), False),
    ]
)


class ValidationEngine:
    """Evaluates ValidationRules against a DataFrame and returns a results DataFrame.

    Row-level rules (not_null, min_max, allowed_values, regex, sql_expression)
    are all evaluated in a single aggregation pass over the data for
    efficiency. Dataset-level rules (unique, custom_sql) require their own
    pass: custom_sql runs an arbitrary Spark SQL query -- which can aggregate,
    self-join, or join against reference tables -- and treats every row the
    query returns as one validation failure.
    """

    def __init__(
        self,
        spark: SparkSession,
        dataset_name: str,
        rules: list[ValidationRule],
        run_id: str | None = None,
        run_timestamp: datetime | None = None,
    ) -> None:
        self.spark = spark
        self.dataset_name = dataset_name
        self.rules = rules
        self.run_id = run_id or str(uuid.uuid4())
        self.run_timestamp = run_timestamp or datetime.now(timezone.utc)

    def run(self, df: DataFrame) -> DataFrame:
        total_records = df.count()
        df.createOrReplaceTempView(self.dataset_name)

        row_level_rules = [r for r in self.rules if not r.is_dataset_level()]
        dataset_level_rules = [r for r in self.rules if r.is_dataset_level()]

        failed_by_rule_id: dict[str, int] = {}

        if row_level_rules and total_records > 0:
            agg_exprs = [
                F.sum(F.when(rule.build_predicate(), 0).otherwise(1)).alias(rule.id)
                for rule in row_level_rules
            ]
            agg_row = df.agg(*agg_exprs).collect()[0]
            for rule in row_level_rules:
                failed_by_rule_id[rule.id] = int(agg_row[rule.id] or 0)
        else:
            for rule in row_level_rules:
                failed_by_rule_id[rule.id] = 0

        for rule in dataset_level_rules:
            failed_by_rule_id[rule.id] = self._evaluate_dataset_level(df, rule, total_records)

        result_rows = [
            self._build_result_row(rule, total_records, failed_by_rule_id[rule.id])
            for rule in self.rules
        ]
        return self.spark.createDataFrame(result_rows, schema=RESULT_SCHEMA)

    def _evaluate_dataset_level(self, df: DataFrame, rule: ValidationRule, total_records: int) -> int:
        if total_records == 0:
            return 0
        if rule.type == "unique":
            dup_total = (
                df.groupBy(rule.column)
                .count()
                .filter(F.col("count") > 1)
                .agg(F.sum("count").alias("dup_total"))
                .collect()[0]["dup_total"]
            )
            return int(dup_total or 0)
        if rule.type == "custom_sql":
            return self._evaluate_custom_sql(rule)
        raise ValueError(f"Unknown dataset-level rule type: {rule.type!r}")

    def _evaluate_custom_sql(self, rule: ValidationRule) -> int:
        """Runs an arbitrary SQL query; every row it returns counts as one failure.

        The dataset being validated is available in the query under the name
        passed as ``dataset_name`` (e.g. ``orders``). Optional
        ``reference_tables`` are read and registered as additional temp
        views first, so the query can join against them (e.g. a customer
        master list, an allow-list of valid IDs, ...).
        """
        params = rule.params or {}
        query = params.get("query")
        if not query:
            raise ValueError(f"custom_sql rule '{rule.id}' requires a 'query'")

        for alias, ref in (params.get("reference_tables") or {}).items():
            ref_path = ref["path"]
            ref_format = ref.get("format", "parquet")
            read_input(self.spark, ref_path, ref_format).createOrReplaceTempView(alias)

        return self.spark.sql(query).count()

    def _build_result_row(self, rule: ValidationRule, total: int, failed: int) -> Row:
        passed = total - failed
        pass_rate = (passed / total) if total else 1.0
        status = "PASS" if failed == 0 else "FAIL"
        return Row(
            run_id=self.run_id,
            run_timestamp=self.run_timestamp,
            run_date=self.run_timestamp.date().isoformat(),
            dataset=self.dataset_name,
            rule_id=rule.id,
            rule_type=rule.type,
            column_name=rule.column,
            severity=rule.severity,
            description=rule.description,
            total_records=total,
            failed_records=failed,
            passed_records=passed,
            pass_rate=pass_rate,
            status=status,
        )
