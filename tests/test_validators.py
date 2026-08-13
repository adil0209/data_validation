from data_validation.validators.engine import ValidationEngine
from data_validation.validators.rules import ValidationRule

RULES = [
    ValidationRule.from_dict({"id": "id_not_null", "type": "not_null", "column": "id"}),
    ValidationRule.from_dict({"id": "id_unique", "type": "unique", "column": "id"}),
    ValidationRule.from_dict({"id": "email_not_null", "type": "not_null", "column": "email"}),
    ValidationRule.from_dict(
        {
            "id": "email_format",
            "type": "regex",
            "column": "email",
            "pattern": r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
        }
    ),
    ValidationRule.from_dict({"id": "amount_range", "type": "min_max", "column": "amount", "min": 0, "max": 100}),
    ValidationRule.from_dict(
        {
            "id": "status_allowed",
            "type": "allowed_values",
            "column": "status",
            "values": ["PENDING", "SHIPPED", "DELIVERED"],
        }
    ),
    ValidationRule.from_dict(
        {"id": "discount_vs_amount", "type": "sql_expression", "expression": "discount <= amount"}
    ),
]


def _make_input_df(spark):
    rows = [
        (1, "a@example.com", 10.0, 5.0, "PENDING"),
        (2, "bad-email", 20.0, 0.0, "SHIPPED"),
        (3, None, -5.0, 0.0, "WEIRD"),
        (1, "dup@example.com", 30.0, 40.0, "DELIVERED"),
    ]
    return spark.createDataFrame(rows, schema=["id", "email", "amount", "discount", "status"])


def _results_by_rule(results_df):
    return {row["rule_id"]: row for row in results_df.collect()}


def test_engine_computes_expected_pass_fail_counts(spark):
    df = _make_input_df(spark)
    engine = ValidationEngine(spark=spark, dataset_name="test_dataset", rules=RULES)
    results_df = engine.run(df)

    assert results_df.count() == len(RULES)
    by_rule = _results_by_rule(results_df)

    assert by_rule["id_not_null"]["status"] == "PASS"
    assert by_rule["id_not_null"]["failed_records"] == 0

    assert by_rule["id_unique"]["status"] == "FAIL"
    assert by_rule["id_unique"]["failed_records"] == 2  # both rows with id=1

    assert by_rule["email_not_null"]["status"] == "FAIL"
    assert by_rule["email_not_null"]["failed_records"] == 1

    assert by_rule["email_format"]["status"] == "FAIL"
    assert by_rule["email_format"]["failed_records"] == 2  # "bad-email" + null email

    assert by_rule["amount_range"]["status"] == "FAIL"
    assert by_rule["amount_range"]["failed_records"] == 1  # -5.0 out of [0, 100]

    assert by_rule["status_allowed"]["status"] == "FAIL"
    assert by_rule["status_allowed"]["failed_records"] == 1  # "WEIRD"

    assert by_rule["discount_vs_amount"]["status"] == "FAIL"
    assert by_rule["discount_vs_amount"]["failed_records"] == 2

    for row in results_df.collect():
        assert row["total_records"] == 4
        assert row["passed_records"] == row["total_records"] - row["failed_records"]
        assert row["dataset"] == "test_dataset"


def test_engine_handles_empty_dataframe(spark):
    df = spark.createDataFrame([], schema="id INT, email STRING, amount DOUBLE, discount DOUBLE, status STRING")
    engine = ValidationEngine(spark=spark, dataset_name="empty_dataset", rules=RULES)
    results_df = engine.run(df)

    for row in results_df.collect():
        assert row["total_records"] == 0
        assert row["failed_records"] == 0
        assert row["status"] == "PASS"
        assert row["pass_rate"] == 1.0


def test_all_rules_pass_on_clean_data(spark):
    rows = [
        (1, "a@example.com", 10.0, 5.0, "PENDING"),
        (2, "b@example.com", 20.0, 0.0, "SHIPPED"),
    ]
    df = spark.createDataFrame(rows, schema=["id", "email", "amount", "discount", "status"])
    engine = ValidationEngine(spark=spark, dataset_name="clean_dataset", rules=RULES)
    results_df = engine.run(df)

    statuses = {row["status"] for row in results_df.collect()}
    assert statuses == {"PASS"}


def test_custom_sql_rule_aggregate_threshold(spark):
    # 2 of 4 rows are CANCELLED -- above a 25% threshold -- so the query
    # (which selects only the CANCELLED rows, gated by that global ratio)
    # should return both of them as failures.
    rows = [
        (1, "PENDING"),
        (2, "CANCELLED"),
        (3, "DELIVERED"),
        (4, "CANCELLED"),
    ]
    df = spark.createDataFrame(rows, schema=["id", "status"])
    rule = ValidationRule.from_dict(
        {
            "id": "cancellation_rate",
            "type": "custom_sql",
            "severity": "warning",
            "query": """
                SELECT * FROM widgets
                WHERE status = 'CANCELLED'
                  AND (SELECT COUNT(*) FROM widgets WHERE status = 'CANCELLED')
                      > 0.25 * (SELECT COUNT(*) FROM widgets)
            """,
        }
    )
    engine = ValidationEngine(spark=spark, dataset_name="widgets", rules=[rule])
    results_df = engine.run(df)

    row = results_df.collect()[0]
    assert row["status"] == "FAIL"
    assert row["failed_records"] == 2
    assert row["total_records"] == 4


def test_custom_sql_rule_with_reference_table_join(spark, tmp_path):
    ref_path = tmp_path / "valid_customers.csv"
    ref_path.write_text("email\na@example.com\nc@example.com\n")

    rows = [
        (1, "a@example.com"),
        (2, "b@example.com"),  # not in the reference table
        (3, "c@example.com"),
    ]
    df = spark.createDataFrame(rows, schema=["id", "customer_email"])
    rule = ValidationRule.from_dict(
        {
            "id": "customer_exists_in_master",
            "type": "custom_sql",
            "severity": "error",
            "reference_tables": {"valid_customers": {"path": str(ref_path), "format": "csv"}},
            "query": """
                SELECT o.* FROM customers o
                LEFT ANTI JOIN valid_customers v ON o.customer_email = v.email
            """,
        }
    )
    engine = ValidationEngine(spark=spark, dataset_name="customers", rules=[rule])
    results_df = engine.run(df)

    row = results_df.collect()[0]
    assert row["status"] == "FAIL"
    assert row["failed_records"] == 1  # only b@example.com has no match
    assert row["total_records"] == 3
