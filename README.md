# data-validation

A PySpark data pipeline that validates a dataset against a set of configurable
rules, stores the results as a Spark DataFrame, and writes that DataFrame to
Azure Data Lake Storage Gen2 (ADLS Gen2) as Parquet.

```
read input data --> run validation rules --> results DataFrame --> write to ADLS Gen2 (abfss://)
```

## Project layout

```
config/
  pipeline_config.yaml        # single source of truth: dataset name, source, per-environment
                               # destinations ("local" / "azure"), and validation rules
data/sample/orders.csv        # sample input data (includes deliberate rule violations)
src/data_validation/
  config.py                   # loads pipeline_config.yaml (picks an environment) + .env secrets
  settings.py                 # code-level defaults shared across modules (jar versions, oauth
                               # provider, supported formats, etc.) -- not deployment config
  spark_session.py            # builds the SparkSession (incl. ADLS Gen2 connector jars)
  azure_auth.py                # configures OAuth (service principal) auth for abfss://
  validators/
    rules.py                  # rule types -> Spark Column predicates
    engine.py                 # ValidationEngine: rules + DataFrame -> results DataFrame
  io/
    readers.py                # generic input reader (csv/parquet/json, local or abfss://)
    writers.py                # generic results writer (parquet/csv/json, local or abfss://)
  pipeline.py                 # orchestrates read -> validate -> write
  cli.py                      # `run-validation-pipeline` entrypoint
tests/                        # pytest unit + integration tests (run locally, no Azure)
```

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows
pip install -e ".[dev]"
```

PySpark needs a JDK (11 or 17) on `PATH`/`JAVA_HOME` -- it does not install one
for you. On Windows, writing to the *local* filesystem (e.g. the `local`
environment's sample run, or `pytest`) additionally needs
[winutils.exe](https://github.com/cdarlint/winutils) on `HADOOP_HOME\bin`;
this is a Windows-only Hadoop shim and is **not** needed for the real
`abfss://` (Azure) read/write path, which uses a different filesystem client.

## Validation rules

Rules live under the `rules:` key in `config/pipeline_config.yaml` and are
applied to whatever dataset that config points at -- no code changes needed
to add or remove a check. Supported rule types:

| type             | required fields                | checks that...                              |
|-------------------|--------------------------------|----------------------------------------------|
| `not_null`        | `column`                       | the column is never null                     |
| `unique`          | `column`                       | the column has no duplicate values           |
| `min_max`         | `column`, `min` and/or `max`   | numeric values fall within range             |
| `allowed_values`  | `column`, `values`             | value is one of an allow-list                |
| `regex`           | `column`, `pattern`            | value matches a regex                        |
| `sql_expression`  | `expression`                   | an arbitrary Spark SQL boolean expression (can reference multiple columns of the same row) holds |
| `custom_sql`      | `query`, `reference_tables?`   | a full Spark SQL query -- aggregates, `GROUP BY`, subqueries, self-joins, or joins against `reference_tables` -- returns zero rows |

Each rule also takes `severity` (`error` \| `warning`, default `error`) and an
optional `description`. Every rule produces one row in the results DataFrame
with total/failed/passed record counts, a pass rate, and a `PASS`/`FAIL`
status -- nulls encountered while evaluating a predicate count as failures,
they are never silently dropped.

`sql_expression` vs `custom_sql`: `sql_expression` is a boolean check evaluated
independently per row (e.g. `discount_amount <= order_amount`) and is cheap --
all `sql_expression`/`not_null`/`min_max`/`allowed_values`/`regex` rules run
together in a single aggregation pass over the data. `custom_sql` is a full
query with its own execution -- needed for anything that isn't a per-row
check, e.g. "no more than 15% of orders should be CANCELLED" or "every
`customer_email` must exist in a reference customer table":

```yaml
- id: customer_exists_in_master
  type: custom_sql
  severity: error
  description: every customer_email must exist in the customer master list
  reference_tables:
    customers:
      path: data/reference/customers.csv
      format: csv
  query: SELECT o.* FROM orders o LEFT ANTI JOIN customers c ON o.customer_email = c.email
```

The dataset under validation is always queryable by its `dataset_name` (here,
`orders`). Write the query to `SELECT` the rows that are *wrong* -- every row
the query returns counts as one failure.

## Results schema

```
run_id, run_timestamp, run_date, dataset, rule_id, rule_type, column_name,
severity, description, total_records, failed_records, passed_records,
pass_rate, status
```

## Environments

`config/pipeline_config.yaml` defines one `source` and a set of named
`environments`, each with its own `destination`. Which one runs is picked by
(in order of precedence): `--env`, the `PIPELINE_ENV` env var, then the
config's top-level `environment` key (currently `local`).

- `local` -- writes Parquet to `data/output/` instead of ADLS Gen2. No Azure
  credentials needed; used for dev and the sample run below.
- `azure` -- writes to ADLS Gen2 via `abfss://`. Edit its `destination.path`
  in `pipeline_config.yaml` to point at your storage account/container.

## Running locally (no Azure)

```bash
run-validation-pipeline --config config/pipeline_config.yaml
```

or

```bash
python -m data_validation.cli --config config/pipeline_config.yaml
```

The CLI prints the results table and exits non-zero if any `error`-severity
rule failed (useful as a CI gate).

## Running against Azure Data Lake Storage Gen2

1. Create (or reuse) an App Registration / service principal and grant it
   **Storage Blob Data Contributor** on the target storage account or
   container.
2. Copy `.env.example` to `.env` and fill in `AZURE_TENANT_ID`,
   `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`. Never commit `.env`.
3. Edit `config/pipeline_config.yaml`: point `environments.azure.destination.path`
   (and/or `source.path`) at your
   `abfss://<container>@<storage_account>.dfs.core.windows.net/...` location.
4. Run:

   ```bash
   run-validation-pipeline --config config/pipeline_config.yaml --env azure --env-file .env
   ```

Under the hood, `spark_session.py` pulls in the `hadoop-azure` connector jar
via `spark.jars.packages`, and `azure_auth.py` registers the service
principal as an OAuth token provider on the Hadoop filesystem configuration
for the specific storage account host, so any `abfss://` path against that
account works for both reads and writes.

### Running via `spark-submit` on a real cluster

For Databricks/YARN/K8s, don't rely on this repo's default `local[*]`
master -- either submit with `spark-submit --master ...` (a session created
inside a notebook/job already has the right master) or set the `SPARK_MASTER`
env var before invoking the CLI.

## Tests

```bash
pytest
```

Tests run against a local PySpark session (`enable_azure=False`, no jars
downloaded, no network/Azure access needed) and cover:
- each rule type's pass/fail counting logic, including null-handling and an
  empty-DataFrame edge case (`tests/test_validators.py`)
- the full read -> validate -> write pipeline against the sample dataset,
  including reading the Parquet output back (`tests/test_pipeline.py`)
