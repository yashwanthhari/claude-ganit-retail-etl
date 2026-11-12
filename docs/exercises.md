# Exercises

Do them in order after the pipeline runs end to end. Each one maps to something you are likely to do on a client project.

1. **Trace a bad row.** Pick a row in `quarantine.orders` (DuckDB) and follow it back to its landing
   file using `_source_file` and `_batch_id`. Then fix the generator or a rule and rerun.
2. **Prove idempotency.** Run `python -m lakehouse.pipeline bronze silver gold warehouse` twice.
   Row counts must not change. Then delete `data/lake/_state/state.json` and rerun: what duplicates
   appear in bronze, and why does silver still end up correct?
3. **Late-arriving data.** Run with `--days 3`, then edit `generator.days` to 5 and run `generate bronze silver gold warehouse`.
   Find an order whose status changed between runs and explain how the MERGE and
   `s.updated_at >= t.updated_at` guard handled it.
4. **Add a rule.** Reject customers whose `signup_date` is in the future. Add a unit test first.
5. **SCD Type 2.** Make `dim_customer` track changes to `city` and `segment` (valid_from, valid_to,
   is_current). Change the generator to emit changed customers on later days. Join the fact to the
   version valid on the order date.
6. **Migrate a stored procedure.** Re-implement `sql/legacy/sp_refresh_daily_sales.sql` as a PySpark
   job writing a Delta table. Reconcile with `marts.daily_sales` and explain any difference.
7. **Performance.** Partition `fact_sales` by `order_date`, run `OPTIMIZE` and `VACUUM` on a Delta table,
   and use `DESCRIBE HISTORY` plus time travel (`versionAsOf`) to inspect an earlier version.
8. **Port the marts to dbt.** Use `dbt-duckdb` so the three SQL files become models with `not_null`
   and `unique` tests. Compare with `dq.py`: what does each approach do better?
9. **Orchestrate.** Run `orchestration/airflow_dag.py` under `airflow standalone`. Make `warehouse`
   fail on purpose and watch retries and downstream task states.
10. **Streaming taste.** Convert the orders ingestion to Spark Structured Streaming reading the landing
    folder, with a checkpoint. Compare with the manifest approach.
11. **Cloud vocabulary.** Write one paragraph each mapping this lab to Azure (ADLS, ADF, Databricks,
    Unity Catalog, Key Vault, Azure DevOps) and to GCP (GCS, Dataproc/Dataflow, BigQuery, Dataform).
    Note what changes in code and what stays the same.
