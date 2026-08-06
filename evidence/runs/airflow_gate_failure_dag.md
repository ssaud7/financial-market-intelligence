# Airflow DAG run — quality gate halts the pipeline

DAG: financial_market_intelligence_gate_failure_demo
Run: manual__2026-08-06T00:00:00+00:00

The gate task FAILED, and gold_merge_must_not_run was marked upstream_failed
with identical start and end timestamps - Airflow never executed it.

`
dag_id                                          | execution_date            | task_id                 | state           | start_date                       | end_date                        
================================================+===========================+=========================+=================+==================================+=================================
financial_market_intelligence_gate_failure_demo | 2026-08-06T00:00:00+00:00 | start                   | success         |                                  | 2026-08-06T00:27:26.757155+00:00
financial_market_intelligence_gate_failure_demo | 2026-08-06T00:00:00+00:00 | quality_gate_tainted    | failed          |                                  | 2026-08-06T00:27:36.178181+00:00
financial_market_intelligence_gate_failure_demo | 2026-08-06T00:00:00+00:00 | gold_merge_must_not_run | upstream_failed | 2026-08-06T00:27:36.207513+00:00 | 2026-08-06T00:27:36.207513+00:00
financial_market_intelligence_gate_failure_demo | 2026-08-06T00:00:00+00:00 | finish                  | upstream_failed | 2026-08-06T00:27:37.226542+00:00 | 2026-08-06T00:27:37.226542+00:00
                                                                                                                                                                                             
`
