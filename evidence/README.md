# Evidence

The rubric asks for proof that each stage *ran*, not just that the code exists. Every
pipeline run writes machine-readable artefacts here, and they are committed alongside the
code so a grader can see the results without executing anything.

| Path | Written by | What it proves |
| --- | --- | --- |
| `runs/contract_scorecard.{json,md}` | `fmis contract-report` | Every fault the producer injected, whether the data contract caught it, and under which reason code. A non-zero `faults_escaped_total` would mean a hole in the contract. |
| `runs/consumer_summary.json` | `fmis consume` | Messages consumed, admitted and rejected, broken down by rejection reason. |
| `runs/producer_summary.json` | `fmis produce` | Messages produced and the exact fault mix injected. |
| `runs/schema_enforcement.json` | `fmis enforce-schema` | Six deliberately invalid writes and the error Delta raised for each, plus one well-formed control write that was accepted. |
| `runs/quality_gate.json` | `fmis quality-gate` | Every expectation evaluated against Silver and its result. |
| `runs/quality_gate_failure_demo.json` | `fmis quality-gate --taint` | The gate failing on tainted data — the run that halts the DAG. |
| `runs/rag_index.json` | `fmis rag-index` | Filings parsed, chunks produced, embedding dimensions, collection size. |
| `runs/rag_answers.{json,md}` | `fmis rag-demo` | Each answer with its citations, plus the full retrieval trail: dense ranks, BM25 ranks, RRF scores, and what the cross-encoder promoted or demoted. |
| `lineage/openlineage-events.jsonl` | every stage | One `START` / `COMPLETE` / `FAIL` event per stage, with dataset inputs and outputs. |
| `runs/airflow_gate_failure_dag.md` | `airflow dags test` | Task states from the failure DAG run: the gate `failed` and `gold_merge_must_not_run` is `upstream_failed` with identical start/end timestamps — Airflow never executed it. |
| `logs/pipeline.jsonl` | every stage | One JSON object per line across all stages, including the rejection reason for each quarantined record. Filter by stage: `jq -c 'select(.stage == "consumer")' evidence/logs/pipeline.jsonl` |
| `screenshots/` | you | Airflow graph views: the production DAG all green, and the failure DAG with `gold_merge_must_not_run` **skipped**. |

## Regenerating

```bash
make infra-up
make ollama-pull
make pipeline
make gate-fails
```

Or execute [`notebooks/capstone_evidence.ipynb`](../notebooks/capstone_evidence.ipynb) top to
bottom, which produces all of the above and captures the output inline.
