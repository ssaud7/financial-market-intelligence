"""Stage 2 — the Delta Lake medallion architecture.

``bronze``              raw JSON feeds landed verbatim, partitioned by ingest date
``silver``              parsed, typed, deduplicated series with CHECK constraints
``gold``                per-ticker aggregate maintained by MERGE on the ticker key
``schema_enforcement``  proof that mismatched and invariant-breaking writes fail

Nothing is re-exported here on purpose: every submodule imports PySpark, and an
eager re-export would start a JVM for anything that so much as touches the
package name. Import from the submodule you actually need.
"""
