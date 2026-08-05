"""Stage 4 (part one) — Great Expectations checks that gate the pipeline.

``suite``  declares the expectations Silver must satisfy, as data.
``gate``   runs them and raises, halting the DAG before Gold is merged.

``suite`` is dependency-free and safe to import anywhere; ``gate`` pulls in
PySpark and Great Expectations, so neither is re-exported here. Import from the
submodule you actually need.
"""
