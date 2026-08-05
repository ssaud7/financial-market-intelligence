"""Stage 4 (part one) — Great Expectations checks that gate the pipeline.

``suite``  declares the expectations Silver must satisfy, as data.
``gate``   runs them and raises, halting the DAG before Gold is merged.
"""

from fmis.quality.gate import QualityGateFailure, run_quality_gate
from fmis.quality.suite import SUITE_NAME, build_specs

__all__ = ["QualityGateFailure", "run_quality_gate", "SUITE_NAME", "build_specs"]
