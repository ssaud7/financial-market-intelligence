"""Stage 2 — the Delta Lake medallion architecture.

``bronze``              raw JSON feeds landed verbatim, partitioned by ingest date
``silver``              parsed, typed, deduplicated series with CHECK constraints
``gold``                per-ticker aggregate maintained by MERGE on the ticker key
``schema_enforcement``  proof that mismatched and invariant-breaking writes fail
"""

from fmis.lakehouse.bronze import load_bronze
from fmis.lakehouse.gold import merge_gold
from fmis.lakehouse.schema_enforcement import run_enforcement_demo
from fmis.lakehouse.silver import build_silver

__all__ = ["load_bronze", "build_silver", "merge_gold", "run_enforcement_demo"]
