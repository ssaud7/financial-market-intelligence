"""Locating and normalising the structured source dataset.

The producer streams the Kaggle *S&P 500 Stocks — Daily Historical Data* export.
Kaggle bundles vary (file names differ between snapshots, and the archive may
still be zipped), so rather than hard-coding one path this module:

1. unpacks any ``.zip`` found under ``data/raw/``,
2. picks the CSV that actually looks like a daily OHLCV table,
3. normalises its header to the contract's field names, and
4. yields date-ordered row dictionaries.

Rows whose price fields are entirely empty are skipped: the Kaggle export pads
every ticker back to the index start date, so a company that listed in 2016 has
years of blank rows. Those are absent data, not corrupt data — deliberate
corruption is injected by the producer instead, where it can be counted.
"""

from __future__ import annotations

import zipfile
from pathlib import Path
from typing import Iterator

import pandas as pd

from fmis.config import settings
from fmis.logging_setup import configure_logging

log = configure_logging("source")

# Source header (lower-cased, stripped) -> contract field name.
COLUMN_ALIASES: dict[str, str] = {
    "date": "trade_date",
    "trade_date": "trade_date",
    "symbol": "ticker",
    "ticker": "ticker",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "close/last": "close",
    "adj close": "adj_close",
    "adj_close": "adj_close",
    "adjclose": "adj_close",
    "volume": "volume",
}

REQUIRED = {"trade_date", "ticker", "open", "high", "low", "close", "volume"}
PRICE_COLUMNS = ("open", "high", "low", "close")


def unpack_archives(raw_dir: Path | None = None) -> list[Path]:
    """Extract every zip under ``data/raw`` into ``data/raw/extracted/<name>/``.

    Idempotent: an archive whose target directory already exists is skipped.
    """
    raw_dir = raw_dir or settings.data_root / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    extracted_root = raw_dir / "extracted"
    targets: list[Path] = []

    for archive in sorted(raw_dir.glob("*.zip")):
        target = extracted_root / archive.stem
        if target.exists() and any(target.iterdir()):
            log.info("source.archive_already_extracted", archive=archive.name, target=str(target))
            targets.append(target)
            continue
        target.mkdir(parents=True, exist_ok=True)
        log.info("source.extracting", archive=archive.name, target=str(target))
        with zipfile.ZipFile(archive) as zf:
            zf.extractall(target)
        targets.append(target)

    return targets


def _normalise_header(columns: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for column in columns:
        key = str(column).strip().lower()
        if key in COLUMN_ALIASES:
            mapping[column] = COLUMN_ALIASES[key]
    return mapping


def find_quotes_csv(raw_dir: Path | None = None) -> Path:
    """Return the CSV under ``data/raw`` that looks like a daily OHLCV table.

    Selection is by content, not filename: the first CSV whose header maps onto
    every required contract field wins, preferring the largest such file so a
    full history beats a truncated sample.
    """
    raw_dir = raw_dir or settings.data_root / "raw"
    unpack_archives(raw_dir)

    candidates: list[tuple[int, Path]] = []
    for csv_path in sorted(raw_dir.rglob("*.csv")):
        try:
            header = pd.read_csv(csv_path, nrows=0).columns.tolist()
        except Exception as exc:  # noqa: BLE001 - a stray unreadable CSV must not abort discovery
            log.warning("source.unreadable_csv", path=str(csv_path), error=str(exc))
            continue

        mapped = set(_normalise_header(header).values())
        if REQUIRED.issubset(mapped):
            candidates.append((csv_path.stat().st_size, csv_path))
            log.info("source.candidate", path=str(csv_path), size_mb=round(csv_path.stat().st_size / 1e6, 1))

    if not candidates:
        raise FileNotFoundError(
            f"No daily OHLCV CSV found under {raw_dir}. Expected a file with columns covering "
            f"{sorted(REQUIRED)} (e.g. sp500_stocks.csv from the Kaggle S&P 500 dataset). "
            "Place the Kaggle download in data/raw/ and re-run."
        )

    _, chosen = max(candidates, key=lambda pair: pair[0])
    log.info("source.selected", path=str(chosen))
    return chosen


def load_quotes(
    csv_path: Path | None = None,
    *,
    tickers: list[str] | None = None,
    chunksize: int = 250_000,
) -> pd.DataFrame:
    """Load, filter and date-order the daily quotes for the configured tickers."""
    csv_path = csv_path or find_quotes_csv()
    wanted = set(tickers if tickers is not None else settings.tickers)

    frames: list[pd.DataFrame] = []
    total_rows = 0

    for chunk in pd.read_csv(csv_path, chunksize=chunksize):
        rename = _normalise_header(chunk.columns.tolist())
        chunk = chunk.rename(columns=rename)
        keep = [c for c in chunk.columns if c in REQUIRED | {"adj_close"}]
        chunk = chunk[keep]
        total_rows += len(chunk)

        chunk["ticker"] = chunk["ticker"].astype(str).str.strip().str.upper()
        if wanted:
            chunk = chunk[chunk["ticker"].isin(wanted)]
        if chunk.empty:
            continue

        # Drop the padding rows described in the module docstring.
        chunk = chunk.dropna(subset=list(PRICE_COLUMNS), how="all")
        frames.append(chunk)

    if not frames:
        raise ValueError(
            f"{csv_path} contained no rows for tickers {sorted(wanted)}. "
            "Check PRODUCER_TICKERS in .env against the symbols present in the dataset."
        )

    df = pd.concat(frames, ignore_index=True)
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce", format="mixed")
    df = df.dropna(subset=["trade_date"])
    df = df.sort_values(["trade_date", "ticker"]).reset_index(drop=True)

    log.info(
        "source.loaded",
        path=str(csv_path),
        rows_scanned=total_rows,
        rows_kept=len(df),
        tickers=sorted(df["ticker"].unique().tolist()),
        date_min=str(df["trade_date"].min().date()),
        date_max=str(df["trade_date"].max().date()),
    )
    return df


def iter_quote_payloads(df: pd.DataFrame) -> Iterator[dict]:
    """Yield contract-shaped dictionaries, oldest session first."""
    for row in df.itertuples(index=False):
        record = {
            "ticker": row.ticker,
            "trade_date": row.trade_date.date().isoformat(),
            "open": _clean_float(row.open),
            "high": _clean_float(row.high),
            "low": _clean_float(row.low),
            "close": _clean_float(row.close),
            "adj_close": _clean_float(getattr(row, "adj_close", None)),
            "volume": _clean_int(row.volume),
        }
        yield record


def _clean_float(value) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value), 6)


def _clean_int(value) -> int | None:
    if value is None or pd.isna(value):
        return None
    return int(value)
