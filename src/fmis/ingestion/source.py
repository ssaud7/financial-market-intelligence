"""Locating and normalising the structured source dataset.

The producer streams a Kaggle export of S&P 500 daily history. Kaggle snapshots
of this dataset come in two shapes, and both are supported because the layout is
detected from the files themselves rather than assumed:

**Per-ticker layout** (the *S&P 500 Stocks — 10 Year* export). One CSV per
symbol, named after it, written by ``yfinance`` with a three-row header::

    Price,Close,High,Low,Open,Volume     <- field names
    Ticker,AAPL,AAPL,AAPL,AAPL,AAPL      <- repeated symbol
    Date,,,,,                            <- index name, then blanks
    2015-12-21,24.199,24.208,23.802,24.188,190362400

Note the column order: **Close comes before High/Low/Open**, and there is no
adjusted close. Reading this with a naive ``read_csv`` yields two junk rows and
silently mis-assigns every price column, so the header rows are skipped and the
columns are named positionally.

**Wide layout** (the *sp500_stocks.csv* export). A single file with ``Date`` and
``Symbol`` columns covering every ticker.

Rows whose price fields are entirely empty are skipped: exports pad each ticker
back to the index start date, so a company that listed in 2016 carries years of
blanks. Those are absent data, not corrupt data — deliberate corruption is
injected by the producer, where it can be counted.
"""

from __future__ import annotations

import zipfile
from pathlib import Path
from typing import Iterator

import pandas as pd

from fmis.config import settings
from fmis.logging_setup import configure_logging

log = configure_logging("source")

# Wide-layout header (lower-cased, stripped) -> contract field name.
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

# The yfinance per-ticker export's first header row, which identifies the layout.
_YF_HEADER_FIRST_CELL = "price"
_YF_HEADER_ROWS = 3


def unpack_archives(raw_dir: Path | None = None) -> list[Path]:
    """Extract every zip under ``data/raw`` into ``data/raw/extracted/<name>/``.

    Idempotent: an archive whose target directory already has content is skipped.
    """
    raw_dir = raw_dir or settings.data_root / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    extracted_root = raw_dir / "extracted"
    targets: list[Path] = []

    for archive in sorted(raw_dir.glob("*.zip")):
        target = extracted_root / archive.stem
        if target.exists() and any(target.rglob("*")):
            log.info("source.archive_already_extracted", archive=archive.name)
            targets.append(target)
            continue
        target.mkdir(parents=True, exist_ok=True)
        log.info("source.extracting", archive=archive.name, target=str(target))
        with zipfile.ZipFile(archive) as zf:
            zf.extractall(target)
        targets.append(target)

    return targets


def _is_yfinance_per_ticker(path: Path) -> bool:
    """True if the file carries the yfinance three-row header."""
    try:
        with path.open(encoding="utf-8", errors="ignore") as handle:
            first = handle.readline().strip().lower()
    except OSError:
        return False
    return first.split(",")[0].strip() == _YF_HEADER_FIRST_CELL


def discover_per_ticker_csvs(raw_dir: Path | None = None) -> dict[str, Path]:
    """Map ticker symbol -> CSV path for the per-ticker export layout."""
    raw_dir = raw_dir or settings.data_root / "raw"
    unpack_archives(raw_dir)

    found: dict[str, Path] = {}
    for path in sorted(raw_dir.rglob("*.csv")):
        # The symbol is the filename; BRK-B.csv and BF-B.csv keep their suffix.
        symbol = path.stem.strip().upper()
        if not symbol or symbol in found:
            continue
        if _is_yfinance_per_ticker(path):
            found[symbol] = path

    if found:
        log.info(
            "source.per_ticker_layout_detected",
            files=len(found),
            sample=sorted(found)[:8],
        )
    return found


def _read_per_ticker_csv(path: Path, ticker: str) -> pd.DataFrame:
    """Read one yfinance per-ticker CSV into contract-shaped columns.

    The three header rows are skipped and columns named positionally, because
    the real field names live in a row pandas would otherwise treat as data.
    """
    frame = pd.read_csv(
        path,
        skiprows=_YF_HEADER_ROWS,
        header=None,
        names=["trade_date", "close", "high", "low", "open", "volume"],
    )
    frame["ticker"] = ticker
    # This export carries no adjusted close; the contract makes it optional.
    frame["adj_close"] = pd.NA
    return frame


def find_quotes_csv(raw_dir: Path | None = None) -> Path:
    """Return the single wide CSV covering every ticker, if the export uses one."""
    raw_dir = raw_dir or settings.data_root / "raw"
    unpack_archives(raw_dir)

    candidates: list[tuple[int, Path]] = []
    for csv_path in sorted(raw_dir.rglob("*.csv")):
        if _is_yfinance_per_ticker(csv_path):
            continue
        try:
            header = pd.read_csv(csv_path, nrows=0).columns.tolist()
        except Exception as exc:  # noqa: BLE001 - one bad file must not abort discovery
            log.warning("source.unreadable_csv", path=str(csv_path), error=str(exc))
            continue

        mapped = {COLUMN_ALIASES.get(str(c).strip().lower()) for c in header}
        if REQUIRED.issubset(mapped):
            candidates.append((csv_path.stat().st_size, csv_path))

    if not candidates:
        raise FileNotFoundError(
            f"No daily OHLCV CSV found under {raw_dir}. Expected either per-ticker "
            "files (yfinance layout) or a wide file with Date/Symbol/OHLCV columns."
        )

    _, chosen = max(candidates, key=lambda pair: pair[0])
    log.info("source.wide_layout_detected", path=str(chosen))
    return chosen


def _load_wide(csv_path: Path, wanted: set[str], chunksize: int) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for chunk in pd.read_csv(csv_path, chunksize=chunksize):
        rename = {
            c: COLUMN_ALIASES[str(c).strip().lower()]
            for c in chunk.columns
            if str(c).strip().lower() in COLUMN_ALIASES
        }
        chunk = chunk.rename(columns=rename)
        chunk = chunk[[c for c in chunk.columns if c in REQUIRED | {"adj_close"}]]
        chunk["ticker"] = chunk["ticker"].astype(str).str.strip().str.upper()
        if wanted:
            chunk = chunk[chunk["ticker"].isin(wanted)]
        if not chunk.empty:
            frames.append(chunk)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def load_quotes(
    csv_path: Path | None = None,
    *,
    tickers: list[str] | None = None,
    chunksize: int = 250_000,
) -> pd.DataFrame:
    """Load, filter and date-order the daily quotes for the configured tickers."""
    wanted = set(tickers if tickers is not None else settings.tickers)

    if csv_path is None:
        per_ticker = discover_per_ticker_csvs()
    else:
        per_ticker = {}

    if per_ticker:
        selected = {t: p for t, p in per_ticker.items() if not wanted or t in wanted}
        missing = sorted(wanted - set(selected)) if wanted else []
        if missing:
            log.warning(
                "source.tickers_not_in_dataset",
                missing=missing,
                available_sample=sorted(per_ticker)[:12],
            )
        if not selected:
            raise ValueError(
                f"None of the configured tickers {sorted(wanted)} were found. "
                f"The dataset contains {len(per_ticker)} symbols, e.g. "
                f"{sorted(per_ticker)[:10]}. Adjust PRODUCER_TICKERS in .env."
            )
        df = pd.concat(
            [_read_per_ticker_csv(path, ticker) for ticker, path in sorted(selected.items())],
            ignore_index=True,
        )
        source_label = f"{len(selected)} per-ticker files"
    else:
        csv_path = csv_path or find_quotes_csv()
        df = _load_wide(csv_path, wanted, chunksize)
        if df.empty:
            raise ValueError(
                f"{csv_path} contained no rows for tickers {sorted(wanted)}. "
                "Check PRODUCER_TICKERS in .env against the dataset's symbols."
            )
        source_label = str(csv_path)

    rows_scanned = len(df)

    # Coerce to numeric before dropping: a stray header row read as data becomes
    # NaN here rather than silently poisoning the stream as a string.
    for column in (*PRICE_COLUMNS, "volume"):
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df = df.dropna(subset=list(PRICE_COLUMNS), how="all")
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce", format="mixed")
    df = df.dropna(subset=["trade_date"])
    df = df.sort_values(["trade_date", "ticker"]).reset_index(drop=True)

    if df.empty:
        raise ValueError(f"No usable rows after cleaning {source_label}.")

    log.info(
        "source.loaded",
        source=source_label,
        rows_scanned=rows_scanned,
        rows_kept=len(df),
        tickers=sorted(df["ticker"].unique().tolist()),
        date_min=str(df["trade_date"].min().date()),
        date_max=str(df["trade_date"].max().date()),
    )
    return df


def iter_quote_payloads(df: pd.DataFrame) -> Iterator[dict]:
    """Yield contract-shaped dictionaries, oldest session first."""
    for row in df.itertuples(index=False):
        yield {
            "ticker": row.ticker,
            "trade_date": row.trade_date.date().isoformat(),
            "open": _clean_float(row.open),
            "high": _clean_float(row.high),
            "low": _clean_float(row.low),
            "close": _clean_float(row.close),
            "adj_close": _clean_float(getattr(row, "adj_close", None)),
            "volume": _clean_int(row.volume),
        }


def _clean_float(value) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value), 6)


def _clean_int(value) -> int | None:
    if value is None or pd.isna(value):
        return None
    return int(value)
