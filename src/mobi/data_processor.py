"""
Module for processing and standardizing Mobi bike share data.

This module handles reading CSV files, standardizing schemas across different
data formats, and combining data into a unified format.
"""

import re
import zipfile
from pathlib import Path

import pandas as pd


class DataProcessorError(Exception):
    """Base exception for data processor errors."""


def read_trip_data_file(file_path: Path) -> pd.DataFrame:
    """
    Read a trip data file (CSV or ZIP) into a DataFrame.

    Args:
        file_path: Path to the data file

    Returns:
        DataFrame containing the trip data

    Raises:
        DataProcessorError: If the file cannot be read
    """
    file_path = Path(file_path)

    try:
        encodings = ["utf-8", "cp1252", "latin1"]
        df = None

        if file_path.suffix.lower() == ".zip":
            with zipfile.ZipFile(file_path, "r") as zip_ref:
                csv_files = [f for f in zip_ref.namelist() if f.endswith(".csv")]
                if not csv_files:
                    raise DataProcessorError(f"No CSV file found in {file_path}")

                for enc in encodings:
                    # engine=c
                    try:
                        with zip_ref.open(csv_files[0]) as csv_file:
                            df = pd.read_csv(csv_file, encoding=enc)
                        print(f"  • Read with encoding={enc}, engine=c")
                        break
                    except UnicodeDecodeError:
                        continue
                    except pd.errors.ParserError:
                        # engine=python
                        try:
                            with zip_ref.open(csv_files[0]) as csv_file:
                                df = pd.read_csv(csv_file, encoding=enc, engine="python")
                            print(f"  • Read with encoding={enc}, engine=python")
                            break
                        except pd.errors.ParserError:
                            # final fallback: python + skip bad lines
                            with zip_ref.open(csv_files[0]) as csv_file:
                                df = pd.read_csv(
                                    csv_file,
                                    encoding=enc,
                                    engine="python",
                                    on_bad_lines="skip",
                                )
                            print(
                                f"  • Read with encoding={enc}, engine=python (skip bad lines)"
                            )
                            break
        else:
            for enc in encodings:
                # engine=c
                try:
                    df = pd.read_csv(file_path, encoding=enc)
                    print(f"  • Read with encoding={enc}, engine=c")
                    break
                except UnicodeDecodeError:
                    continue
                except pd.errors.ParserError:
                    # engine=python
                    try:
                        df = pd.read_csv(file_path, encoding=enc, engine="python")
                        print(f"  • Read with encoding={enc}, engine=python")
                        break
                    except pd.errors.ParserError:
                        # final fallback: python + skip bad lines
                        df = pd.read_csv(
                            file_path,
                            encoding=enc,
                            engine="python",
                            on_bad_lines="skip",
                        )
                        print(
                            f"  • Read with encoding={enc}, engine=python (skip bad lines)"
                        )
                        break

        if df is None:
            raise DataProcessorError("Failed to decode file with standard fallbacks")

        return df

    except Exception as e:
        raise DataProcessorError(f"Failed to read {file_path}: {e}") from e


def standardize_trip_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize the schema of a trip data DataFrame.

    Mobi data may have different column names over time. This function
    normalizes them to a consistent format.

    Args:
        df: Input DataFrame with trip data

    Returns:
        DataFrame with standardized column names and types

    Raises:
        DataProcessorError: If required columns are missing
    """
    # Create a copy to avoid modifying the original
    df = df.copy()

    # Define column name mappings (old name -> new name)
    column_mappings = {
        # Common variations of column names
        "departure": "departure_time",
        "return": "return_time",
        "departure_station": "departure_station_name",
        "return_station": "return_station_name",
        "covered_distance": "covered_distance_km",
        "duration": "duration_sec",
        "stopover": "has_stopover",
        "bike": "bike_id",
        "account": "account_id",
    }

    # Apply column mappings (case-insensitive)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    for old_name, new_name in column_mappings.items():
        if old_name in df.columns and new_name not in df.columns:
            df = df.rename(columns={old_name: new_name})

    # Parse datetime columns using two common formats, then auto
    def _parse_datetime_simple(series: pd.Series) -> pd.Series:
        series_str = series.astype("string")
        fmt1 = "%Y-%m-%d %H:%M"
        parsed = pd.to_datetime(series_str, format=fmt1, errors="coerce")
        ratio = parsed.isna().mean()
        if ratio <= 0.10:
            print(f"  • Parsed {series.name} format={fmt1}; NaT ratio={ratio:.2%}")
            return parsed
        fmt2 = "%m/%d/%Y %H:%M"
        parsed2 = pd.to_datetime(series_str, format=fmt2, errors="coerce")
        ratio2 = parsed2.isna().mean()
        if ratio2 <= 0.10:
            print(f"  • Parsed {series.name} format={fmt2}; NaT ratio={ratio2:.2%}")
            return parsed2
        parsed3 = pd.to_datetime(series_str, errors="coerce")
        ratio3 = parsed3.isna().mean()
        print(f"  • Parsed {series.name} format=auto; NaT ratio={ratio3:.2%}")
        return parsed3

    datetime_columns = ["departure_time", "return_time"]
    for col in datetime_columns:
        if col in df.columns:
            df[col] = _parse_datetime_simple(df[col])

    # Convert numeric columns
    numeric_columns = {
        "covered_distance_km": float,
        "duration_sec": float,
    }
    for col, dtype in numeric_columns.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)

    # Convert boolean columns
    if "has_stopover" in df.columns:
        # Handle various representations of boolean
        df["has_stopover"] = df["has_stopover"].map(
            {
                "Yes": True,
                "No": False,
                "yes": True,
                "no": False,
                True: True,
                False: False,
            }
        )

    # Extract station IDs and clean station names
    # Expected format: "XXXX Some Station Name" where XXXX is a 4-char code
    station_name_columns = []
    if "departure_station_name" in df.columns:
        station_name_columns.append("departure_station_name")
    if "return_station_name" in df.columns:
        station_name_columns.append("return_station_name")

    if station_name_columns:
        id_extract_pattern = r"^\s*([A-Za-z0-9]{4})\b"
        name_strip_pattern = r"^\s*[A-Za-z0-9]{4}\s+"

        for name_col in station_name_columns:
            series_str = df[name_col].astype("string")
            extracted_id = series_str.str.extract(id_extract_pattern)[0]
            cleaned_name = series_str.str.replace(name_strip_pattern, "", regex=True).str.strip()

            id_col = name_col.replace("_name", "_id")
            if id_col in df.columns:
                df[id_col] = df[id_col].astype("string").fillna(extracted_id)
            else:
                df[id_col] = extracted_id

            # Prefer cleaned name if we actually removed an ID; otherwise keep original
            df[name_col] = cleaned_name.fillna(series_str)

    # Sanitize column names for Spark/Delta compatibility:
    # - lowercase
    # - replace non [a-z0-9_] chars with underscore
    # - prefix leading digits with underscore
    # - collapse repeated underscores and trim edges
    # - ensure uniqueness by suffixing _1, _2, ...
    def _sanitize_names(names: list[str]) -> list[str]:
        sanitized = []
        seen = set()
        for name in names:
            n = str(name).strip().lower()
            n = re.sub(r"[^a-z0-9_]+", "_", n)
            n = re.sub(r"_+", "_", n).strip("_")
            if n == "" or n is None:
                n = "column"
            if re.match(r"^[0-9]", n):
                n = f"_{n}"
            base = n
            i = 1
            while n in seen:
                n = f"{base}_{i}"
                i += 1
            seen.add(n)
            sanitized.append(n)
        return sanitized

    df.columns = _sanitize_names(list(df.columns))

    return df


def combine_trip_data(file_paths: list[Path]) -> pd.DataFrame:
    """
    Read and combine multiple trip data files into a single DataFrame.

    Args:
        file_paths: list of paths to trip data files

    Returns:
        Combined DataFrame with all trip data

    Raises:
        DataProcessorError: If files cannot be read or combined
    """
    if not file_paths:
        raise DataProcessorError("No files provided to combine")

    all_dataframes = []

    for i, file_path in enumerate(file_paths, 1):
        print(f"[{i}/{len(file_paths)}] Reading {file_path.name}...")

        try:
            df = read_trip_data_file(file_path)
            df = standardize_trip_schema(df)

            # Add metadata about the source file
            df["source_file"] = file_path.name

            all_dataframes.append(df)
            print(f"  ✓ Loaded {len(df):,} rows")

        except DataProcessorError as e:
            print(f"  ✗ Failed: {e}")
            continue

    if not all_dataframes:
        raise DataProcessorError("No data files were successfully read")

    print("\nCombining all data...")
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    print(f"✓ Combined dataset has {len(combined_df):,} total rows")

    return combined_df


def save_to_parquet(
    df: pd.DataFrame,
    output_path: Path,
    compression: str = "snappy",
) -> Path:
    """
    Save a DataFrame to a Parquet file (Spark/Delta compatible).

    Args:
        df: DataFrame to save
        output_path: Path where the Parquet file should be saved
        compression: Compression algorithm to use (snappy, gzip, brotli, etc.)

    Returns:
        Path to the saved Parquet file

    Raises:
        DataProcessorError: If the file cannot be saved
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Use pyarrow engine and coerce timestamps to microseconds so Spark can read the file
        # without TIMESTAMP(NANOS) incompatibility. This is important when Spark reads the Parquet
        # directly (as opposed to pandas→Spark conversion).
        df.to_parquet(
            output_path,
            engine="pyarrow",
            compression=compression,
            index=False,
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
        )
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"\n✓ Saved to {output_path}")
        print(f"  File size: {file_size_mb:.2f} MB")
        print(f"  Rows: {len(df):,}")
        print(f"  Columns: {len(df.columns)}")
        return output_path

    except Exception as e:
        raise DataProcessorError(f"Failed to save Parquet file: {e}") from e


def get_data_summary(df: pd.DataFrame) -> dict:
    """
    Generate a summary of the trip data.

    Args:
        df: DataFrame with trip data

    Returns:
        Dict containing summary statistics
    """
    summary = {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "columns": list(df.columns),
        "memory_usage_mb": df.memory_usage(deep=True).sum() / (1024 * 1024),
    }

    # Date range if datetime columns exist
    if "departure_time" in df.columns:
        summary["date_range"] = {
            "start": df["departure_time"].min(),
            "end": df["departure_time"].max(),
        }

    # Null counts
    null_counts = df.isnull().sum()
    summary["null_counts"] = null_counts[null_counts > 0].to_dict()

    # Data types
    summary["dtypes"] = df.dtypes.astype(str).to_dict()

    return summary
