"""End-to-end pharmaceutical products transformation pipeline."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.config import DEFAULT_OUTPUT_DIR, FINAL_COLUMNS
from src.transformations.aggregations import (
    category_aggregations,
    most_common_ingredient_per_manufacturer,
    pivot_dosage_forms,
)
from src.transformations.categorical import (
    add_categorical_features,
    expensive_manufacturers,
    manufacturer_summary,
)
from src.transformations.cleaning import clean_data
from src.transformations.medicine_processing import parse_medicine_fields
from src.transformations.price_transforms import transform_prices
from src.transformations.profiling import run_profiling
from src.transformations.quality_checks import build_quality_flags, run_quality_report
from src.transformations.window_functions import apply_window_functions


def read_raw_data(spark: SparkSession, input_path: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
        .csv(input_path)
    )


def build_curated_dataset(df: DataFrame) -> DataFrame:
    """Apply full transformation pipeline and return final analytical table."""
    # 1. Cleaning
    df = clean_data(df)

    # 2. Medicine parsing
    df = parse_medicine_fields(df)

    # 3. Price transformations
    df = transform_prices(df)

    # 4. Categorical features
    df = add_categorical_features(df)

    # 5. Window functions
    df = apply_window_functions(df)

    # 6. Quality flags
    df = build_quality_flags(df)

    # Select final curated columns (plus useful extras for analysis)
    available = [c for c in FINAL_COLUMNS if c in df.columns]
    return df.select(*available)


def _write_curated_csv(df: DataFrame, output_dir: Path, local_mode: bool = True) -> Path:
    """Write curated dataset to CSV."""
    output_dir.mkdir(parents=True, exist_ok=True)

    if local_mode:
        # Pandas single-file write avoids Windows Hadoop native library issues in PyCharm
        curated_path = output_dir / "curated_products.csv"
        df.toPandas().to_csv(curated_path, index=False)
        return curated_path

    curated_path = output_dir / "curated_products"
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(str(curated_path))
    )
    return curated_path


def run_analytical_outputs(
    df: DataFrame,
    output_dir: Path,
    show_samples: bool = True,
    local_mode: bool = True,
) -> None:
    """Write aggregation tables and print sample analytical outputs."""
    if show_samples:
        print("\n=== CURATED DATASET SAMPLE ===")
        df.show(10, truncate=False)

        print("\n=== CATEGORY AGGREGATIONS ===")
        category_aggregations(df).show(15, truncate=False)

        print("\n=== TOP MANUFACTURERS ===")
        manufacturer_summary(df).show(15, truncate=False)

        print("\n=== PIVOT: DOSAGE FORMS BY MANUFACTURER (top 5) ===")
        pivot_dosage_forms(df).show(5, truncate=False)

        print("\n=== MOST COMMON INGREDIENT PER MANUFACTURER (top 10) ===")
        most_common_ingredient_per_manufacturer(df).show(10, truncate=False)

        print("\n=== UNUSUALLY EXPENSIVE PRODUCTS (top 10) ===")
        expensive_manufacturers(df).show(10, truncate=False)

        run_quality_report(df)

    curated_path = _write_curated_csv(df, output_dir, local_mode=local_mode)
    print(f"\nCurated output written to: {curated_path}")


def run_pipeline(
    spark: SparkSession,
    input_path: str,
    output_dir: Path | None = None,
    profile: bool = True,
    sample_fraction: float | None = None,
    local_mode: bool = True,
) -> DataFrame:
    """
    Run the full pipeline.

    Args:
        spark: Active SparkSession
        input_path: Path to raw CSV
        output_dir: Where to write curated output
        profile: Whether to run profiling on raw data
        sample_fraction: Optional fraction (0-1) for quick local testing
    """
    output_dir = output_dir or DEFAULT_OUTPUT_DIR

    print(f"Reading data from: {input_path}")
    raw_df = read_raw_data(spark, input_path)

    if sample_fraction and 0 < sample_fraction < 1:
        print(f"Sampling {sample_fraction * 100:.1f}% of data for quick test")
        raw_df = raw_df.sample(fraction=sample_fraction, seed=42)

    if profile:
        print("\n" + "=" * 60)
        print("PHASE 1: DATA PROFILING (RAW)")
        print("=" * 60)
        run_profiling(raw_df)

    print("\n" + "=" * 60)
    print("PHASE 2-7: TRANSFORM -> AGGREGATE -> QUALITY")
    print("=" * 60)
    curated_df = build_curated_dataset(raw_df)
    run_analytical_outputs(curated_df, output_dir, local_mode=local_mode)

    return curated_df
