"""
Data Parser for Czech Secondary School Application Data

This script reads the raw scraped CSV from the Power BI scraper,
normalizes and cleans the data, and prepares it for enrichment.

Handles:
- Data type conversion
- Missing value handling
- Validation
- Calculation of derived metrics (acceptance rates, etc.)
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

INPUT_CSV = Path(__file__).parent.parent / "output" / "01_scraped_schools.csv"
OUTPUT_CSV = Path(__file__).parent.parent / "output" / "02_parsed_schools.csv"

OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

# Logging setup
LOG_DIR = Path(__file__).parent.parent / "log"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"02_parser_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# File handler
file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setLevel(logging.DEBUG)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Formatter
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add handlers
logger.addHandler(file_handler)
logger.addHandler(console_handler)


def normalize_numeric(value: Any) -> float:
    """Convert value to float, handling various formats and text artifacts."""
    if value is None or value == "" or value == "N/A":
        return None

    try:
        # Handle string with decimal point or comma
        if isinstance(value, str):
            value = value.strip()

            # Remove text artifacts from scraped data
            # "49b." → "49"
            # "64.5b." → "64.5"
            value = value.replace("b.", "").strip()

            # Handle values with parentheses like "120 (21+38+59+0+2)"
            # Take the first number before the parenthesis
            if "(" in value:
                value = value.split("(")[0].strip()

            # Replace comma with period for European decimals
            value = value.replace(",", ".")

            # If empty after cleaning, return None
            if not value:
                return None

        return float(value)
    except (ValueError, TypeError):
        return None


def calculate_acceptance_rate(accepted: float, applications: float) -> float:
    """Calculate acceptance rate as percentage."""
    if accepted is None or applications is None or applications == 0:
        return None
    return round((accepted / applications) * 100, 2)


def parse_and_normalize() -> pd.DataFrame:
    """
    Parse raw scraped data and normalize it.

    Returns a pandas DataFrame with:
    - Numeric conversion of all score and count columns
    - Validation of data ranges
    - Calculation of acceptance rates
    - Extraction of region from city field
    """

    if not INPUT_CSV.exists():
        logger.error(f"Input file not found: {INPUT_CSV}")
        return None

    logger.info(f"Reading: {INPUT_CSV}")

    try:
        df = pd.read_csv(INPUT_CSV, dtype=str)
    except Exception as e:
        logger.error(f"Error reading CSV: {e}", exc_info=True)
        return None

    logger.info(f"Loaded {len(df)} rows")

    # Numeric columns to convert
    numeric_cols = [
        "round1_capacity",
        "round1_applications",
        "round1_accepted",
        "round1_min_score",
        "round1_avg_score",
        "round2_capacity",
        "round2_applications",
        "round2_accepted",
        "round2_min_score",
        "round2_avg_score",
    ]

    logger.debug(f"Converting {len(numeric_cols)} columns to numeric format")
    # Convert to numeric
    for col in numeric_cols:
        df[col] = df[col].apply(normalize_numeric)

    # Region/city extraction removed (pattern-based extraction is unreliable)

    # Calculate acceptance rates for each round
    df["round1_acceptance_rate"] = df.apply(
        lambda row: calculate_acceptance_rate(
            row["round1_accepted"], row["round1_applications"]
        ),
        axis=1,
    )

    df["round2_acceptance_rate"] = df.apply(
        lambda row: calculate_acceptance_rate(
            row["round2_accepted"], row["round2_applications"]
        ),
        axis=1,
    )

    # Validate score ranges (should be 0-100, state exams are 50 points each = 100 total)
    for col in [
        "round1_min_score",
        "round1_avg_score",
        "round2_min_score",
        "round2_avg_score",
    ]:
        # Flag potentially invalid scores (> 100 or < 0)
        invalid = df[(df[col].notna()) & ((df[col] < 0) | (df[col] > 100))]
        if not invalid.empty:
            logger.warning(f"Found {len(invalid)} rows with invalid scores in {col}")

    # Sort by school name and curriculum
    df = df.sort_values(["school_name", "curriculum_name", "curriculum_detail"])
    logger.debug("Sorted data by school_name and curriculum_name")

    # Reorder columns for logical grouping
    column_order = [
        "school_name",
        "curriculum_name",
        "curriculum_code",
        "curriculum_detail",
        "round1_capacity",
        "round1_applications",
        "round1_accepted",
        "round1_acceptance_rate",
        "round1_min_score",
        "round1_avg_score",
        "round2_capacity",
        "round2_applications",
        "round2_accepted",
        "round2_acceptance_rate",
        "round2_min_score",
        "round2_avg_score",
    ]

    df = df[column_order]
    logger.debug(f"Reordered columns to {len(column_order)} columns")

    # Remove duplicates (keep first occurrence)
    df = df.drop_duplicates(
        subset=["school_name", "curriculum_name", "curriculum_detail"], keep="first"
    )
    logger.debug(f"Removed duplicates, final row count: {len(df)}")

    return df


def write_parsed_csv(df: pd.DataFrame):
    """Write parsed and normalized data to CSV."""
    try:
        df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
        logger.info(f"Written {len(df)} records to {OUTPUT_CSV}")
    except Exception as e:
        logger.error(f"Error writing CSV: {e}", exc_info=True)


def print_statistics(df: pd.DataFrame):
    """Print basic statistics about the parsed data."""
    logger.info("Data Statistics:")
    logger.info("-" * 60)

    # Capacity statistics
    if df["round1_capacity"].notna().sum() > 0:
        logger.info("Round 1 Capacity:")
        logger.info(f"  Total: {df['round1_capacity'].sum():.0f}")
        logger.info(f"  Min: {df['round1_capacity'].min():.0f}")
        logger.info(f"  Max: {df['round1_capacity'].max():.0f}")
        logger.info(f"  Avg: {df['round1_capacity'].mean():.1f}")

    # Application statistics
    if df["round1_applications"].notna().sum() > 0:
        logger.info("Round 1 Applications:")
        logger.info(f"  Total: {df['round1_applications'].sum():.0f}")
        logger.info(f"  Avg per curriculum: {df['round1_applications'].mean():.1f}")

    # Acceptance rate statistics
    if df["round1_acceptance_rate"].notna().sum() > 0:
        logger.info("Round 1 Acceptance Rates:")
        logger.info(f"  Min: {df['round1_acceptance_rate'].min():.1f}%")
        logger.info(f"  Max: {df['round1_acceptance_rate'].max():.1f}%")
        logger.info(f"  Avg: {df['round1_acceptance_rate'].mean():.1f}%")

    # Score statistics
    if df["round1_avg_score"].notna().sum() > 0:
        logger.info("Round 1 Average Scores:")
        logger.info(f"  Min: {df['round1_avg_score'].min():.1f}")
        logger.info(f"  Max: {df['round1_avg_score'].max():.1f}")
        logger.info(f"  Overall Avg: {df['round1_avg_score'].mean():.1f}")

    logger.info("-" * 60)


def main():
    """Main parsing workflow."""

    logger.info("=" * 60)
    logger.info("Starting Data Parser")
    logger.info("=" * 60)

    df = parse_and_normalize()

    if df is None or df.empty:
        logger.error("No data to process")
        return

    write_parsed_csv(df)
    print_statistics(df)

    logger.info("=" * 60)
    logger.info("Parsing complete!")
    logger.info(f"Input:  {INPUT_CSV}")
    logger.info(f"Output: {OUTPUT_CSV}")
    logger.info(f"Log:    {LOG_FILE}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
