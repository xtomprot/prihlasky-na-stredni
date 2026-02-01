"""Unit tests for 2_parser.py"""

import sys
from pathlib import Path

import pandas as pd

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from src.parser import calculate_acceptance_rate, normalize_numeric


class TestNormalizeNumeric:
    """Test numeric normalization function."""

    def test_normalize_valid_float(self):
        """Test normalizing valid float strings."""
        assert normalize_numeric("45.5") == 45.5
        assert normalize_numeric("100") == 100.0
        assert normalize_numeric("0") == 0.0

    def test_normalize_with_comma(self):
        """Test normalizing European format with comma."""
        assert normalize_numeric("45,5") == 45.5
        assert normalize_numeric("123,45") == 123.45

    def test_normalize_with_artifact(self):
        """Test removing scraped data artifacts."""
        assert normalize_numeric("49b.") == 49.0
        assert normalize_numeric("64.5b.") == 64.5

    def test_normalize_with_parentheses(self):
        """Test handling values with parentheses."""
        assert normalize_numeric("120 (21+38+59+0+2)") == 120.0

    def test_normalize_none_values(self):
        """Test handling None and empty values."""
        assert normalize_numeric(None) is None
        assert normalize_numeric("") is None
        assert normalize_numeric("N/A") is None

    def test_normalize_invalid_string(self):
        """Test handling invalid strings."""
        assert normalize_numeric("invalid") is None
        assert normalize_numeric("abc123") is None

    def test_normalize_numeric_input(self):
        """Test handling numeric input."""
        assert normalize_numeric(45.5) == 45.5
        assert normalize_numeric(100) == 100.0


class TestCalculateAcceptanceRate:
    """Test acceptance rate calculation."""

    def test_calculate_normal_rate(self):
        """Test normal acceptance rate calculation."""
        rate = calculate_acceptance_rate(40, 100)
        assert rate == 40.0

    def test_calculate_partial_rate(self):
        """Test partial acceptance rate."""
        rate = calculate_acceptance_rate(33, 100)
        assert rate == 33.0

    def test_calculate_high_competition_rate(self):
        """Test high competition rate."""
        rate = calculate_acceptance_rate(10, 100)
        assert rate == 10.0

    def test_calculate_none_accepted(self):
        """Test with None accepted."""
        rate = calculate_acceptance_rate(None, 100)
        assert rate is None

    def test_calculate_none_applications(self):
        """Test with None applications."""
        rate = calculate_acceptance_rate(40, None)
        assert rate is None

    def test_calculate_zero_applications(self):
        """Test with zero applications (avoid division by zero)."""
        rate = calculate_acceptance_rate(40, 0)
        assert rate is None

    def test_calculate_zero_accepted_zero_applications(self):
        """Test with both zeros."""
        rate = calculate_acceptance_rate(0, 0)
        assert rate is None

    def test_calculate_rounding(self):
        """Test that result is rounded to 2 decimal places."""
        rate = calculate_acceptance_rate(1, 3)
        # 1/3 * 100 = 33.333... rounded to 33.33
        assert rate == 33.33


class TestParserDataValidation:
    """Test data validation in parser."""

    def test_numeric_column_conversion(self):
        """Test conversion of numeric columns."""
        # Create sample data with string numbers
        data = {
            "school_name": ["School 1"],
            "curriculum_name": ["Elektrikář"],
            "curriculum_code": ["2312B"],
            "curriculum_detail": ["Silnoproud"],
            "round1_capacity": ["50"],
            "round1_applications": ["100"],
            "round1_accepted": ["40"],
            "round1_min_score": ["75.5"],
            "round1_avg_score": ["85"],
            "round2_capacity": ["30"],
            "round2_applications": ["50"],
            "round2_accepted": ["25"],
            "round2_min_score": ["70"],
            "round2_avg_score": ["80"],
        }

        df = pd.DataFrame(data)

        # Convert to numeric
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

        for col in numeric_cols:
            df[col] = df[col].apply(normalize_numeric)

        # Verify conversions
        assert df["round1_capacity"].iloc[0] == 50.0
        assert df["round1_min_score"].iloc[0] == 75.5
        assert df["round1_avg_score"].iloc[0] == 85.0

    def test_acceptance_rate_calculation_in_dataframe(self):
        """Test acceptance rate calculation on DataFrame."""
        data = {
            "school_name": ["School 1", "School 2"],
            "curriculum_name": ["Obor 1", "Obor 2"],
            "round1_accepted": [40.0, 10.0],
            "round1_applications": [100.0, 100.0],
        }

        df = pd.DataFrame(data)
        df["round1_acceptance_rate"] = df.apply(
            lambda row: calculate_acceptance_rate(
                row["round1_accepted"], row["round1_applications"]
            ),
            axis=1,
        )

        assert df["round1_acceptance_rate"].iloc[0] == 40.0
        assert df["round1_acceptance_rate"].iloc[1] == 10.0
