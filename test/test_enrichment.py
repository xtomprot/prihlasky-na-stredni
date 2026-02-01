"""Unit tests for 3_enrichment.py"""

import sys
from pathlib import Path
from unittest.mock import patch
from urllib.parse import quote

import pandas as pd

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from src.enrichment import TransportChecker


class TestTransportChecker:
    """Test public transport checking functionality."""

    def test_build_idos_url_basic(self):
        """Test IDOS URL construction."""
        checker = TransportChecker()
        url = checker.build_idos_url("Střední škola Praha")

        assert "idos.cz" in url
        assert "Rajská%20zahrada" in url or "Rajsk%C3%A1%20zahrada" in url
        assert "07:45" in url
        assert "Střední%20škola%20Praha" in url or quote("Střední škola Praha") in url
        assert "arr=true" in url

    def test_build_idos_url_with_code(self):
        """Test IDOS URL construction with location code."""
        checker = TransportChecker()
        url = checker.build_idos_url("Střední škola Praha", arrival_code="12345")

        assert "12345" in url or "tc=12345" in url

    def test_build_idos_url_handles_none(self):
        """Test IDOS URL construction with None destination."""
        checker = TransportChecker()
        url = checker.build_idos_url(None)

        assert url is None

    def test_build_idos_url_date_format(self):
        """Test that IDOS URL contains valid date format."""
        checker = TransportChecker()
        url = checker.build_idos_url("Test School")

        # Date should be in DD.MM.YYYY format
        import re

        date_pattern = r"\d{2}\.\d{2}\.\d{4}"
        assert re.search(date_pattern, url), "Date not in DD.MM.YYYY format"

    def test_transport_checker_caching(self):
        """Test that transport information is cached."""
        checker = TransportChecker(start_point="Rajská zahrada")

        # First call should go through normal flow
        result1 = checker.check_transport("Test School", "School Address")

        # Second call with same parameters should be cached
        result2 = checker.check_transport("Test School", "School Address")

        # Results should be identical
        assert result1 == result2

    @patch("requests.get")
    def test_check_transport_request_failure(self, mock_get):
        """Test handling of request failures."""
        mock_get.side_effect = Exception("Network error")

        checker = TransportChecker(start_point="Rajská zahrada")
        result = checker.check_transport("Test School", "School Address")

        # Should return result with error status
        assert result["available"] is None
        assert result["data_source"] == "error"

    def test_transport_checker_target_from_initialization(self):
        """Test that transport checker initializes with correct target from."""
        checker = TransportChecker(start_point="Nové Město")

        assert checker.target_from == "Nové Město"
        assert checker.target_time == "07:45"
        assert checker.from_code == "301003"

    def test_transport_checker_default_initialization(self):
        """Test that transport checker initializes with default target from."""
        checker = TransportChecker()

        assert checker.target_from == "Rajská zahrada"
        assert checker.target_time == "07:45"
        assert checker.from_code == "301003"

    def test_transport_checker_configuration(self):
        """Test that transport checker uses correct configuration."""
        checker = TransportChecker()

        # Check IDOS URL base
        assert checker.idos_url == "https://idos.cz/pid/spojeni/"

        # Check cache is initialized
        assert isinstance(checker.cache, dict)
        assert len(checker.cache) == 0


class TestEnrichedDataStructure:
    """Test enriched data structure and columns."""

    def test_enrichment_output_headers(self):
        """Test that all expected output headers are defined."""
        from src.enrichment import OUTPUT_HEADERS

        expected_headers = [
            "school_name",
            "curriculum_name",
            "curriculum_code",
            "public_transport_available",
            "public_transport_info",
            "school_website",
            "school_phone",
            "enrichment_status",
        ]

        for header in expected_headers:
            assert header in OUTPUT_HEADERS

    def test_enrichment_dataframe_structure(self):
        """Test creating enriched dataframe with proper structure."""
        from src.enrichment import OUTPUT_HEADERS

        data = {
            "school_name": ["School 1"],
            "curriculum_name": ["Obor 1"],
            "curriculum_code": ["2312B"],
        }

        df = pd.DataFrame(data)

        # Add all output headers
        for col in OUTPUT_HEADERS:
            if col not in df.columns:
                df[col] = None

        # Verify all columns present
        for header in OUTPUT_HEADERS:
            assert header in df.columns

        # Verify column order
        df = df[OUTPUT_HEADERS]
        assert list(df.columns) == OUTPUT_HEADERS


class TestSchoolsConfigLoading:
    """Test loading school configuration."""

    def test_load_config_valid_json(self, tmp_path):
        """Test loading valid schools config JSON."""
        import json

        from src.enrichment import load_schools_config

        config_file = tmp_path / "schools_addresses.json"
        test_data = {
            "Gymnasium Praha": {
                "website": "https://www.gymnasium.cz",
                "address": "Gymnazium, Prague",
            },
            "SOŠ Brno": {
                "website": "https://www.sos-brno.cz",
                "address": "SOŠ, Brno",
            },
        }

        with open(config_file, "w") as f:
            json.dump(test_data, f)

        config = load_schools_config(config_file)

        assert len(config) == 2
        assert config["Gymnasium Praha"]["website"] == "https://www.gymnasium.cz"
        assert config["SOŠ Brno"]["address"] == "SOŠ, Brno"

    def test_load_config_missing_file(self, tmp_path):
        """Test loading non-existent config file."""
        from src.enrichment import load_schools_config

        config_file = tmp_path / "nonexistent.json"
        config = load_schools_config(config_file)

        assert config == {}

    def test_load_config_empty_json(self, tmp_path):
        """Test loading empty config JSON."""
        import json

        from src.enrichment import load_schools_config

        config_file = tmp_path / "schools_addresses.json"
        test_data = {}

        with open(config_file, "w") as f:
            json.dump(test_data, f)

        config = load_schools_config(config_file)
        assert config == {}
