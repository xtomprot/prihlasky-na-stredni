"""Unit tests for 1_scraper.py"""

import json
import sys
from pathlib import Path

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from src.scraper import (
    build_query,
    load_curriculums_list,
    load_schools_list,
    parse_response,
)


class TestBuildQuery:
    """Test Power BI query builder."""

    def test_build_query_with_school_only(self):
        """Test building query for a single school."""
        query = build_query("Test School")

        # Verify query structure
        assert query["version"] == "1.0.0"
        assert len(query["queries"]) == 1
        assert "modelId" in query

        # Verify WHERE clause contains school filter
        where_conditions = query["queries"][0]["Query"]["Commands"][0][
            "SemanticQueryDataShapeCommand"
        ]["Query"]["Where"]
        assert len(where_conditions) > 0

    def test_build_query_with_curriculums(self):
        """Test building query with curriculum filter."""
        curriculums = ["Obor 1", "Obor 2"]
        query = build_query("Test School", curriculums=curriculums)

        where_conditions = query["queries"][0]["Query"]["Commands"][0][
            "SemanticQueryDataShapeCommand"
        ]["Query"]["Where"]

        # Should have school AND curriculum filters
        assert len(where_conditions) == 2

    def test_build_query_without_where(self):
        """Test building query without WHERE clause."""
        query = build_query("Test School", include_where=False)

        where_conditions = query["queries"][0]["Query"]["Commands"][0][
            "SemanticQueryDataShapeCommand"
        ]["Query"]["Where"]

        # WHERE should be empty
        assert len(where_conditions) == 0


class TestParseResponse:
    """Test Power BI response parser."""

    def test_parse_response_with_valid_data(self):
        """Test parsing a valid Power BI response."""
        response = {
            "results": [
                {
                    "result": {
                        "data": {
                            "dsr": {
                                "DS": [
                                    {
                                        "PH": [
                                            {
                                                "DM0": [
                                                    {
                                                        "S": [
                                                            {
                                                                "N": "G0",
                                                                "DN": "D0",
                                                                "T": 1,
                                                            },
                                                            {
                                                                "N": "G1",
                                                                "DN": "D1",
                                                                "T": 1,
                                                            },
                                                            {
                                                                "N": "G2",
                                                                "DN": "D2",
                                                                "T": 1,
                                                            },
                                                            {
                                                                "N": "G3",
                                                                "DN": None,
                                                                "T": 4,
                                                            },
                                                            {
                                                                "N": "G4",
                                                                "DN": None,
                                                                "T": 4,
                                                            },
                                                            {
                                                                "N": "G5",
                                                                "DN": None,
                                                                "T": 4,
                                                            },
                                                            {
                                                                "N": "G6",
                                                                "DN": None,
                                                                "T": 4,
                                                            },
                                                            {
                                                                "N": "G7",
                                                                "DN": None,
                                                                "T": 4,
                                                            },
                                                            {
                                                                "N": "G8",
                                                                "DN": None,
                                                                "T": 4,
                                                            },
                                                            {
                                                                "N": "G9",
                                                                "DN": None,
                                                                "T": 4,
                                                            },
                                                            {
                                                                "N": "G10",
                                                                "DN": None,
                                                                "T": 4,
                                                            },
                                                            {
                                                                "N": "G11",
                                                                "DN": None,
                                                                "T": 4,
                                                            },
                                                            {
                                                                "N": "G12",
                                                                "DN": None,
                                                                "T": 4,
                                                            },
                                                            {
                                                                "N": "G13",
                                                                "DN": None,
                                                                "T": 4,
                                                            },
                                                            {
                                                                "N": "G14",
                                                                "DN": None,
                                                                "T": 4,
                                                            },
                                                        ],
                                                        "C": [
                                                            0,  # G0 - code from D0
                                                            0,  # G1 - name from D1
                                                            0,  # G2 - spec from D2
                                                            50,  # G3 - capacity (numeric)
                                                            100,  # G4 - applications
                                                            40,  # G5 - accepted
                                                            75,  # G6 - min score
                                                            85,  # G7 - avg score
                                                            0,  # G8 - separator
                                                            30,  # G9 - round2 capacity
                                                            50,  # G10
                                                            25,  # G11
                                                            70,  # G12
                                                            80,  # G13
                                                        ],
                                                    }
                                                ]
                                            }
                                        ],
                                        "ValueDicts": {
                                            "D0": ["2312B"],
                                            "D1": ["Elektrikář"],
                                            "D2": ["Silnoproud"],
                                        },
                                    }
                                ]
                            }
                        }
                    }
                }
            ]
        }

        records = parse_response(response, "Test School", {})

        # Should extract at least one record
        assert len(records) > 0
        record = records[0]

        # Verify record structure
        assert record["school_name"] == "Test School"
        assert record["curriculum_name"] == "Elektrikář"
        assert record["curriculum_code"] == "2312B"

    def test_parse_response_empty(self):
        """Test parsing empty response."""
        response = {"results": [{"result": {"data": {"dsr": {"DS": []}}}}]}

        records = parse_response(response, "Test School", {})
        assert records == []

    def test_parse_response_missing_core_data(self):
        """Test parsing response with missing curriculum name."""
        response = {
            "results": [
                {
                    "result": {
                        "data": {
                            "dsr": {
                                "DS": [
                                    {
                                        "PH": [
                                            {
                                                "DM0": [
                                                    {
                                                        "S": [
                                                            {
                                                                "N": "G0",
                                                                "DN": "D0",
                                                                "T": 1,
                                                            },
                                                            {
                                                                "N": "G1",
                                                                "DN": "D1",
                                                                "T": 1,
                                                            },
                                                            {
                                                                "N": "G2",
                                                                "DN": "D2",
                                                                "T": 1,
                                                            },
                                                            {
                                                                "N": "G3",
                                                                "DN": None,
                                                                "T": 4,
                                                            },
                                                            {
                                                                "N": "G4",
                                                                "DN": None,
                                                                "T": 4,
                                                            },
                                                            {
                                                                "N": "G5",
                                                                "DN": None,
                                                                "T": 4,
                                                            },
                                                            {
                                                                "N": "G6",
                                                                "DN": None,
                                                                "T": 4,
                                                            },
                                                            {
                                                                "N": "G7",
                                                                "DN": None,
                                                                "T": 4,
                                                            },
                                                            {
                                                                "N": "G8",
                                                                "DN": None,
                                                                "T": 4,
                                                            },
                                                            {
                                                                "N": "G9",
                                                                "DN": None,
                                                                "T": 4,
                                                            },
                                                            {
                                                                "N": "G10",
                                                                "DN": None,
                                                                "T": 4,
                                                            },
                                                            {
                                                                "N": "G11",
                                                                "DN": None,
                                                                "T": 4,
                                                            },
                                                            {
                                                                "N": "G12",
                                                                "DN": None,
                                                                "T": 4,
                                                            },
                                                            {
                                                                "N": "G13",
                                                                "DN": None,
                                                                "T": 4,
                                                            },
                                                            {
                                                                "N": "G14",
                                                                "DN": None,
                                                                "T": 4,
                                                            },
                                                        ],
                                                        # Missing C array - should be skipped
                                                    }
                                                ]
                                            }
                                        ],
                                        "ValueDicts": {
                                            "D0": ["2312B"],
                                            "D1": ["Elektrikář"],
                                        },
                                    }
                                ]
                            }
                        }
                    }
                }
            ]
        }

        records = parse_response(response, "Test School", {})
        assert records == []


class TestLoadSchoolsList:
    """Test loading schools from JSON."""

    def test_load_schools_valid_json(self, tmp_path):
        """Test loading valid schools JSON."""
        json_file = tmp_path / "schools.json"
        test_data = {
            "Values": [
                [{"Literal": {"Value": "'Gymnasium Praha'"}}],
                [{"Literal": {"Value": "'SOŠ Brno'"}}],
            ]
        }

        with open(json_file, "w") as f:
            json.dump(test_data, f)

        schools = load_schools_list(json_file)

        assert len(schools) == 2
        assert "Gymnasium Praha" in schools
        assert "SOŠ Brno" in schools

    def test_load_schools_empty_json(self, tmp_path):
        """Test loading empty JSON."""
        json_file = tmp_path / "schools.json"
        test_data = {"Values": []}

        with open(json_file, "w") as f:
            json.dump(test_data, f)

        schools = load_schools_list(json_file)
        assert schools == []

    def test_load_schools_missing_file(self, tmp_path):
        """Test loading non-existent file."""
        json_file = tmp_path / "nonexistent.json"
        schools = load_schools_list(json_file)
        assert schools == []


class TestLoadCurriculumsList:
    """Test loading curriculums from JSON."""

    def test_load_curriculums_valid_json(self, tmp_path):
        """Test loading valid curriculums JSON."""
        json_file = tmp_path / "curriculums.json"
        test_data = {
            "Values": [
                [{"Literal": {"Value": "'Elektrikář (2312B)'"}}],
                [{"Literal": {"Value": "'Tesař (2612A)'"}}],
            ]
        }

        with open(json_file, "w") as f:
            json.dump(test_data, f)

        curriculums = load_curriculums_list(json_file)

        assert len(curriculums) == 2
        assert curriculums["Elektrikář"] == "2312B"
        assert curriculums["Tesař"] == "2612A"

    def test_load_curriculums_empty_json(self, tmp_path):
        """Test loading empty JSON."""
        json_file = tmp_path / "curriculums.json"
        test_data = {"Values": []}

        with open(json_file, "w") as f:
            json.dump(test_data, f)

        curriculums = load_curriculums_list(json_file)
        assert curriculums == {}
