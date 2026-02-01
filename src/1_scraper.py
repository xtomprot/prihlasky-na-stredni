"""
Power BI Scraper for Czech Secondary School Application Data

This script queries the Power BI embedded dashboard one school at a time to extract
admission statistics (capacity, applications, acceptance rates, scores) for each
school and curriculum combination.

The script writes results incrementally to CSV after each school to avoid losing
data if the process fails mid-run.
"""

import argparse
import csv
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

# Configuration - Hardcoded from HAR log analysis
CONFIG = {
    "dataset_id": "58a0ec54-35f2-4fc3-94f0-f9e0891c5e1c",
    "report_id": "0b26f6ef-7ff7-45a8-8f12-78d023127d91",
    "api_endpoint": "https://wabi-west-europe-d-primary-api.analysis.windows.net/public/reports/querydata",
    "model_id": 5073049,
    "request_delay": 2.0,  # seconds between requests
    "timeout": 30,  # seconds
    # Power BI resource query - default value (may become obsolete)
    "powerbi_resource_query": "20ed6fa8-cfee-406f-b105-945624c1d966",
    # Power BI resource key - default token (expires periodically, update if needed)
    "powerbi_resource_key": "20ed6fa8-cfee-406f-b105-945624c1d966",
}

# Power BI resource key (refreshable token)
# Can be provided via command-line argument (--token), environment variable (POWERBI_RESOURCE_KEY), or uses default
POWERBI_RESOURCE_KEY = None


def resolve_powerbi_resource_key(token_arg: Optional[str] = None) -> str:
    """Resolve the Power BI resource key from multiple sources with user prompt fallback.

    Priority:
    1. Command-line argument (--token)
    2. Environment variable POWERBI_RESOURCE_KEY
    3. Default value from CONFIG
    4. Prompt user for new value if default fails

    Returns:
        str: The Power BI resource key to use
    """
    # Use command-line argument if provided
    if token_arg:
        logger.info("Using POWERBI_RESOURCE_KEY from command-line argument")
        return token_arg

    # Check environment variable
    env_key = os.getenv("POWERBI_RESOURCE_KEY")
    if env_key:
        logger.info("Using POWERBI_RESOURCE_KEY from environment variable")
        return env_key

    # Use default from CONFIG
    default_key = CONFIG.get("powerbi_resource_key")
    if default_key:
        logger.warning(
            f"No --token argument or POWERBI_RESOURCE_KEY environment variable provided. "
            f"Using default key from CONFIG (first 8 chars: {default_key[:8]}...)"
        )
        logger.warning(
            "If you encounter authentication errors, the default token may have expired. "
            "Provide a new token via --token argument."
        )

        # Ask user if they want to use default or provide a new one
        print("\n" + "=" * 80)
        print("POWERBI_RESOURCE_KEY Configuration")
        print("=" * 80)
        print(f"Default token found: {default_key[:8]}...{default_key[-8:]}")
        print("\nOptions:")
        print("  1. Press ENTER to use the default token")
        print("  2. Type a new token and press ENTER to override")
        print("=" * 80)

        user_input = input("Your choice: ").strip()

        if user_input:
            logger.info(
                "Using user-provided POWERBI_RESOURCE_KEY from interactive prompt"
            )
            return user_input
        else:
            logger.info("Using default POWERBI_RESOURCE_KEY from CONFIG")
            return default_key

    # No token available at all
    logger.error(
        "POWERBI_RESOURCE_KEY not provided. Use --token argument or POWERBI_RESOURCE_KEY environment variable."
    )
    raise ValueError(
        "POWERBI_RESOURCE_KEY is required. Provide via --token argument or environment variable."
    )


def _resolve_powerbi_resource_key(default: str) -> str:
    """Resolve the Power BI resource key.

    Priority:
    1. Environment variable `POWERBI_RESOURCE_KEY` if present
    2. Fallback to provided default
    """
    env_key = os.getenv("POWERBI_RESOURCE_KEY")
    if env_key:
        logger.debug("Using POWERBI_RESOURCE_KEY from environment")
        return env_key

    # Use default hardcoded value
    logger.debug("Using hardcoded POWERBI_RESOURCE_KEY")
    return default


def resolve_powerbi_resource_query() -> str:
    """Resolve the Power BI resource query UUID.

    Priority:
    1. Environment variable POWERBI_RESOURCE_QUERY if present
    2. Use stored default from CONFIG
    3. If default might be obsolete, prompt user for new value

    The default value is: 20ed6fa8-cfee-406f-b105-945624c1d966
    If this UUID becomes obsolete, users can provide a new value via
    the POWERBI_RESOURCE_QUERY environment variable.
    """
    env_query = os.getenv("POWERBI_RESOURCE_QUERY")
    if env_query:
        logger.debug("Using POWERBI_RESOURCE_QUERY from environment variable")
        return env_query

    # Use the default from CONFIG
    default_query = CONFIG["powerbi_resource_query"]
    logger.debug(f"Using default POWERBI_RESOURCE_QUERY: {default_query}")

    # Optionally prompt user if they want to use a different value
    logger.info(
        "Using default Power BI Resource Query. "
        "If this UUID becomes obsolete, set POWERBI_RESOURCE_QUERY environment variable."
    )

    return default_query


# Output file
OUTPUT_CSV = Path(__file__).parent.parent / "output" / "01_scraped_schools.csv"
OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

# Logging setup
LOG_DIR = Path(__file__).parent.parent / "log"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"01_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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

# CSV Headers
CSV_HEADERS = [
    "school_name",
    "curriculum_name",
    "curriculum_code",
    "curriculum_detail",
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


def cleanup_debug_folders():
    """Clean up debug request and response folders at the start of a run."""
    debug_dirs = [
        Path(__file__).parent.parent / "output" / "debug_requests",
        Path(__file__).parent.parent / "output" / "debug_responses",
    ]

    for debug_dir in debug_dirs:
        if debug_dir.exists():
            try:
                # Remove all files in the directory
                for file in debug_dir.glob("*"):
                    if file.is_file():
                        file.unlink()
                logger.info(f"Cleaned up debug folder: {debug_dir}")
            except Exception as e:
                logger.warning(f"Failed to clean up debug folder {debug_dir}: {e}")
        else:
            # Create the directory if it doesn't exist
            try:
                debug_dir.mkdir(parents=True, exist_ok=True)
                logger.debug(f"Created debug folder: {debug_dir}")
            except Exception as e:
                logger.warning(f"Failed to create debug folder {debug_dir}: {e}")


def load_schools_list(json_file: Path) -> List[str]:
    """Extract school names from schools.json file."""
    logger.debug(f"Loading schools from {json_file}")
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        schools = []
        if isinstance(data, dict) and "Values" in data:
            for entry in data["Values"]:
                if isinstance(entry, list) and len(entry) > 0:
                    literal_value = entry[0].get("Literal", {}).get("Value", "")
                    # Remove surrounding quotes
                    school_name = literal_value.strip("'\"")
                    schools.append(school_name)
                    logger.debug(f"  Loaded school: {school_name}")

        logger.info(f"Successfully loaded {len(schools)} schools")
        return schools
    except Exception as e:
        logger.error(f"Error loading schools: {e}", exc_info=True)
        return []


def load_curriculums_list(json_file: Path) -> Dict[str, str]:
    """Extract curriculum names and codes from curriculums.json file."""
    logger.debug(f"Loading curriculums from {json_file}")
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        curriculums = {}
        if isinstance(data, dict) and "Values" in data:
            for entry in data["Values"]:
                if isinstance(entry, list) and len(entry) > 0:
                    literal_value = entry[0].get("Literal", {}).get("Value", "")
                    # Format: 'Name (CODE)'
                    literal_value = literal_value.strip("'\"")
                    if "(" in literal_value and ")" in literal_value:
                        name = literal_value[: literal_value.rfind("(")].strip()
                        code = literal_value[
                            literal_value.rfind("(") + 1 : literal_value.rfind(")")
                        ].strip()
                        curriculums[name] = code
                        logger.debug(f"  Loaded curriculum: {name} ({code})")

        logger.info(f"Successfully loaded {len(curriculums)} curriculums")
        return curriculums
    except Exception as e:
        logger.error(f"Error loading curriculums: {e}", exc_info=True)
        return {}


def build_query(
    school_name: str, curriculums: List[str] = None, include_where: bool = True
) -> Dict[str, Any]:
    """Build a Power BI semantic query for admission data by school and curriculum.

    Args:
        school_name: Name of the school to filter by
        curriculums: List of curriculum names to filter by (optional)
        include_where: Whether to apply WHERE filters (default True)
    """

    # Default to empty list if no curriculums provided
    if curriculums is None:
        curriculums = []

    # Build the curriculum filter values
    curriculum_conditions = []
    for curriculum in curriculums:
        curriculum_conditions.append([{"Literal": {"Value": f"'{curriculum}'"}}])

    where_conditions = []

    # Add school filter if include_where is True
    if include_where:
        where_conditions.append(
            {
                "Condition": {
                    "In": {
                        "Expressions": [
                            {
                                "Column": {
                                    "Expression": {"SourceRef": {"Source": "111"}},
                                    "Property": "a03Name",
                                }
                            }
                        ],
                        "Values": [[{"Literal": {"Value": f"'{school_name}'"}}]],
                    }
                }
            }
        )

        # Add curriculum filter if provided
        if curriculum_conditions:
            where_conditions.append(
                {
                    "Condition": {
                        "In": {
                            "Expressions": [
                                {
                                    "Column": {
                                        "Expression": {"SourceRef": {"Source": "d"}},
                                        "Property": "obory",
                                    }
                                }
                            ],
                            "Values": curriculum_conditions,
                        }
                    }
                }
            )

    query = {
        "version": "1.0.0",
        "queries": [
            {
                "Query": {
                    "Commands": [
                        {
                            "SemanticQueryDataShapeCommand": {
                                "Query": {
                                    "Version": 2,
                                    "From": [
                                        {
                                            "Name": "11",
                                            "Entity": "13_2025_prijimacky",
                                            "Type": 0,
                                        },
                                        {
                                            "Name": "111",
                                            "Entity": "13_Kapacity_skoly_SS",
                                            "Type": 0,
                                        },
                                        {"Name": "l", "Entity": "Points", "Type": 0},
                                        {
                                            "Name": "t",
                                            "Entity": "testovani_zaci",
                                            "Type": 0,
                                        },
                                        {"Name": "d", "Entity": "dim_obory", "Type": 0},
                                        {
                                            "Name": "p",
                                            "Entity": "Points_SKOR",
                                            "Type": 0,
                                        },
                                        {
                                            "Name": "z",
                                            "Entity": "Zpusob_zadani",
                                            "Type": 0,
                                        },
                                    ],
                                    "Select": [
                                        {
                                            "Column": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "11"}
                                                },
                                                "Property": "KKOV",
                                            },
                                            "Name": "13_2025_prijimacky.KKOV",
                                            "NativeReferenceName": "Kód1",
                                        },
                                        {
                                            "Column": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "11"}
                                                },
                                                "Property": "obor",
                                            },
                                            "Name": "13_2025_prijimacky.obor",
                                            "NativeReferenceName": "Obor",
                                        },
                                        {
                                            "Column": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "11"}
                                                },
                                                "Property": "zamereni",
                                            },
                                            "Name": "13_2025_prijimacky.zamereni",
                                            "NativeReferenceName": "Zaměření1",
                                        },
                                        {
                                            "Column": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "11"}
                                                },
                                                "Property": "forma",
                                            },
                                            "Name": "13_2025_prijimacky.forma",
                                            "NativeReferenceName": "Forma studia1",
                                        },
                                        {
                                            "Column": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "11"}
                                                },
                                                "Property": "k1_kapacita",
                                            },
                                            "Name": "Sum(13_2025_prijimacky.k1_kapacita)",
                                            "NativeReferenceName": "k1_kapacita",
                                        },
                                        {
                                            "Column": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "11"}
                                                },
                                                "Property": "k1_pocet_podanych",
                                            },
                                            "Name": "13_2025_prijimacky.k1_pocet_podanych",
                                            "NativeReferenceName": "Počet podaných přihlášek celkem1",
                                        },
                                        {
                                            "Column": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "11"}
                                                },
                                                "Property": "k1_pocet_prijatych",
                                            },
                                            "Name": "13_2025_prijimacky.k1_pocet_prijatych",
                                            "NativeReferenceName": "Počet přijatých celkem1",
                                        },
                                        {
                                            "Column": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "11"}
                                                },
                                                "Property": "k1_min_skor",
                                            },
                                            "Name": "13_2025_prijimacky.k1_min_skor",
                                            "NativeReferenceName": "Min. počet bodů",
                                        },
                                        {
                                            "Column": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "11"}
                                                },
                                                "Property": "k1_prum_skor",
                                            },
                                            "Name": "13_2025_prijimacky.k1_prum_skor",
                                            "NativeReferenceName": "Prům. počet bodů",
                                        },
                                        {
                                            "Column": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "11"}
                                                },
                                                "Property": "pom",
                                            },
                                            "Name": "13_2025_prijimacky.pom",
                                            "NativeReferenceName": " ",
                                        },
                                        {
                                            "Column": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "11"}
                                                },
                                                "Property": "k2_kapacita",
                                            },
                                            "Name": "13_2025_prijimacky.k2_kapacita",
                                            "NativeReferenceName": "Kap. 1",
                                        },
                                        {
                                            "Column": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "11"}
                                                },
                                                "Property": "k2_pocet_podanych",
                                            },
                                            "Name": "13_2025_prijimacky.k2_pocet_podanych",
                                            "NativeReferenceName": "Počet podaných přihlášek celkem 1",
                                        },
                                        {
                                            "Column": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "11"}
                                                },
                                                "Property": "k2_pocet_prijatych",
                                            },
                                            "Name": "13_2025_prijimacky.k2_pocet_prijatych",
                                            "NativeReferenceName": "Počet přijatých celkem 1",
                                        },
                                        {
                                            "Column": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "11"}
                                                },
                                                "Property": "k2_min_skor",
                                            },
                                            "Name": "13_2025_prijimacky.k2_min_skor",
                                            "NativeReferenceName": "Min. počet bodů ",
                                        },
                                        {
                                            "Column": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "11"}
                                                },
                                                "Property": "k2_prum_skor",
                                            },
                                            "Name": "13_2025_prijimacky.k2_prum_skor",
                                            "NativeReferenceName": "Prům. počet bodů ",
                                        },
                                    ],
                                    "Where": where_conditions,
                                    "OrderBy": [
                                        {
                                            "Direction": 1,
                                            "Expression": {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "11"}
                                                    },
                                                    "Property": "k1_prum_skor",
                                                }
                                            },
                                        }
                                    ],
                                },
                                "Binding": {
                                    "Primary": {
                                        "Groupings": [{"Projections": list(range(15))}]
                                    },
                                    "DataReduction": {
                                        "DataVolume": 3,
                                        "Primary": {"Window": {"Count": 500}},
                                    },
                                    "Version": 1,
                                },
                                "ExecutionMetricsKind": 1,
                            }
                        }
                    ]
                },
                "QueryId": "",
                "ApplicationContext": {
                    "DatasetId": CONFIG["dataset_id"],
                    "Sources": [
                        {
                            "ReportId": CONFIG["report_id"],
                            "VisualId": "b8b4c55cb84dc45e8c03",
                        }
                    ],
                },
            }
        ],
        "cancelQueries": [],
        "modelId": CONFIG["model_id"],
    }

    return query


def query_power_bi(
    school_name: str, curriculums: List[str] = None, include_where: bool = True
) -> Optional[Dict[str, Any]]:
    """
    Query Power BI API for a specific school's data.

    Args:
        school_name: Name of the school to query
        curriculums: List of curriculum names to filter by (optional)
        include_where: Whether to apply WHERE filters

    Returns the parsed response or None if the request fails.
    """
    if not POWERBI_RESOURCE_KEY:
        logger.error("POWERBI_RESOURCE_KEY not set. Cannot query Power BI.")
        return None

    logger.debug(f"Building query for school: {school_name}")
    query = build_query(
        school_name, curriculums=curriculums, include_where=include_where
    )

    headers = {
        "Content-Type": "application/json",
        "X-PowerBI-ResourceKey": POWERBI_RESOURCE_KEY,
    }

    try:
        # Save outgoing query for debugging
        save_debug_request(query, school_name)
        response = requests.post(
            CONFIG["api_endpoint"],
            json=query,
            headers=headers,
            timeout=CONFIG["timeout"],
            params={"synchronous": "true"},
        )
        # If HTTP error, raise for status
        try:
            response.raise_for_status()
        except Exception:
            # Save raw response body if available
            try:
                resp_text = response.text
                logger.error("HTTP error body: %s", resp_text[:1000])
            except Exception:
                pass
            raise

        # Parse JSON and detect semantic errors returned in DataShapes
        resp_json = response.json()
        try:
            dsr = (
                resp_json.get("results", [])[0]
                .get("result", {})
                .get("data", {})
                .get("dsr", {})
            )
            data_shapes = dsr.get("DataShapes", [])
            if data_shapes:
                first = data_shapes[0]
                if isinstance(first, dict) and "odata.error" in first:
                    logger.error(
                        "Power BI semantic error for %s: %s",
                        school_name,
                        (
                            first["odata.error"]["message"]["value"]
                            if isinstance(first.get("odata.error"), dict)
                            else first["odata.error"]
                        ),
                    )
                    save_debug_response(resp_json, school_name + "_error")
                    return resp_json
        except Exception:
            logger.debug("No DataShapes error detection executed")

        logger.debug(f"Successfully received response for {school_name}")
        return resp_json
    except Exception as e:
        logger.error(f"Error querying Power BI for {school_name}: {e}", exc_info=True)
        return None


def save_debug_response(response: Dict[str, Any], school_name: str):
    """Save raw JSON response to output/debug_responses for inspection."""
    try:
        dbg_dir = Path(__file__).parent.parent / "output" / "debug_responses"
        dbg_dir.mkdir(parents=True, exist_ok=True)
        safe_name = "".join(c if c.isalnum() else "_" for c in school_name)[:200]
        out_file = dbg_dir / f"{safe_name}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(response, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved debug response for {school_name} to {out_file}")
    except Exception:
        logger.exception("Failed to save debug response")


def save_debug_request(query: Dict[str, Any], school_name: str):
    """Save the outgoing query JSON to output/debug_requests for inspection."""
    try:
        dbg_dir = Path(__file__).parent.parent / "output" / "debug_requests"
        dbg_dir.mkdir(parents=True, exist_ok=True)
        safe_name = "".join(c if c.isalnum() else "_" for c in school_name)[:200]
        out_file = dbg_dir / f"{safe_name}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(query, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved debug request for {school_name} to {out_file}")
    except Exception:
        logger.exception("Failed to save debug request")


def parse_response(
    response: Dict[str, Any], school_name: str, curriculums: Dict[str, str]
) -> List[Dict[str, Any]]:
    """
    Parse the Power BI semantic query response and extract school/curriculum records.

    The response structure contains:
    - descriptor.Select[]: List of 15 columns with metadata (Name, Value like G0-G14)
    - dsr.DS[0].PH[0].DM0[]: Array of data rows
      - S: Schema array mapping column index to type and dictionary name (DN)
      - C: Values array (indices into ValueDicts or numeric values)

    Returns a list of dictionaries with the extracted data.
    """
    records = []

    try:
        # Navigate the response structure
        if "results" not in response:
            logger.warning(f"No 'results' in response for {school_name}")
            return records

        result = response["results"][0].get("result", {})
        data = result.get("data", {})
        dsr = data.get("dsr", {})
        ds_list = dsr.get("DS", [])

        if not ds_list:
            logger.info(f"No data in response for {school_name}")
            return records

        # First DS entry contains the data
        ds = ds_list[0]
        ph_list = ds.get("PH", [])
        value_dicts = ds.get("ValueDicts", {})

        # Get schema from first DM0 to understand column mappings
        dm0_schema = None
        if ph_list and ph_list[0].get("DM0"):
            dm0_schema = ph_list[0]["DM0"][0].get("S", [])

        if not dm0_schema:
            logger.warning(f"No schema found in DM0 for {school_name}")
            return records

        logger.debug(
            f"Schema has {len(dm0_schema)} columns, ValueDicts: {list(value_dicts.keys())}"
        )

        # Build schema map: col_index -> {Name: "GX", DN: "DY" or None, T: type}
        schema_map = {}
        for col_idx, schema_col in enumerate(dm0_schema):
            schema_map[col_idx] = {
                "name": schema_col.get("N"),  # "G0", "G1", etc.
                "dict": schema_col.get("DN"),  # "D0", "D1", etc. or None for numeric
                "type": schema_col.get("T"),  # Type: 1=categorical, 4=numeric
            }

        # Iterate through projection hits (one per grouping)
        for round_idx, ph in enumerate(ph_list, 1):
            dm0_list = ph.get("DM0", [])
            logger.debug(f"Group {round_idx}: {len(dm0_list)} rows")

            previous_resolved = {}  # Track previous row for "R" (repeat) rows

            for row_idx, dm0 in enumerate(dm0_list):
                # Skip rows without C data
                if "C" not in dm0:
                    logger.debug(f"  Row {row_idx}: No C array, skipping")
                    continue

                # Extract values from C array using schema map with bitmasks
                indices = dm0.get("C", [])
                repeat_mask = dm0.get("R", 0) or 0
                null_mask = dm0.get("Ø", 0) or 0
                logger.debug(
                    f"  Row {row_idx}: {len(indices)} values in C array (R={repeat_mask}, Ø={null_mask})"
                )

                resolved = {}
                idx_pointer = 0

                # Iterate through schema columns and resolve values using masks
                for col_idx, schema_info in schema_map.items():
                    col_name = schema_info["name"]
                    dict_name = schema_info["dict"]

                    is_null = ((null_mask >> col_idx) & 1) == 1
                    is_repeat = ((repeat_mask >> col_idx) & 1) == 1

                    if is_null:
                        resolved[col_name] = None
                        logger.debug(f"    [{col_idx}/{col_name}] Null (Ø bit set)")
                        continue

                    if is_repeat:
                        resolved[col_name] = previous_resolved.get(col_name)
                        logger.debug(
                            f"    [{col_idx}/{col_name}] Repeat (R bit set): {resolved[col_name]}"
                        )
                        continue

                    if idx_pointer >= len(indices):
                        resolved[col_name] = None
                        logger.debug(
                            f"    [{col_idx}/{col_name}] No value available in C array, setting None"
                        )
                        continue

                    value = indices[idx_pointer]
                    idx_pointer += 1

                    if dict_name:
                        # Categorical value - resolve from ValueDict
                        if dict_name in value_dicts and isinstance(value, int):
                            if value < len(value_dicts[dict_name]):
                                resolved[col_name] = value_dicts[dict_name][value]
                                logger.debug(
                                    f"    [{col_idx}/{col_name}] {dict_name}[{value}] = {resolved[col_name]}"
                                )
                            else:
                                logger.warning(
                                    f"    [{col_idx}/{col_name}] Index {value} out of range for {dict_name} (len={len(value_dicts[dict_name])})"
                                )
                                resolved[col_name] = None
                        else:
                            logger.warning(
                                f"    [{col_idx}/{col_name}] Invalid dict lookup: {dict_name}[{value}]"
                            )
                            resolved[col_name] = None
                    else:
                        # Numeric value - use directly
                        resolved[col_name] = value
                        logger.debug(
                            f"    [{col_idx}/{col_name}] Numeric value: {value}"
                        )

                if idx_pointer != len(indices):
                    logger.debug(
                        f"  Row {row_idx}: Unused C values: {len(indices) - idx_pointer}"
                    )

                # Extract admission data from resolved values
                kkov = resolved.get("G0")  # KKOV (curriculum code)
                obor = resolved.get("G1")  # obor (curriculum name)
                zamereni = resolved.get("G2")  # specialization
                # forma = resolved.get("G3")  # form of study (not currently used)

                # Skip rows where core data is missing
                if not obor:
                    logger.debug(f"  Row {row_idx}: Skipping - no obor data")
                    continue

                k1_kapacita = resolved.get("G4")
                k1_pocet_podanych = resolved.get("G5")
                k1_pocet_prijatych = resolved.get("G6")
                k1_min_skor = resolved.get("G7")
                k1_prum_skor = resolved.get("G8")
                # G9 is "pom" - skip it
                k2_kapacita = resolved.get("G10")
                k2_pocet_podanych = resolved.get("G11")
                k2_pocet_prijatych = resolved.get("G12")
                k2_min_skor = resolved.get("G13")
                k2_prum_skor = resolved.get("G14")

                record = {
                    "school_name": school_name,
                    "curriculum_name": obor,
                    "curriculum_code": kkov,
                    "curriculum_detail": zamereni,
                    "round1_capacity": k1_kapacita,
                    "round1_applications": k1_pocet_podanych,
                    "round1_accepted": k1_pocet_prijatych,
                    "round1_min_score": k1_min_skor,
                    "round1_avg_score": k1_prum_skor,
                    "round2_capacity": k2_kapacita,
                    "round2_applications": k2_pocet_podanych,
                    "round2_accepted": k2_pocet_prijatych,
                    "round2_min_score": k2_min_skor,
                    "round2_avg_score": k2_prum_skor,
                }

                logger.debug(
                    f"  Created record: {obor}, cap={k1_kapacita}, app={k1_pocet_podanych}, min={k1_min_skor}, avg={k1_prum_skor}"
                )
                records.append(record)

                # Save this resolved row for potential repeat rows
                previous_resolved = resolved

    except Exception as e:
        logger.error(f"Error parsing response for {school_name}: {e}", exc_info=True)

    return records


def write_records_to_csv(records: List[Dict[str, Any]], append: bool = True):
    """Write or append records to CSV file."""
    file_exists = OUTPUT_CSV.exists()
    mode = "a" if append and file_exists else "w"

    try:
        logger.debug(f"Writing {len(records)} records to CSV (mode={mode})")
        with open(OUTPUT_CSV, mode, newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)

            # Write header only if creating new file
            if mode == "w":
                logger.debug("Writing CSV header")
                writer.writeheader()

            for record in records:
                writer.writerow(record)

        logger.info(f"Written {len(records)} records to {OUTPUT_CSV.name}")
    except Exception as e:
        logger.error(f"Error writing to CSV: {e}", exc_info=True)


def main():
    """Main scraping workflow."""
    global POWERBI_RESOURCE_KEY

    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Scrape Czech secondary school admission data from Power BI"
    )
    parser.add_argument(
        "--token",
        type=str,
        help="Power BI resource key (X-PowerBI-ResourceKey header value). Can also use POWERBI_RESOURCE_KEY environment variable.",
    )
    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info("Starting Power BI Scraper")
    logger.info("=" * 80)

    # Resolve POWERBI_RESOURCE_KEY from argument, environment, or default with prompt
    POWERBI_RESOURCE_KEY = resolve_powerbi_resource_key(args.token)
    logger.info(
        f"✓ POWERBI_RESOURCE_KEY configured (first 8 chars): {POWERBI_RESOURCE_KEY[:8]}..."
    )

    # Resolve POWERBI_RESOURCE_QUERY
    powerbi_resource_query = resolve_powerbi_resource_query()
    logger.info(f"Using POWERBI_RESOURCE_QUERY: {powerbi_resource_query}")

    # Clean up debug folders at the start of each run
    cleanup_debug_folders()

    schools_file = Path(__file__).parent.parent / "config" / "schools.json"
    curriculums_file = Path(__file__).parent.parent / "config" / "curriculums.json"

    if not schools_file.exists():
        logger.error(f"Schools file not found: {schools_file}")
        return

    if not curriculums_file.exists():
        logger.error(f"Curriculums file not found: {curriculums_file}")
        return

    logger.debug(f"Schools file: {schools_file}")
    logger.debug(f"Curriculums file: {curriculums_file}")

    # Load school and curriculum lists
    schools = load_schools_list(schools_file)
    curriculums = load_curriculums_list(curriculums_file)

    logger.info(f"Loaded {len(schools)} schools and {len(curriculums)} curricula")
    logger.info(f"Output: {OUTPUT_CSV}")
    logger.info(f"Request delay: {CONFIG['request_delay']}s")

    # Clear output file if it exists
    if OUTPUT_CSV.exists():
        logger.debug(f"Clearing existing output file: {OUTPUT_CSV}")
        OUTPUT_CSV.unlink()

    # Query each school
    successful = 0
    failed = 0
    for idx, school in enumerate(schools, 1):
        logger.info(f"[{idx}/{len(schools)}] Querying: {school}")

        response = query_power_bi(school)
        if response:
            # Save raw response for debugging every time
            save_debug_response(response, school)

            records = parse_response(response, school, curriculums)
            if records:
                write_records_to_csv(records, append=True)
                successful += 1
            else:
                logger.warning(f"No records extracted for {school}")

                # Fallback: retry without WHERE clause to inspect available data
                logger.info(
                    f"Retrying {school} query without WHERE filter to gather debug data..."
                )
                try:
                    fallback_resp = query_power_bi(school, include_where=False)
                    if fallback_resp:
                        save_debug_response(fallback_resp, school + "_fallback")
                        logger.info(f"Saved fallback response for {school}")
                    else:
                        logger.warning(
                            f"Fallback query returned no response for {school}"
                        )
                except Exception:
                    logger.exception("Fallback query failed")
                failed += 1
        else:
            logger.warning(f"Query failed for {school}, continuing...")
            failed += 1

        # Wait before next request (except last one)
        if idx < len(schools):
            logger.debug(f"Waiting {CONFIG['request_delay']}s before next request...")
            time.sleep(CONFIG["request_delay"])

    logger.info("=" * 80)
    logger.info(f"Scraping complete! Successful: {successful}, Failed: {failed}")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
