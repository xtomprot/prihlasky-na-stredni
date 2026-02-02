# Czech Secondary School Application Data Workflow

Python workflow that scrapes, parses, and enriches data from https://www.vzdelavanivdatech.cz/. Reasons for development were:
1) missing export feature
2) missing foreign key from results table to list of schools. Made the wizzard useless. 

## Overview

The workflow consists of three interconnected Python scripts that work together to:

1. **Scrape** statistics from a Power BI dashboard of `Česká školní inspekce` https://www.vzdelavanivdatech.cz/ -> https://app.powerbi.com/view?r=eyJrIjoiMjBlZDZmYTgtY2ZlZS00MDZmLWIxMDUtOTQ1NjI0YzFkOTY2IiwidCI6IjE4ZjgyNjAxLTVkYWYtNDBiOS04MTk2LTVjMDkwYjFjYjdkMCIsImMiOjl9
   
2. **Parse** the scraped data into a normalized CSV format(e.g. string "59b." => decimal 59.0)
   
3. **Enrich** each record with supplementary information:
   - public transport duration/takovers scraped from IDOS

## Prerequisites

### Python Dependencies

Install all required packages:

```bash
pip install -r requirements.txt
```

**Key Libraries**:
- `requests`: HTTP requests for Power BI API and web scraping
- `pandas`: Data manipulation and CSV handling
- `openpyxl`: final transformation from CSV to XLSX

### Input Data Files

The workflow expects these files in the `config/` directory:

```
config/
├── schools.json       # List of Czech secondary schools to process (this is a subset scraped from /query requests catched in browser F12 when actually using the wizzard https://app.powerbi.com/view?r=eyJrIjoiMjBlZDZmYTgtY2ZlZS00MDZmLWIxMDUtOTQ1NjI0YzFkOTY2IiwidCI6IjE4ZjgyNjAxLTVkYWYtNDBiOS04MTk2LTVjMDkwYjFjYjdkMCIsImMiOjl9)
└── curriculums.json   # List of study programs/curricula (this should be ideally deprecated in future upgrades)
└── schools_addresses.json   # street and streetNr for IDOS; public web of the school for convenience; automation did not pay off for my use case - I manually copy pasted from google maps where I looked up by school name

```

*Note*: These files are included in the repository.

## Workflow

### Step 1: Web Scraping (`1_scraper.py`)

Queries the Power BI semantic query API for each school individually to fetch:
- School identification
- Curriculum/program names and codes
- Application round statistics:
  - Capacity (seats available)
  - Number of applications submitted
  - Number of accepted students
  - Minimum score required
  - Average score of accepted students
- Data for both Round 1 and Round 2 applications

**Key Features**:
- Iterates through schools one-by-one (Power BI doesn't return school identifiers in bulk queries)
- Writes results incrementally to CSV after each school (fault tolerance)
- Configurable request delays (default: 2 seconds between requests)
- Hardcoded Power BI data set identifiers (DatasetId, ReportId, ModelId)
- default token can be overriden by `--token` param
- if token fails, user is prompted for fresh

**Run**:
```bash
python 1_scraper.py
```

**Output**: `output/01_scraped_schools.csv`

---

### Step 2: Data Parsing (`2_parser.py`)

Normalizes and validates the raw scraped data:
- Converts all numeric fields to proper data types
- Handles missing/invalid values
- Calculates derived metrics:
  - Acceptance rate (accepted / applications) as percentage
- Deduplicates records
- Sorts by school and curriculum for readability

**Run**:
```bash
python 2_parser.py
```

**Output**: `output/02_parsed_schools.csv`

---

### Step 3: Python Enrichment (`3_enrichment.py`)

Enriches each record with additional information from external sources:

#### Transport Accessibility
- Uses IDOS.cz (Czech public transport API) to check connections
- Default route: **Praha Rajská zahrada → School Location**
- Target arrival: **07:45 AM on workdays**
- Returns: journey duration, departure/arrival times, number of transfers, route info
- **Customizable starting point**: Use `--pid-stop` to specify a different departure station

**Run**:
```bash
# Use default starting point (Rajská zahrada)
python 3_enrichment.py

# Use custom starting point
python 3_enrichment.py --pid-stop "Nádraží Veleslavín"
python 3_enrichment.py --pid-stop "Náměstí míru"
```

**Output**: `output/03_enriched_schools.csv`

---


## Deep Configuration

### Scraper (`1_scraper.py`)

Edit the `CONFIG` dictionary:

```python
CONFIG = {
    "dataset_id": "58a0ec54-35f2-4fc3-94f0-f9e0891c5e1c",  # Power BI dataset
    "report_id": "0b26f6ef-7ff7-45a8-8f12-78d023127d91",   # Power BI report
    "api_endpoint": "https://wabi-west-europe-d-primary-api...",
    "request_delay": 2.0,  # seconds between school queries
    "timeout": 30,  # HTTP timeout
}
```

### Enrichment (`3_enrichment.py`)

Edit the `CONFIG` dictionary:

```python
CONFIG = {
    "target_arrival_time": "07:45",
    "idos_request_delay": 0.5,
    "timeout": 10,
}
```

---

## Development & Testing

### Code Quality

The project uses modern Python linting and formatting tools:

```bash
# Format code with black
black src/

# Sort imports with isort
isort src/ --profile black

# Check for linting issues with ruff
ruff check src/
```

### Running Unit Tests

Comprehensive test suite with 40+ test cases:

```bash
pytest test/ -v                    # Run all tests with verbose output
pytest test/ -q                    # Run tests in quiet mode
pytest test/test_scraper.py -v    # Run specific test file
pytest test/test_parser.py::TestNormalizeNumeric -v  # Run specific test class
```

Tests cover:
- Power BI query builder and response parsing
- Data normalization and validation
- Public transport information extraction
- Configuration loading and error handling