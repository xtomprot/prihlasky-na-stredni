# Czech Secondary School Application Data Workflow

An agentic workflow that scrapes, parses, and enriches data about Czech secondary school (střední škola) applications, admission statistics, and entrance exam information.

## Overview

The workflow consists of three interconnected Python scripts that work together to:

1. **Scrape** application statistics from a Power BI dashboard covering Czech secondary schools
2. **Parse** the scraped data into a normalized CSV format with derived metrics
3. **Enrich** each record with supplementary information:
   - Public transport accessibility from Praha Rajská zahrada (arrival by 7:45 AM)
   - School enrollment criteria (extra points policies, minimum thresholds)
   - Official contact information and websites

## Data Source

- **Power BI Dashboard**: https://app.powerbi.com/view?r=eyJrIjoiMjBlZDZmYTgtY2ZlZS00MDZmLWIxMDUtOTQ1NjI0YzFkOTY2IiwidCI6IjE4ZjgyNjAxLTVkYWYtNDBiOS04MTk2LTVjMDkwYjFjYjdkMCIsImMiOjl9
- **Dashboard Data**: Admission statistics for 2025 academic year, organized by school and curriculum
- **Test Data**: Results of state-wide unified entrance exams (Czech language + Mathematics, 50 points each)

## Prerequisites

### System Requirements
- Python 3.8 or higher
- Internet connection for web scraping
- ~5-10 minutes runtime for full workflow (depending on internet speed)

### Python Dependencies

Install all required packages:

```bash
pip install -r requirements.txt
```

**Key Libraries**:
- `requests`: HTTP requests for Power BI API and web scraping
- `beautifulsoup4`: HTML parsing from school websites
- `pandas`: Data manipulation and CSV handling
- `lxml`: HTML/XML parsing backend

### Input Data Files

The workflow expects these files in the `train/` directory:

```
train/
├── schools.json       # List of Czech secondary schools to process
└── curriculums.json   # List of study programs/curricula
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
- Hardcoded Power BI credentials (DatasetId, ReportId, ModelId)

**Run**:
```bash
python 1_scraper.py
```

**Output**: `data/01_scraped_schools.csv`

---

### Step 2: Data Parsing (`2_parser.py`)

Normalizes and validates the raw scraped data:
- Converts all numeric fields to proper data types
- Handles missing/invalid values
- Calculates derived metrics:
  - Acceptance rate (accepted / applications) as percentage
  - Regional classification
  - City extraction from combined fields
- Deduplicates records
- Sorts by school and curriculum for readability

**Run**:
```bash
python 2_parser.py
```

**Output**: `data/02_parsed_schools.csv`

**Example Statistics Generated**:
```
Schools: 19
Curricula: 4+
Regions: 7
Total capacity (R1): 5,000+
Total applications (R1): 12,000+
Average acceptance rate: 45-55%
```

---

### Step 3: AI Enrichment (`3_enrichment.py`)

Enriches each record with additional information from external sources:

#### Transport Accessibility
- Uses IDOS.cz (Czech public transport API) to check connections
- Route: **Praha Rajská zahrada → School Location**
- Target arrival: **07:45 AM on workdays**
- Returns: journey duration, departure/arrival times, route info
- *Note*: IDOS API access requires manual setup; currently includes placeholder logic

#### School Enrollment Criteria
Scrapes official school websites for:
- **Extra points policies**:
  - Points for grade average
  - Points for mathematics performance
  - Points for extracurricular activities
- **Minimum thresholds**:
  - Czech language exam minimum (out of 50 points)
  - Mathematics exam minimum (out of 50 points)
- **Selection procedures**: Additional criteria or notes from school

**Run**:
```bash
python 3_enrichment.py
```

**Output**: `data/03_enriched_schools.csv`

**Enrichment Log**: `data/enrichment_log.json` (status of each enrichment operation)

---

## CSV Output Format

### After Parsing (`02_parsed_schools.csv`)

| Column | Type | Notes |
|--------|------|-------|
| `school_name` | string | Official school name |
| `region` | string | Czech region (Kraj) |
| `city` | string | City/town location |
| `curriculum_name` | string | Study program name |
| `curriculum_code` | string | KKOV code (Czech curriculum classification) |
| `round1_capacity` | int | Seats available in Round 1 |
| `round1_applications` | int | Applications submitted (Round 1) |
| `round1_accepted` | int | Students accepted (Round 1) |
| `round1_acceptance_rate` | float | Acceptance percentage (Round 1) |
| `round1_min_score` | float | Lowest score of accepted students (0-100) |
| `round1_avg_score` | float | Average score of accepted students (0-100) |
| `round2_*` | various | Same metrics for supplementary Round 2 applications |

### After Enrichment (`03_enriched_schools.csv`)

*All columns from parsing, plus*:

| Column | Type | Notes |
|--------|------|-------|
| `public_transport_available` | bool | Is there viable public transport connection? |
| `public_transport_info` | string | Route description (e.g., "M1 subway + local bus") |
| `public_transport_duration_minutes` | int | Total travel time from center to school |
| `school_website` | string | Official school URL |
| `school_phone` | string | Main contact phone number |
| `school_email` | string | Admissions contact email |
| `extra_points_grade_avg` | int/float | Points awarded for good grades |
| `extra_points_math` | int/float | Points awarded for math performance |
| `extra_points_extracurricular` | int/float | Points awarded for activities/clubs |
| `min_threshold_czech_language` | int/float | Minimum points needed on Czech exam (0-50) |
| `min_threshold_math` | int/float | Minimum points needed on Math exam (0-50) |
| `enrollment_notes` | string | Additional selection criteria or policies |
| `enrichment_status` | string | "completed", "pending", or error message |

---

## Usage Examples

### Run the Complete Workflow

```bash
# Install dependencies
pip install -r requirements.txt

# Execute in sequence
python 1_scraper.py     # ~40-120 seconds
python 2_parser.py      # ~5 seconds
python 3_enrichment.py  # ~2-5 minutes (depends on web scraping)
```

### Re-run Specific Steps

The scripts are independent after data generation. You can:

```bash
# Modify parser logic and re-run without re-scraping
python 2_parser.py

# Test enrichment logic without hitting Power BI
python 3_enrichment.py
```

### Analyze Results

```python
import pandas as pd

# Load final enriched dataset
df = pd.read_csv("data/03_enriched_schools.csv")

# Filter schools with excellent acceptance rates
easy_schools = df[df['round1_acceptance_rate'] > 60]

# Find schools accessible by public transport
accessible = df[df['public_transport_available'] == True]

# Schools with strict math requirements
strict_math = df[df['min_threshold_math'] > 30]

# Combine criteria
df[
    (df['public_transport_available'] == True) &
    (df['round1_acceptance_rate'] > 50) &
    (df['round1_min_score'] < 60)
]
```

---

## Configuration

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
    "target_city": "Praha Rajská zahrada",  # Starting point for transport
    "target_arrival_time": "07:45",
    "idos_request_delay": 0.5,
    "school_site_request_delay": 2.0,
    "timeout": 10,
}
```

---

## Error Handling & Recovery

### Scraper Failures

If the scraper crashes at school #15:
1. The CSV already contains schools 1-14 (written incrementally)
2. Fix the issue or investigate school #15
3. Re-run: it will continue from where it left off or restart depending on configuration

### Parser Failures

- Input validation catches misformatted numeric data
- Missing values are preserved as `NaN`
- Invalid score ranges (>100) trigger warnings but don't stop processing

### Enrichment Failures

- Individual school enrichment failures don't stop the workflow
- Status tracked in `enrichment_log.json` per school
- Can retry failed schools manually or skip them

---

## Troubleshooting

### Power BI API Errors

**Issue**: `401 Unauthorized` or `403 Forbidden`

**Solution**:
- Power BI credentials may have expired
- Check if the dashboard is still publicly accessible
- May need to re-extract DatasetId/ReportId from current HAR logs

**Issue**: `Empty response` or timeouts

**Solution**:
- Increase `request_delay` in CONFIG
- Check internet connection
- Verify school names match exactly in `schools.json`

### School Website Scraping

**Issue**: Website patterns not matching, enrollment criteria not found

**Solution**:
- Some schools may not list criteria on their website
- Consider using Czech Ministry of Education (MŠMT) registry instead
- Manual lookup may be required for some schools

### Transport Data

**Issue**: IDOS API not responding

**Solution**:
- IDOS.cz may require direct browser access or JavaScript rendering
- Consider using Selenium for browser automation if needed
- Manual transport verification alternative: use `journeys.idos.cz`

---

## Data Quality Notes

### Limitations

1. **Missing School IDs in Power BI**: Requires iterating schools one-by-one
2. **Enrollment Criteria**: Not all schools publish detailed requirements online
3. **Contact Information**: May be outdated or incomplete
4. **Transport**: Requires IDOS API access which may have rate limiting

### Known Issues

- Some school websites are not easily machine-readable
- Enrollment criteria terminology varies by school
- Transport info requires real-time lookup (not static data)

### Validation

The parser automatically:
- Validates score ranges (0-100)
- Removes duplicate records
- Checks for missing regions/cities
- Calculates acceptance rates to identify outliers

---

## Future Enhancements

### Phase 2: LLM Integration

Use Claude Haiku/GPT for intelligent data extraction:
- Extract unstructured text from school websites
- Identify enrollment criteria even when not in standard format
- Summarize complex selection procedures

### Phase 3: Real-time Data

- Cached IDOS results with expiration
- School registry integration (MŠMT/ČŠOI)
- Historical trend data (multi-year comparisons)

### Phase 4: Analysis & Recommendations

- Match student profiles to school recommendations
- Visualizations (interactive dashboards)
- Export formats (JSON, Excel, web API)

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

### Project Structure

```
prihlasky-na-stredni/
├── src/
│   ├── 1_scraper.py          # Power BI data scraping
│   ├── 2_parser.py           # Data parsing and validation
│   └── 3_enrichment.py       # Data enrichment with transport info
├── test/
│   ├── test_scraper.py       # Scraper unit tests
│   ├── test_parser.py        # Parser unit tests
│   ├── test_enrichment.py    # Enrichment unit tests
│   └── conftest.py           # Pytest configuration
├── config/                    # Configuration files
├── output/                    # Generated output files (git-ignored)
├── log/                       # Application logs (git-ignored)
├── .gitignore                # Git ignore rules
├── requirements.txt          # Python dependencies
└── README.md                 # This file
```

---

## Advanced Features

### Customizing the Public Transport Starting Point

By default, routes are calculated from "Rajská zahrada" station. To use a different starting point:

```bash
python src/3_enrichment.py --pid-stop "Náměstí míru"
```

This is useful for:
- Students from different regions
- Comparing accessibility from multiple locations
- Analyzing transport patterns

### Updating Obsolete Configuration Values

The Power BI resource query UUID may become obsolete over time. To use a custom value:

```bash
export POWERBI_RESOURCE_QUERY="your-new-uuid-here"
python src/1_scraper.py --token "your-powerbi-token"
```

Or set the environment variable permanently in your shell profile.

---

## License & Attribution

Data sources:
- Power BI Dashboard: Czech Government/Ministry of Education
- IDOS: Czech public transport operator
- School websites: Individual school institutions

---

## Support

For issues, check:
1. Input files exist in `train/` directory
2. Python dependencies installed correctly
3. Internet connection stable
4. Log files in `data/` directory for error details

---

**Last Updated**: February 1, 2026
