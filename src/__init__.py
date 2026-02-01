"""
Source package initialization

This module provides import aliases for the numbered scripts to support testing.
"""

# Create import aliases from numbered scripts
try:
    # Import from 1_scraper.py (renamed to scraper for imports)
    import importlib.util

    spec_scraper = importlib.util.spec_from_file_location(
        "scraper",
        __file__.replace("__init__.py", "1_scraper.py"),
    )
    scraper = importlib.util.module_from_spec(spec_scraper)
    spec_scraper.loader.exec_module(scraper)

    spec_parser = importlib.util.spec_from_file_location(
        "parser",
        __file__.replace("__init__.py", "2_parser.py"),
    )
    parser = importlib.util.module_from_spec(spec_parser)
    spec_parser.loader.exec_module(parser)

    spec_enrichment = importlib.util.spec_from_file_location(
        "enrichment",
        __file__.replace("__init__.py", "3_enrichment.py"),
    )
    enrichment = importlib.util.module_from_spec(spec_enrichment)
    spec_enrichment.loader.exec_module(enrichment)

except Exception as e:
    import warnings

    warnings.warn(f"Failed to import numbered scripts: {e}")

__all__ = ["scraper", "parser", "enrichment"]
