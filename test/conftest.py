"""Pytest configuration and fixtures"""

import importlib.util
import sys
from pathlib import Path

# Add src directory to Python path for imports
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))


def import_module_from_file(module_name, file_path):
    """Import a module from a file path."""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# Make modules available for import
scraper = import_module_from_file("scraper", src_path / "1_scraper.py")
parser = import_module_from_file("parser", src_path / "2_parser.py")
enrichment = import_module_from_file("enrichment", src_path / "3_enrichment.py")

sys.modules["src.scraper"] = scraper
sys.modules["src.parser"] = parser
sys.modules["src.enrichment"] = enrichment
