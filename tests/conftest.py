import sys
from pathlib import Path

# Make src/ importable, so tests can import the ecommerce package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
