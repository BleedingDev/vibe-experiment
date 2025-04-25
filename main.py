import sys
from pathlib import Path

# Add src directory to path to import pipeline module
sys.path.insert(0, str(Path(__file__).parent / "src"))

from pipeline import main as pipeline_main

if __name__ == "__main__":
    pipeline_main()
