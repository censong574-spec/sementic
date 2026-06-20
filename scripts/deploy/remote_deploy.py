"""Backward compatibility wrapper — prefer: python scripts/deploy/deploy.py"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from deploy import main  # noqa: E402

if __name__ == "__main__":
    main()
