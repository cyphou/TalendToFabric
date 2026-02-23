"""
Test runner script — Run all tests with coverage and HTML reporting.

Usage:
    python tests/run_tests.py               # Run all tests
    python tests/run_tests.py --unit        # Only unit tests
    python tests/run_tests.py --regression  # Only regression tests
    python tests/run_tests.py --module parser  # Only parser tests
"""

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def run_tests(args=None):
    """Run pytest with standard options."""
    cmd = [
        sys.executable, "-m", "pytest",
        str(PROJECT_ROOT / "tests"),
        "-v",
        "--tb=short",
        "-ra",
    ]

    if args:
        cmd.extend(args)

    print(f"Running: {' '.join(cmd)}")
    print(f"{'=' * 70}")
    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT))
    return result.returncode


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Run Talend-to-Fabric migration tests")
    ap.add_argument("--unit", action="store_true", help="Run only unit tests")
    ap.add_argument("--integration", action="store_true", help="Run only integration tests")
    ap.add_argument("--regression", action="store_true", help="Run only regression tests")
    ap.add_argument("--module", choices=["parser", "translator", "sql", "validation"],
                    help="Run tests for a specific module")
    ap.add_argument("--coverage", action="store_true", help="Run with coverage report")
    ap.add_argument("--html", action="store_true", help="Generate HTML report")
    args = ap.parse_args()

    pytest_args = []

    if args.unit:
        pytest_args.extend(["-m", "unit"])
    elif args.integration:
        pytest_args.extend(["-m", "integration"])
    elif args.regression:
        pytest_args.extend(["-m", "regression"])
    elif args.module:
        pytest_args.extend(["-m", args.module])

    if args.coverage:
        pytest_args.extend([
            "--cov=parser", "--cov=translator", "--cov=validation",
            "--cov-report=term-missing",
        ])

    if args.html:
        pytest_args.extend(["--html=tests/report.html", "--self-contained-html"])

    sys.exit(run_tests(pytest_args))
