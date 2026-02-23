# Contributing to TalendToFabric

Thank you for your interest in contributing! This document provides guidelines for contributing to the project.

## Getting Started

1. **Fork** the repository and clone your fork locally
2. Install dependencies: `pip install -r requirements.txt`
3. Run the test suite: `python -m pytest tests/ -q --tb=short`

## Development Workflow

1. Create a feature branch from `main`:
   ```bash
   git checkout -b feature/my-feature
   ```
2. Make your changes
3. Run the full test suite and ensure all **423+ tests** pass:
   ```bash
   python -m pytest tests/ -q --tb=short
   ```
4. Commit with a descriptive message following [Conventional Commits](https://www.conventionalcommits.org/):
   ```
   feat: add SAP HANA SQL dialect translator
   fix: correct tMap expression parsing for nested functions
   docs: update migration guide with new template
   test: add tests for Snowflake FLATTEN translation
   ```
5. Push and open a Pull Request

## Adding a New SQL Dialect

1. Add a new subclass of `BaseSQLTranslator` in `translator/sql_translator.py`
2. Define the dialect-specific regex rules in `_get_rules()`
3. Add a section in `mapping/datatype_map.json`
4. Add tests in `tests/test_sql_dialects.py`
5. Update `docs/component-mapping.md` and `docs/migration-guide.md`

## Adding a New Talend Component Mapping

1. Add the entry in `mapping/component_map.json` under the appropriate category
2. Add tests in `tests/test_new_features.py`
3. Update `docs/component-mapping.md`

## Adding a New Spark Template

1. Create the template in `templates/spark/`
2. Register it in `SparkTranslator.TEMPLATE_MAP` in `translator/translate_to_spark.py`
3. Add tests in `tests/test_translate_to_spark.py`
4. Update `docs/migration-guide.md` and `inventory/complexity_assessment.md`

## Code Style

- Follow PEP 8
- Use type hints where practical
- Add docstrings to public functions and classes
- Keep functions focused and under ~50 lines

## Test Guidelines

- All new features must have tests
- Tests go in the appropriate `tests/test_*.py` file
- Use the fixtures in `tests/fixtures/` for XML parsing tests
- Run `python -m pytest tests/ -q --tb=short` before submitting

## Questions?

Open an issue on GitHub or contact the maintainer.
