# validation_check_builder.py
import itertools
from typing import Any, Dict, List

import dagster as dg
import pandas as pd
from dagster import AssetCheckResult, MetadataValue

from framework.model.config_models import AssetConfig, AssetSchema, AssetTests
from framework.validation.engine.validation_engine import ValidationEngine
from framework.validation.engine.validation_registry import Severity


class ValidationFactory:
    # ---------------------------------------------------------
    # Public entrypoint
    # ---------------------------------------------------------
    @staticmethod
    def build_validation_schema(asset_config: AssetConfig) -> Dict[str, Any]:

        schema = {
            "asset": asset_config.name,
            "columns": {},
            "rules": [],
        }

        # -----------------------------------------------------
        # Column schema → implicit + explicit tests
        # -----------------------------------------------------
        for col in asset_config.columns or []:
            schema["columns"][col.name] = ValidationFactory._build_column_entry(col)

        # -----------------------------------------------------
        # Asset-level tests (table scope)
        # -----------------------------------------------------
        for test in asset_config.tests or []:
            schema["rules"].append(ValidationFactory._build_asset_test(test))

        return schema

    # =========================================================
    # Column handling
    # =========================================================
    @staticmethod
    def _build_column_entry(col: AssetSchema) -> Dict[str, Any]:
        """
        Builds column entry with:
        - dtype (for _normalize_dataframe)
        - rules[] (implicit + explicit AssertTests)
        """
        entry = {
            # ✅ CRITICAL for _normalize_dataframe
            "dtype": col.dtype,
            "rules": [],
        }

        # -------------------------
        # Implicit tests
        # -------------------------
        if col.dtype:
            entry["rules"].append(
                {
                    "rule": "expect_column_type",
                    "column": col.name,
                    "dtype": col.dtype,
                    "severity": Severity.ERROR,
                }
            )

        if col.nullable is False:
            entry["rules"].append(
                {
                    "rule": "expect_column_values_to_not_be_null",
                    "column": col.name,
                    "severity": Severity.ERROR,
                }
            )

        if col.unique:
            entry["rules"].append(
                {
                    "rule": "expect_column_values_to_be_unique",
                    "column": col.name,
                    "severity": Severity.ERROR,
                }
            )

        # -------------------------
        # Explicit tests (ALL AssertTests)
        # -------------------------
        for test in col.tests:
            entry["rules"].append(ValidationFactory._build_column_test(col.name, test))

        return entry

    @staticmethod
    def _normalize_assert_test(test: AssetTests):
        """
        Normalizes an AssertTest which can be:
        - "expect_column_values_to_be_unique"
        - { expect_column_values_to_be_between: { arguments: {...}, severity: ERROR } }
        """
        value = test.root  # RootModel value

        # Case 1: bare string
        if isinstance(value, str):
            return value, {}, Severity.ERROR

        # Case 2: dict with single key → StructuredAssertTest
        rule_name, payload = next(iter(value.items()))

        arguments = payload.arguments or {}
        severity = Severity(payload.severity or "ERROR")

        return rule_name, arguments, severity

    @staticmethod
    def _build_column_test(column_name: str, test: AssetTests) -> Dict[str, Any]:
        rule_name, arguments, severity = ValidationFactory._normalize_assert_test(test)

        rule = {
            "rule": rule_name,
            "column": column_name,
            "severity": severity,
        }

        rule.update(arguments)
        return rule

    # =========================================================
    # Asset-level (table) tests
    # =========================================================
    @staticmethod
    def _build_asset_test(test: AssetTests) -> Dict[str, Any]:
        rule_name, arguments, severity = ValidationFactory._normalize_assert_test(test)

        rule = {
            "rule": rule_name,
            "severity": severity,
        }

        rule.update(arguments)
        return rule


class ValidationCheckBuilder:
    from dagster import AssetCheckResult

    @staticmethod
    def validation_result_to_asset_check(
        result,
        check_name: str,
    ) -> AssetCheckResult:

        metadata = {}

        # ----------------------------------
        # Rule-provided metadata
        # ----------------------------------
        if result.metadata:
            for key, value in result.metadata.items():
                if value is None:
                    continue

                if isinstance(value, bool):
                    metadata[key] = MetadataValue.bool(value)

                elif isinstance(value, int):
                    metadata[key] = MetadataValue.int(value)

                elif isinstance(value, float):
                    metadata[key] = MetadataValue.float(value)

                elif isinstance(value, str):
                    metadata[key] = MetadataValue.text(value)

                elif isinstance(value, (dict, list)):
                    metadata[key] = MetadataValue.json(value)

                else:
                    # Safe fallback
                    metadata[key] = MetadataValue.text(str(value))

        # ----------------------------------
        # Failing rows (markdown table)
        # ----------------------------------
        if result.failing_rows is not None and not result.failing_rows.empty:
            metadata["failing_rows"] = MetadataValue.md(
                result.failing_rows.head().to_markdown()
            )

        # ----------------------------------
        # Standard framework metadata
        # ----------------------------------
        metadata["severity"] = MetadataValue.text(
            result.severity.value
            if hasattr(result.severity, "value")
            else str(result.severity)
        )

        metadata["scope"] = MetadataValue.text(
            result.scope.value if hasattr(result.scope, "value") else str(result.scope)
        )

        if result.column:
            metadata["column"] = MetadataValue.text(result.column)

        return AssetCheckResult(
            check_name=check_name,
            passed=result.passed,
            metadata=metadata,
        )

    @staticmethod
    def build_checks(asset_configs: List[AssetConfig]) -> List:

        checks = []

        for config in asset_configs:
            # Skip assets without validation requirements
            if not config.columns and not config.tests:
                continue

            check = ValidationCheckBuilder.build_checks_for_asset(config)
            if check:
                checks.append(check)

        return list(itertools.chain.from_iterable(checks))

    @staticmethod
    def build_checks_for_asset(config: AssetConfig):

        asset_name = config.name
        asset_key = dg.AssetKey(asset_name)

        schema = ValidationFactory.build_validation_schema(config)
        engine = ValidationEngine(schema)

        checks = []

        for check_name, rule in engine.configured_rules.items():
            blocking = rule["severity"] == Severity.ERROR

            def make_check(bound_check_name: str):
                @dg.asset_check(
                    asset=asset_key, name=bound_check_name, blocking=blocking
                )
                def rule_check(context, asset_value: pd.DataFrame):

                    try:
                        result = engine.run(asset_value, bound_check_name)

                        yield ValidationCheckBuilder.validation_result_to_asset_check(
                            result=result, check_name=bound_check_name
                        )

                    except Exception as e:
                        context.log.error(
                            f"Validation error in {asset_name} [{bound_check_name}]: {str(e)}"
                        )
                        raise

                return rule_check

            checks.append(make_check(check_name))

        return checks
