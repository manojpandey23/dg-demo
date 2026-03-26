# validation_engine.py

from copy import deepcopy

import pandas as pd
from framework.utils.pd_utils import DTYPE_COERCERS

from framework.validation.engine.validation_registry import ValidationRegistry


class ValidationEngine:
    def __init__(self, schema: dict):
        self.schema = schema

    @property
    def configured_rules(self) -> dict[str, dict]:

        rules: dict[str, dict] = {}

        # -------------------------
        # Column-scoped rules
        # -------------------------
        for column_name, entry in self.schema.get("columns", {}).items():
            for rule in entry.get("rules", []):
                rule = dict(rule)
                rule["column"] = column_name
                check_name = f"{column_name}_{rule['rule']}"
                rule["check_name"] = check_name
                rules[check_name] = rule

        # -------------------------
        # Asset / table-scoped rules
        # -------------------------
        for rule in self.schema.get("rules", []):
            rule = dict(rule)  # defensive copy
            check_name = rule["rule"]
            rule["check_name"] = check_name
            rules[check_name] = rule

        return rules

    def _prepare_rule(self, rule: dict) -> dict:
        rule = deepcopy(rule)
        return rule

    def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        column_rules = self.schema.get("columns", {})

        for column_name, rule in column_rules.items():
            dtype = rule.get("dtype")
            if dtype and column_name in df.columns:
                coercer = DTYPE_COERCERS.get(dtype)
                if coercer:
                    df[column_name] = coercer(df[column_name])

        return df

    def iter_results(self, df):
        df = self._normalize_dataframe(df)

        for section_name, section in self.schema.items():
            if section_name in ("version", "asset"):
                continue

            # -------------------------
            # COLUMN RULES (NEW SHAPE)
            # -------------------------
            if section_name == "columns":
                for column_name, column_entry in section.items():
                    for rule in column_entry.get("rules", []):
                        rule = self._prepare_rule(rule)
                        rule["column"] = column_name

                        rule_def = ValidationRegistry.get(rule["rule"])
                        yield rule_def.fn(df, rule)

            # -------------------------
            # ASSET / TABLE RULES
            # -------------------------
            elif section_name == "rules":
                for rule in section:
                    rule = self._prepare_rule(rule)

                    rule_def = ValidationRegistry.get(rule["rule"])
                    yield rule_def.fn(df, rule)

    def run(self, df, rule_name):
        df = self._normalize_dataframe(df)
        rule = self.configured_rules[rule_name]
        rule_def = ValidationRegistry.get(rule["rule"])
        return rule_def.fn(df, rule)
