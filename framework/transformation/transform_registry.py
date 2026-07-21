from typing import Any, Callable, Dict

import pandas as pd

from framework.transformation.system_context import AssetContextView
from framework.transformation.transformation_context import TransformationContext


_PD_SAFE = {
    "Timestamp": pd.Timestamp,
    "to_datetime": pd.to_datetime,
    "to_numeric": pd.to_numeric,
    "to_timedelta": pd.to_timedelta,
    "Series": pd.Series,
    "isna": pd.isna,
    "NaT": pd.NaT,
}


class _PdProxy:
    """Restricted proxy exposing only safe pandas functions to eval() scope."""

    def __getattr__(self, name: str):
        if name in _PD_SAFE:
            return _PD_SAFE[name]
        raise AttributeError(
            f"pd.{name} is not available in transform expressions. "
            f"Allowed: {sorted(_PD_SAFE)}"
        )


class TransformationRegistry:
    def __init__(self):
        self._functions: Dict[str, Callable] = {}

    def register(self, name: str, fn: Callable):
        if name in self._functions:
            raise ValueError(f"Transform '{name}' already registered")
        self._functions[name] = fn

    def build_scope(
        self,
        df: pd.DataFrame,
        context: TransformationContext,
        output_column: str,
        *,
        system_context: AssetContextView | None = None,
    ) -> Dict[str, Any]:
        scope: Dict[str, Any] = {"__builtins__": {}}

        for name, fn in self._functions.items():
            scope[name] = lambda *args, _fn=fn: _fn(df, context, output_column, *args)

        if system_context is not None:
            scope["context"] = system_context

        scope["pd"] = _PdProxy()
        return scope


# ✅ SINGLE global registry
TRANSFORMS = TransformationRegistry()


def transform(name: str):
    def decorator(fn):
        TRANSFORMS.register(name, fn)
        return fn

    return decorator
