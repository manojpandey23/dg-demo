from typing import Any, Callable, Dict

import pandas as pd

from framework.transformation.system_context import AssetContextView
from framework.transformation.transformation_context import TransformationContext


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
        scope: Dict[str, Any] = {}

        for name, fn in self._functions.items():
            scope[name] = lambda *args, _fn=fn: _fn(df, context, output_column, *args)

        if system_context is not None:
            scope["context"] = system_context

        scope["pd"] = pd
        return scope


# ✅ SINGLE global registry
TRANSFORMS = TransformationRegistry()


def transform(name: str):
    def decorator(fn):
        TRANSFORMS.register(name, fn)
        return fn

    return decorator
