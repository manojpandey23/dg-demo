# """
# JMESPath-based ``ref()`` resolver for config property placeholders.
#
# Walks a merged config dict and replaces string values matching
# ``ref("jmespath.expression")`` with the resolved value from the
# full config tree.  Supports recursive resolution (a ref that
# resolves to another ref) with cycle detection.
#
# **Important**: values under the key ``expr`` are **skipped** — those
# are runtime column transformation expressions that use a different
# ``ref()`` function (see ``TransformationContext``).
# """
#
# from __future__ import annotations
#
# import re
# from copy import deepcopy
# from typing import Any
#
# import jmespath
#
# # Matches:  ref("some.jmes.path")  or  ref('some.jmes.path')
# _REF_RE = re.compile(r"""^ref\(\s*(['"])(.*?)\1\s*\)$""")
#
# # Keys whose values must never be resolved (runtime expressions)
# _SKIP_KEYS = frozenset({"expr"})
#
# # Maximum recursion depth for nested ref resolution
# _MAX_DEPTH = 50
#
#
# def _is_ref(value: Any) -> str | None:
#     """If *value* is a ``ref("…")`` string, return the JMESPath expression."""
#     if not isinstance(value, str):
#         return None
#     m = _REF_RE.match(value.strip())
#     return m.group(2) if m else None
#
#
# def _resolve_value(
#     value: Any,
#     root: dict[str, Any],
#     path: str,
#     visiting: set[str],
#     depth: int,
# ) -> Any:
#     """Resolve a single value, recursing if the result is itself a ref."""
#     expr = _is_ref(value)
#     if expr is None:
#         return value
#
#     if depth > _MAX_DEPTH:
#         raise ValueError(
#             f"ref() resolution exceeded max depth ({_MAX_DEPTH}) at '{path}'"
#         )
#
#     ref_key = f"ref({expr})"
#     if ref_key in visiting:
#         raise ValueError(
#             f"Circular ref() detected: '{ref_key}' at '{path}'. "
#             f"Resolution chain: {visiting}"
#         )
#
#     visiting = visiting | {ref_key}  # immutable copy per branch
#
#     result = jmespath.search(expr, root)
#     if result is None:
#         raise ValueError(
#             f"ref() resolved to None — expression '{expr}' at '{path}' "
#             f"did not match anything in the config tree"
#         )
#
#     # Recursive resolution if the result is itself a ref string
#     return _resolve_value(result, root, path, visiting, depth + 1)
#
#
# def _walk_and_resolve(
#     obj: Any,
#     root: dict[str, Any],
#     path: str,
#     parent_key: str | None,
# ) -> Any:
#     """Recursively walk the config structure and resolve ref() placeholders."""
#     if parent_key in _SKIP_KEYS:
#         return obj
#
#     if isinstance(obj, dict):
#         return {
#             k: _walk_and_resolve(v, root, f"{path}.{k}", k)
#             for k, v in obj.items()
#         }
#
#     if isinstance(obj, list):
#         return [
#             _walk_and_resolve(item, root, f"{path}[{i}]", parent_key)
#             for i, item in enumerate(obj)
#         ]
#
#     if isinstance(obj, str):
#         return _resolve_value(obj, root, path, set(), 0)
#
#     return obj
#
#
# def resolve_refs(config: dict[str, Any]) -> dict[str, Any]:
#     """Resolve all ``ref("…")`` placeholders in *config*.
#
#     Parameters
#     ----------
#     config:
#         Merged config dict (resources + assets + jobs + sensors).
#
#     Returns
#     -------
#     A deep copy of *config* with all ``ref()`` strings replaced by
#     their resolved values.
#
#     Raises
#     ------
#     ValueError
#         On unresolvable expressions, circular references, or depth overflow.
#     """
#     working = deepcopy(config)
#     return _walk_and_resolve(working, working, "$", None)
#
