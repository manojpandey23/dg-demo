"""
Expression evaluator for dynamic path and file-pattern templates.

Uses a **decorator-based function registry**.  Functions decorated with
``@expr_function`` are available inside ``{…}`` placeholders and
evaluated at runtime via ``eval()``.

Template examples (assuming today is 2026-03-21)::

    path/{rdd()}/                              → path/20260321/
    path/{rdd('yyyy_mm_dd')}/                  → path/2026_03_21/
    path/{rdd('yyyy/mm/dd')}/                  → path/2026/03/21/
    path/{rdd('yy')}/                          → path/26/
    file_{today()}.csv                         → file_20260321.csv
    file_{today()}_*.csv                       → file_20260321_*.csv
    file_{today(format='yyyymmdd')}_*.csv      → file_20260321_*.csv

New date helpers can be added simply by decorating a function —
no changes to the evaluator are needed.
"""

from __future__ import annotations

import datetime as dt
import re
from typing import Callable, Final

# ------------------------------------------------------------------
# Format token → strftime mapping
# ------------------------------------------------------------------
_TOKEN_MAP: Final[list[tuple[str, str]]] = [
    ("yyyy", "%Y"),
    ("yy", "%y"),
    ("mm", "%m"),
    ("dd", "%d"),
]


def _to_strftime(fmt: str) -> str:
    """Convert a user-facing format string to a Python strftime pattern.

    Replaces tokens longest-first so ``yyyy`` is matched before ``yy``.
    Non-token characters (separators like ``/``, ``_``, ``-``) pass through.
    """
    result = fmt
    for token, strftime_code in _TOKEN_MAP:
        result = result.replace(token, strftime_code)
    return result


# ------------------------------------------------------------------
# Function registry
# ------------------------------------------------------------------

_EXPR_REGISTRY: dict[str, Callable[..., str]] = {}


def expr_function(fn: Callable[..., str]) -> Callable[..., str]:
    """Decorator — registers *fn* as a template expression function.

    The function becomes callable inside ``{…}`` placeholders by its
    ``__name__``.
    """
    _EXPR_REGISTRY[fn.__name__] = fn
    return fn


def get_expr_registry() -> dict[str, Callable[..., str]]:
    """Return a shallow copy of the current expression function registry."""
    return dict(_EXPR_REGISTRY)


# ------------------------------------------------------------------
# Registered expression functions
# ------------------------------------------------------------------

_DEFAULT_FORMAT: Final[str] = "yyyymmdd"
_RDD_DEFAULT_FORMAT: Final[str] = "yyyy/mm/dd"


@expr_function
def rdd(fmt: str = _RDD_DEFAULT_FORMAT) -> str:
    """Run-data-date — today's date in the given format.

    Tokens: ``yyyy``, ``yy``, ``mm``, ``dd`` plus arbitrary separators.

    Examples::

        rdd()              → '20260321'
        rdd('yyyy_mm_dd')  → '2026_03_21'
        rdd('yyyy/mm/dd')  → '2026/03/21'
        rdd('yy')          → '26'
    """
    return dt.date.today().strftime(_to_strftime(fmt))


@expr_function
def today(format: str = _DEFAULT_FORMAT) -> str:
    """Today's date in the given format.

    Tokens: ``yyyy``, ``yy``, ``mm``, ``dd`` plus arbitrary separators.

    Examples::

        today()                    → '20260321'
        today(format='yyyy_mm_dd') → '2026_03_21'
    """
    return dt.date.today().strftime(_to_strftime(format))


# ------------------------------------------------------------------
# Template evaluation
# ------------------------------------------------------------------

# Matches any {expression} placeholder (non-greedy, no nested braces)
_EXPR_RE: Final[re.Pattern[str]] = re.compile(r"\{([^}]+)\}")


def evaluate_expr(template: str) -> str:
    """Evaluate all ``{expression}`` placeholders in a template string.

    Each expression inside ``{…}`` is evaluated via ``eval()`` with
    only the registered expression functions in scope.

    Parameters
    ----------
    template:
        String that may contain ``{fn()}`` or ``{fn('arg')}`` placeholders.

    Returns
    -------
    Fully resolved string.

    Raises
    ------
    ValueError
        If an expression inside ``{…}`` cannot be evaluated.

    Examples
    --------
    >>> evaluate_expr("data/{rdd()}/files")
    'data/20260321/files'
    >>> evaluate_expr("report_{today(format='yyyy_mm_dd')}.csv")
    'report_2026_03_21.csv'
    """

    def _replace(match: re.Match[str]) -> str:
        expr = match.group(1).strip()
        try:
            result = eval(expr, {"__builtins__": {}}, _EXPR_REGISTRY)  # noqa: S307
            return str(result)
        except Exception as e:
            raise ValueError(
                f"Failed to evaluate expression '{expr}' in template '{template}': {e}"
            ) from e

    return _EXPR_RE.sub(_replace, template)
