"""
Example: User-defined functions for the framework.

Create a module like this in your project, then load it via:

    loader = FrameworkLoader(
        config_dir=Path("configs"),
        user_modules=["my_project.custom_functions"],
    )

Or in your definitions.py:

    from framework import FrameworkLoader, expr_function

    @expr_function
    def client_id() -> str:
        return "ACME_CORP"

    @expr_function
    def fiscal_quarter() -> str:
        import datetime
        month = datetime.date.today().month
        q = (month - 1) // 3 + 1
        return f"Q{q}"

These functions become available in YAML configs:

    config:
      prefix: "data/{client_id()}/{fiscal_quarter()}/"
      file_pattern: "report_{today()}_*.csv"

For sensor filter functions, register them with @expr_function
and reference by name in the tracking config:

    sensors:
      - name: my_sensor
        type: file_drop
        config:
          tracking:
            filter_fn: only_complete_files

Then define the function:

    @expr_function
    def only_complete_files(path) -> bool:
        # Skip files that end with .tmp (still being written)
        return not str(path).endswith('.tmp')
"""

from framework import expr_function


@expr_function
def client_id() -> str:
    """Return the client identifier for path construction."""
    import os

    return os.environ.get("CLIENT_ID", "demo")


@expr_function
def fiscal_quarter() -> str:
    """Return the current fiscal quarter (Q1-Q4)."""
    import datetime

    month = datetime.date.today().month
    q = (month - 1) // 3 + 1
    return f"Q{q}"


@expr_function
def only_complete_files(path) -> bool:
    """Filter out incomplete uploads (*.tmp, *.partial)."""
    s = str(path)
    return not s.endswith(".tmp") and not s.endswith(".partial")
