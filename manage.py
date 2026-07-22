#!/usr/bin/env python3
"""Pipeline manager — select which example pipelines to load into Dagster.

Usage:
    python manage.py list                  Show available and loaded pipelines
    python manage.py add 01 02             Load pipelines by number
    python manage.py add cash orders       Load pipelines by name (partial match)
    python manage.py add all               Load every pipeline
    python manage.py remove 01             Unload a pipeline
    python manage.py remove all            Unload everything
    python manage.py reset                 Same as 'remove all'

The Dagster dev server hot-reloads — changes take effect automatically.
"""

from __future__ import annotations

import argparse
import re
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CATALOG_DIR = ROOT / "demo" / "catalog"
ACTIVE_DIR = ROOT / "demo" / "configs"

PROTECTED = {"demo.resource"}

RESOURCE_DEPS: dict[str, list[str]] = {
    "07_mixed_backend.macro": ["snowflake.resource"],
    "08_s3_file_ingestion.macro": ["s3.resource"],
}

PREREQ_NOTES: dict[str, str] = {
    "07_mixed_backend.macro": (
        "  Requires: pip install 'dagster-config-framework[snowflake]'\n"
        "  Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, etc."
    ),
    "08_s3_file_ingestion.macro": (
        "  Requires: pip install 'dagster-config-framework[s3]'\n"
        "  Set AWS_PROFILE, S3_BUCKET, AWS_REGION"
    ),
}


def get_catalog() -> list[Path]:
    return sorted(CATALOG_DIR.glob("*.macro"))


def get_loaded() -> set[str]:
    return {p.name for p in ACTIVE_DIR.glob("*.macro")}


def pipeline_label(filename: str) -> str:
    name = filename.replace(".macro", "")
    parts = name.split("_", 1)
    if len(parts) == 2:
        return parts[1].replace("_", " ").title()
    return name


def match_pipelines(queries: list[str], catalog: list[Path]) -> list[Path]:
    if not queries or queries == ["all"]:
        return list(catalog)

    matched = []
    for q in queries:
        q_lower = q.lower().strip()
        found = False
        for p in catalog:
            stem = p.stem.lower()
            num = stem.split("_", 1)[0]
            if q_lower == num or q_lower in stem:
                if p not in matched:
                    matched.append(p)
                found = True
        if not found:
            print(f"  No match for '{q}' — skipping")
    return matched


def cmd_list(_args: argparse.Namespace) -> None:
    catalog = get_catalog()
    loaded = get_loaded()

    if not catalog:
        print("\n  No pipelines found in demo/catalog/\n")
        return

    print("\n  Available pipelines:\n")
    print(f"  {'NUM':<6}{'NAME':<30}{'STATUS':<10}")
    print(f"  {'---':<6}{'----':<30}{'------':<10}")

    for p in catalog:
        num = p.stem.split("_", 1)[0]
        label = pipeline_label(p.name)
        status = "loaded" if p.name in loaded else "-"
        print(f"  {num:<6}{label:<30}{status:<10}")

    loaded_count = sum(1 for p in catalog if p.name in loaded)
    print(f"\n  {loaded_count} of {len(catalog)} pipelines loaded")

    extra_loaded = loaded - {p.name for p in catalog} - PROTECTED
    if extra_loaded:
        print(f"  Custom configs also loaded: {', '.join(sorted(extra_loaded))}")

    print()


def cmd_add(args: argparse.Namespace) -> None:
    catalog = get_catalog()
    targets = match_pipelines(args.pipelines, catalog)

    if not targets:
        print("  Nothing to add.")
        return

    loaded = get_loaded()
    added = []

    for p in targets:
        if p.name in loaded:
            print(f"  {pipeline_label(p.name)} — already loaded")
            continue

        shutil.copy2(p, ACTIVE_DIR / p.name)
        added.append(p.name)
        print(f"  + {pipeline_label(p.name)}")

        for res_file in RESOURCE_DEPS.get(p.name, []):
            res_src = CATALOG_DIR / res_file
            res_dst = ACTIVE_DIR / res_file
            if res_src.exists() and not res_dst.exists():
                shutil.copy2(res_src, res_dst)
                print(f"    + {res_file} (required resource)")

        if p.name in PREREQ_NOTES:
            print(PREREQ_NOTES[p.name])

    if added:
        print(f"\n  Added {len(added)} pipeline(s). Dagster will auto-reload.\n")
    else:
        print()


def cmd_remove(args: argparse.Namespace) -> None:
    catalog = get_catalog()

    if args.pipelines == ["all"]:
        targets_names = get_loaded() - PROTECTED
    else:
        targets = match_pipelines(args.pipelines, catalog)
        targets_names = {p.name for p in targets}

    if not targets_names:
        print("  Nothing to remove.")
        return

    removed = []
    remaining_loaded = (get_loaded() - targets_names) - PROTECTED

    for name in sorted(targets_names):
        dest = ACTIVE_DIR / name
        if dest.exists():
            dest.unlink()
            removed.append(name)
            print(f"  - {pipeline_label(name)}")

    for res_file, dependents in _inverted_resource_deps().items():
        still_needed = any(d in remaining_loaded for d in dependents)
        res_path = ACTIVE_DIR / res_file
        if not still_needed and res_path.exists() and res_file not in PROTECTED:
            res_path.unlink()
            print(f"    - {res_file} (no longer needed)")

    if removed:
        print(f"\n  Removed {len(removed)} pipeline(s). Dagster will auto-reload.\n")
    else:
        print()


def cmd_reset(_args: argparse.Namespace) -> None:
    loaded = get_loaded() - PROTECTED
    if not loaded:
        print("  No pipelines loaded — already clean.\n")
        return

    for name in sorted(loaded):
        dest = ACTIVE_DIR / name
        if dest.exists():
            dest.unlink()
            print(f"  - {pipeline_label(name)}")

    for res_file in _all_resource_files():
        res_path = ACTIVE_DIR / res_file
        if res_path.exists() and res_file not in PROTECTED:
            res_path.unlink()
            print(f"  - {res_file}")

    print(f"\n  All pipelines removed. Dagster will auto-reload.\n")


def _inverted_resource_deps() -> dict[str, list[str]]:
    inv: dict[str, list[str]] = {}
    for macro, resources in RESOURCE_DEPS.items():
        for r in resources:
            inv.setdefault(r, []).append(macro)
    return inv


def _all_resource_files() -> set[str]:
    files: set[str] = set()
    for resources in RESOURCE_DEPS.values():
        files.update(resources)
    return files


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Select which example pipelines to load into Dagster.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python manage.py list              Show available pipelines\n"
            "  python manage.py add 01 02         Load pipelines 01 and 02\n"
            "  python manage.py add cash           Load by name match\n"
            "  python manage.py add all            Load everything\n"
            "  python manage.py remove 01          Unload pipeline 01\n"
            "  python manage.py reset              Unload all pipelines\n"
        ),
    )
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("list", help="Show available and loaded pipelines")

    p_add = sub.add_parser("add", help="Load pipelines into Dagster")
    p_add.add_argument(
        "pipelines", nargs="+",
        help="Pipeline numbers (01, 02), partial names (cash, orders), or 'all'",
    )

    p_remove = sub.add_parser("remove", help="Unload pipelines from Dagster")
    p_remove.add_argument(
        "pipelines", nargs="+",
        help="Pipeline numbers, partial names, or 'all'",
    )

    sub.add_parser("reset", help="Unload all pipelines")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return

    {"list": cmd_list, "add": cmd_add, "remove": cmd_remove, "reset": cmd_reset}[
        args.command
    ](args)


if __name__ == "__main__":
    main()
