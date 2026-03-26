#!/usr/bin/env python3
"""
Complete test of Framework V2 with Jobs, Sensors, and Validations
+ Validation registry cross-check
"""
import sys
import os

from price_domain.framework.validation_check_builder import ValidationFactory

sys.path.insert(
    0,
    '/projects/price-domain'
)

from src.price_domain.framework import FrameworkLoader

from price_domain.framework.validation.engine.validation_registry import ValidationRegistry


from price_domain.framework.validation.engine.validation_registry import ValidationRegistry


def validate_rules_exist_from_checks(checks):
    print("\n[2.5] Validating rules from generated asset checks...")

    missing = []

    for check in checks:
        rules = getattr(check, "_configured_validation_rules", [])

        for rule in rules:
            rule_name = rule["rule"]

            try:
                ValidationRegistry.get(rule_name)
            except Exception:
                missing.append(
                    f"Check={check.__name__}, Rule={rule_name}"
                )

    if missing:
        print("\n❌ UNKNOWN VALIDATION RULES FOUND:")
        for m in missing:
            print(f"   - {m}")
        raise ValueError(
            f"{len(missing)} validation rules are not registered"
        )

    print("    ✅ All rules used by checks exist in registry")


def main():
    print("\n" + "=" * 70)
    print("FRAMEWORK V2 - COMPLETE LOADING TEST")
    print("=" * 70)

    try:
        # --------------------------------------------------
        # 1. Initialize loader
        # --------------------------------------------------
        print("\n[1] Initializing Framework Loader...")
        loader = FrameworkLoader(
            config_dir='configs',
            resources_yaml='resources.yaml',
            environment='local'
        )
        print("    ✅ Loader initialized")
        print(f"    ✅ Resources loaded: {list(loader.resources.keys())}")

        # --------------------------------------------------
        # 2. Load configuration
        # --------------------------------------------------
        print("\n[2] Loading framework_pipeline.yaml...")
        assets, jobs, sensors, checks = loader.load_from_file(
            'framework_pipeline.yaml'
        )
        print("    ✅ Configuration loaded")

        # --------------------------------------------------
        # 2.5 Validate rules vs registry (NEW)
        # --------------------------------------------------
        validate_rules_exist_from_checks(checks)

        # --------------------------------------------------
        # 3. Display results
        # --------------------------------------------------
        print("\n" + "-" * 70)
        print("RESULTS:")
        print("-" * 70)

        print(f"\n📊 ASSETS ({len(assets)}):")
        for i, asset in enumerate(assets, 1):
            print(f"   {i}. ✅ {asset.key.to_user_string()}")

        print(f"\n⚙️  JOBS ({len(jobs)}):")
        for i, job in enumerate(jobs, 1):
            print(f"   {i}. ✅ {job.name}")

        print(f"\n📡 SENSORS ({len(sensors)}):")
        for i, sensor in enumerate(sensors, 1):
            name = sensor.name if hasattr(sensor, 'name') else str(sensor)
            print(f"   {i}. ✅ {name}")

        print(f"\n✔️  VALIDATION CHECKS ({len(checks)}):")

        if checks:
            for checks_def in checks:
                for rule in checks_def.check_specs_by_output_name:
                    print(
                        f"   ├─ ✅ {rule}")



                    # --------------------------------------------------
        # 4. Create definitions
        # --------------------------------------------------
        print("\n[3] Creating Dagster Definitions...")
        defs = loader.get_definitions('framework_pipeline.yaml')
        print("    ✅ Definitions created successfully")

        # --------------------------------------------------
        # Summary
        # --------------------------------------------------
        print("\n" + "=" * 70)
        print("✅ FRAMEWORK V2 FULLY LOADED AND OPERATIONAL!")
        print("=" * 70)
        print(f"\nSummary:")
        print(f"  • Assets: {len(assets)}")
        print(f"  • Jobs: {len(jobs)}")
        print(f"  • Sensors: {len(sensors)}")
        print(f"  • Validations: {len(checks)}")
        print(f"  • Resources: {len(loader.resources)}")
        print("\n" + "=" * 70 + "\n")

        return 0

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    exit(main())