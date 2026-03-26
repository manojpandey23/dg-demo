#!/usr/bin/env python3
"""Test framework loading with jobs, sensors, and validations"""

from src.price_domain.framework import FrameworkLoader

try:
    loader = FrameworkLoader(
        config_dir='configs',
        resources_yaml='resources.yaml',
        environment='local'
    )
    
    assets, jobs, sensors, checks = loader.load_from_file('framework_pipeline.yaml')
    
    print(f'\n✅ FRAMEWORK LOADED SUCCESSFULLY\n')
    print(f'📊 Assets: {len(assets)}')
    if assets:
        for asset in assets:
            print(f'   ├─ {asset.name}')
    
    print(f'\n⚙️  Jobs: {len(jobs)}')
    if jobs:
        for job in jobs:
            print(f'   ├─ {job.name}')
    
    print(f'\n📡 Sensors: {len(sensors)}')
    if sensors:
        for sensor in sensors:
            name = sensor.name if hasattr(sensor, 'name') else str(sensor)
            print(f'   ├─ {name}')
    
    print(f'\n✔️  Validation Checks: {len(checks)}')
    if checks:
        for check in checks:
            print(f'   ├─ {check.__name__ if hasattr(check, "__name__") else type(check).__name__}')
    
    print(f'\n✅ ALL COMPONENTS LOADED SUCCESSFULLY!')
    
except Exception as e:
    print(f'\n❌ Error: {e}')
    import traceback
    traceback.print_exc()

