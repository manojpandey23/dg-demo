#!/usr/bin/env python3
"""Test sensor and job loading with fixes"""
import sys
sys.path.insert(0, '/projects/price-domain')

try:
    from src.price_domain.framework import FrameworkLoader
    
    loader = FrameworkLoader(
        config_dir='../src/price_domain/configs',
        resources_yaml='resources.yaml',
        environment='local'
    )
    
    assets, jobs, sensors, checks = loader.load_from_file('framework_pipeline.yaml')
    
    print("\n✅ Framework Loaded Successfully!")
    print(f"\nAssets: {len(assets)}")
    for asset in assets:
        print(f"  • {asset.name if hasattr(asset, 'name') else type(asset).__name__}")
    
    print(f"\nJobs: {len(jobs)}")
    for job in jobs:
        print(f"  • {job.name if hasattr(job, 'name') else type(job).__name__}")
    
    print(f"\nSensors: {len(sensors)}")
    for sensor in sensors:
        name = getattr(sensor, 'name', getattr(sensor, '__name__', type(sensor).__name__))
        print(f"  • {name}")
    
    print(f"\nValidation Checks: {len(checks)}")
    
    # Try creating definitions
    defs = loader.get_definitions('framework_pipeline.yaml')
    print("\n✅ Definitions Created Successfully!")
    
    # Check sensor details
    print(f"\nSensor Targets:")
    for sensor in sensors:
        if hasattr(sensor, 'job_name'):
            print(f"  • {sensor.name} → {sensor.job_name}")
        else:
            print(f"  • {getattr(sensor, 'name', 'unknown')} → (job linked)")
    
    print("\n✅ ALL TESTS PASSED!")
    
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

