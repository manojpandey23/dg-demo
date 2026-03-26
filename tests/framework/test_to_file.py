#!/usr/bin/env python3
"""Test and write results to file"""
import sys
sys.path.insert(0, '/projects/price-domain')

from src.price_domain.framework import FrameworkLoader

output = []
output.append("\n" + "="*70)
output.append("FRAMEWORK V2 - COMPLETE LOADING TEST")
output.append("="*70)

try:
    output.append("\n[1] Initializing Framework Loader...")
    loader = FrameworkLoader(
        config_dir='../src/price_domain/configs',
        resources_yaml='resources.yaml',
        environment='local'
    )
    output.append("    ✅ Loader initialized")
    output.append(f"    ✅ Resources: {list(loader.resources.keys())}")
    
    output.append("\n[2] Loading framework_pipeline.yaml...")
    assets, jobs, sensors, checks = loader.load_from_file('framework_pipeline.yaml')
    output.append("    ✅ Configuration loaded")
    
    output.append(f"\n📊 ASSETS: {len(assets)}")
    for asset in assets:
        name = asset.name if hasattr(asset, 'name') else str(type(asset).__name__)
        output.append(f"   • {name}")
    
    output.append(f"\n⚙️  JOBS: {len(jobs)}")
    for job in jobs:
        name = job.name if hasattr(job, 'name') else str(type(job).__name__)
        output.append(f"   • {name}")
    
    output.append(f"\n📡 SENSORS: {len(sensors)}")
    for sensor in sensors:
        if hasattr(sensor, '__name__'):
            name = sensor.__name__
        elif hasattr(sensor, 'name'):
            name = sensor.name
        else:
            name = type(sensor).__name__
        output.append(f"   • {name}")
    
    output.append(f"\n✔️  VALIDATION CHECKS: {len(checks)}")
    for check in checks:
        if hasattr(check, '__name__'):
            name = check.__name__
        else:
            name = type(check).__name__
        output.append(f"   • {name}")
    
    output.append("\n" + "="*70)
    output.append("✅ SUCCESS! All components loaded!")
    output.append("="*70)
    
except Exception as e:
    output.append(f"\n❌ ERROR: {e}")
    import traceback
    output.append(traceback.format_exc())

# Write to file
with open('/tmp/framework_test_results.txt', 'w') as f:
    f.write('\n'.join(output))

# Also print
print('\n'.join(output))

