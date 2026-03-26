#!/usr/bin/env python
"""Test YAML-driven resources system"""

from src.price_domain.framework import FrameworkLoader

try:
    print("\n🧪 Testing YAML-Driven Resources System\n")
    
    # Initialize loader
    loader = FrameworkLoader(
        config_dir="../src/price_domain/configs",
        resources_yaml="resources.yaml",
        environment="local"
    )
    
    print(f"✅ FrameworkLoaderV2 initialized")
    print(f"✅ Resources loaded: {list(loader.resources.keys())}")
    
    # Try to get definitions
    print(f"\n📖 Loading definitions from framework_pipeline_v2.yaml...")
    defs = loader.get_definitions("framework_pipeline_v2.yaml")
    
    print(f"✅ Definitions loaded successfully!")
    print(f"   - Assets: {len(defs.assets)}")
    print(f"   - Jobs: {len(defs.jobs) if defs.jobs else 0}")
    print(f"   - Sensors: {len(defs.sensors) if defs.sensors else 0}")
    print(f"   - Resources: {len(defs.resources)}")
    
    # Print resource summary
    print(f"\n🔌 Resources Available:")
    for name in sorted(loader.resources.keys()):
        print(f"   ✅ {name}")
    
    print(f"\n✅ YAML-DRIVEN RESOURCES SYSTEM WORKING!")
    
except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()

