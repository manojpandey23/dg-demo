"""
Framework package for config-driven Dagster pipeline generation.

This package provides a meta-framework for generating Dagster assets, jobs, and sensors
from YAML configuration files. It abstracts the complexity of orchestration and validation
into a simple configuration-based approach, similar to DBT but for any data source/destination.

Key modules:
- config_models: Pydantic V2 models for config validation
- resources_builder: Build resources from resources.yaml
- framework_loader: Load resources and pipelines from YAML
- asset_builder: Generate assets from V2 config
- job_builder: Generate jobs from config
- sensor_builder: Generate sensors from config
"""

from framework.builder.core_loader import FrameworkLoader
from framework.builder.resources_builder import ResourceBuilder

__all__ = ["FrameworkLoader", "ResourceBuilder"]
