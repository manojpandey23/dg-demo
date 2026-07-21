"""Complete framework loading test with validation registry check."""

from pathlib import Path

import pytest

from framework import FrameworkLoader
from framework.validation.engine.validation_registry import ValidationRegistry


@pytest.fixture
def config_dir():
    return Path(__file__).resolve().parents[2] / "src" / "test_domain" / "configs"


def test_framework_complete_load(config_dir):
    if not config_dir.exists():
        pytest.skip("test_domain configs not found")

    loader = FrameworkLoader(config_dir=config_dir, environment="local")
    defs = loader.get_definitions()
    assert defs is not None


def test_validation_registry_has_rules():
    rules = ValidationRegistry.all()
    assert len(rules) > 0, "No validation rules registered"
